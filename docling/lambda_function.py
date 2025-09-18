# handle sqs event from s3 when a new document needs to be parsed and chunked for embedding
# enqueues one task per chunk of text and refs to tables, images

import os
os.environ["HF_HOME"] = "/tmp/huggingface"
os.environ["EASYOCR_MODULE_PATH"] = "/tmp"
os.environ["MODULE_PATH"] = "/tmp"

import json
import requests
import traceback
import base64
import string
import uuid
import zipfile
import io
import re

from PIL import Image

from typing_extensions import override
from typing import Any

from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
from docling.backend.msword_backend import MsWordDocumentBackend
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat, DocumentStream
from docling.datamodel.pipeline_options import (
	AcceleratorDevice,
	AcceleratorOptions,
	PdfPipelineOptions,
)
from docling.datamodel.pipeline_options import PipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption, WordFormatOption

from docling.chunking import HybridChunker
from docling_core.transforms.chunker.hierarchical_chunker import (
	DocChunk, 
	ChunkingDocSerializer,
	ChunkingSerializerProvider,
)
from docling_core.types.doc.document import (
	PictureClassificationData,
	PictureDescriptionData,
	PictureItem,
	PictureMoleculeData,
	RefItem,
	DoclingDocument
)
from docling_core.transforms.serializer.common import create_ser_result
from docling_core.transforms.serializer.base import BaseDocSerializer, SerializationResult
from docling_core.transforms.serializer.markdown import MarkdownTableSerializer, MarkdownPictureSerializer

import logging
logger = logging.getLogger(__name__)

# page range scan
import fitz

import boto3
BOTO_S3 = boto3.client('s3', region_name=os.environ["AWS_REGION"])

BOTO_BEDROCK = boto3.client("bedrock-runtime", region_name="us-east-1")
LLM_MODEL = "arn:aws:bedrock:us-east-1:928244370134:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0"

BOTO_SQS = boto3.client('sqs', region_name=os.environ["AWS_REGION"])
response = BOTO_SQS.get_queue_url(QueueName='docling-chunk')
SQS_DOCLING_CHUNK = response.get('QueueUrl')


def lambda_handler(event: dict, context):
	print('event', event)

	# sqs events of s3 events for concurrency throttling
	# can accept multiple sqs events, capped to 1
	if event.get('Records'):

		for sqs_record in event['Records']:

			body = json.loads(sqs_record['body'])
	
			# s3 might have combined multiple events
			for record in body.get('Records', []):
				event_name = record.get('eventName')				
				s3_bucket = record.get('s3', {}).get('bucket', {}).get('name')
				s3_object = record.get('s3', {}).get('object', {})
				s3_key = s3_object.get('key')
				s3_size = s3_object.get('size')
				print(event_name, s3_key, 'size', s3_size)

				if event_name == 'ObjectRemoved:Delete': return
				if not s3_size: return

				# Get the object content
				response = BOTO_S3.get_object(Bucket=s3_bucket, Key=s3_key)
	
				record_event = {
					's3_uri': f's3://{s3_bucket}/{s3_key}',
					'content': response['Body'].read()
				}
				handle_single_event(record_event)

	else:
		# local testing
		# { presignedUrl: 'http...' }
		handle_single_event(event)


class DocumentFormatError(Exception):
	pass

class DocumentType:
	PDF = ".pdf"
	DOC = ".doc"
	DOCX = ".docx"
	RTF = ".rtf"
	IMAGE = ".png"
	TXT = ".txt"
	XLSX = ".xlsx"

class DocumentDetector:
	def __init__(self, bytes_stream: bytes):
		self.bytes_stream = bytes_stream

	def _detect_document_type(self) -> str:
		"""Detect document type using file signatures"""
		magic_numbers = {
			b'%PDF': DocumentType.PDF,
			b'\xd0\xcf\x11\xe0': DocumentType.DOC,
			b'{\\\rtf1': DocumentType.RTF,
		}
		header = self.bytes_stream[:4]
		for signature, doc_type in magic_numbers.items():
			if header.startswith(signature):
				return doc_type
		if header.startswith(b'PK\x03\x04'):
			try:
				with zipfile.ZipFile(io.BytesIO(self.bytes_stream)) as zf:
					if any(f.endswith('word/document.xml') for f in zf.namelist()):
						return DocumentType.DOCX
					if any(f.endswith('xl/workbook.xml') for f in zf.namelist()):
						return DocumentType.XLSX
			except zipfile.BadZipFile:
				pass
		try:
			Image.open(io.BytesIO(self.bytes_stream)).verify()
			return DocumentType.IMAGE
		except Exception:
			pass
	
		if all(chr(b) in string.printable for b in self.bytes_stream[:100]):
			return DocumentType.TXT

		raise DocumentFormatError("Unsupported document format")

# -------- helpers to slice the document to a single chunk --------
def _to_refitem(x):
	if isinstance(x, RefItem):
		return x
	if isinstance(x, str):
		return RefItem(cref=x)

	sr = getattr(x, "self_ref", None)
	if isinstance(sr, RefItem):
		return sr
	if isinstance(sr, str):
		return RefItem(cref=sr)

	raise TypeError(f"Unsupported ref: {type(x)} -> {x!r}")

def _resolve_node(ref_like, doc: DoclingDocument):
	return _to_refitem(ref_like).resolve(doc)  # -> NodeItem (has .children)

def _walk_descendants(node):
	stack = [node]
	while stack:
		n = stack.pop()
		yield n
		# Docling nodes expose their tree via .children (groups/sections) or may be leafs.
		kids = getattr(n, "children", None)
		if isinstance(kids, list) and kids:
			stack.extend(reversed(kids))

# ---------- custom picture serializer -> 
class AnnotationPictureSerializer(MarkdownPictureSerializer):
	@override
	def serialize(
		self,
		*,
		item: PictureItem,
		doc_serializer: BaseDocSerializer,
		doc: DoclingDocument,
		**kwargs: Any,
	) -> SerializationResult:
		text_parts: list[str] = []
		for annotation in item.annotations:
			if isinstance(annotation, PictureClassificationData):
				predicted_class = (
					annotation.predicted_classes[0].class_name
					if annotation.predicted_classes
					else None
				)
				if predicted_class is not None:
					text_parts.append(f"Picture type: {predicted_class}")
			elif isinstance(annotation, PictureMoleculeData):
				text_parts.append(f"SMILES: {annotation.smi}")
			elif isinstance(annotation, PictureDescriptionData):
				text_parts.append(f"Picture description: {annotation.text}")

		# Prefer Docling's cref if present; fall back to index
		cref = getattr(getattr(item, "self_ref", None), "cref", None)
		idx = None
		if cref and cref.startswith("#/pictures/"):
			try: idx = int(cref.split("/")[-1])
			except Exception: idx = None
		if idx is None:
			try:
				idx = doc.pictures.index(item)

			except ValueError:
				# last resort
				pass

		if idx:
			text_parts.insert(0, f'<!-- image id="{"" if idx is None else idx}" -->')

		text_res = "\n".join(text_parts)
		text_res = doc_serializer.post_process(text=text_res)
		return create_ser_result(text=text_res, span_source=item)

class MDTableSerializerProvider(ChunkingSerializerProvider):
	def get_serializer(self, doc: DoclingDocument):
		return ChunkingDocSerializer(
			doc=doc,
			table_serializer=MarkdownTableSerializer(),      # pipe-MD tables
			picture_serializer=AnnotationPictureSerializer(),   # our placeholders
		)

class DoclingParser(DocumentDetector):
	def __init__(self, base64_content: str = None, bytes_content: bytes = None, source_params = None):

		self.source_params = source_params or {}

		"""Initialize parser with base64 encoded content or raw bytes"""
		if base64_content is not None:
			self.bytes_stream = base64.standard_b64decode(base64_content)

		elif bytes_content is not None:
			self.bytes_stream = bytes_content

		else:
			raise ValueError("Either base64_content or bytes_content must be provided")

		if len(self.bytes_stream) < 10:
			raise ValueError("File must be provided")

		super().__init__(self.bytes_stream)
		self.doc_type = self._detect_document_type()


	def _configure_converter(self):
		"""Configure optimized converter for speed"""

		if self.doc_type==".pdf":
			pipeline_options = PdfPipelineOptions()
		
			# this could extract text from images but takes ages
			# prefer to let the summarize / describe step do that bit
			pipeline_options.do_ocr = False
		
			pipeline_options.do_table_structure = True
			pipeline_options.table_structure_options.do_cell_matching = True
		
			# images needed for multimodal LLM
			pipeline_options.generate_picture_images = True

			accelerator_options = AcceleratorOptions()
			accelerator_options.device = AcceleratorDevice.CPU
			accelerator_options.num_threads = 8
		
			doc_converter = DocumentConverter(
				format_options={
					InputFormat.PDF: PdfFormatOption(
						pipeline_options=pipeline_options,
						backend=DoclingParseV2DocumentBackend,
						accelerator_options=accelerator_options
					)
				}
			)
			return doc_converter
	
		elif self.doc_type==".docx":
			doc_converter = DocumentConverter(
				format_options={
					InputFormat.DOCX: WordFormatOption(backend=MsWordDocumentBackend)
				}
			)
			return doc_converter
	
		else:
			doc_converter = DocumentConverter()
			return doc_converter


	def _get_document_source(self) -> DocumentStream:
		"""Create document stream with appropriate extension"""
		buf = io.BytesIO(self.bytes_stream)
		return DocumentStream(name=f"{str(uuid.uuid4())}{self.doc_type}", stream=buf)


	# quick scan
	def detect_page_ranges(self):
	
		# LLM
		user_prompt = [
            { "text": """From the content provided, detect the contiguous page ranges that focus on sustainability/ESG/climate.

Input: list of JSON page excerpts, each with:
  { "page": <int 1-based page>, "excerpt": "<first ~256 chars>" }

Decision rules:
- Treat the TITLE (first line) as a possible heading. If it contains terms like 
  sustainability, ESG, environment, climate, responsible business, non-financial report, etc., 
  then include that page even if the rest of the excerpt is sparse.
- Relevant keywords/aliases (not exhaustive): sustainability, ESG, climate, net zero, emissions, 
  Scope 1/2/3, carbon intensity, TCFD, SASB, GRI, IFRS S1/S2, decarbonization, modern slavery, 
  human rights, biodiversity, waste, water, circular economy, impact.
- Prefer RECALL: include intro/section-heading pages like "Sustainability and Talking about ESG."
- Merge contiguous pages into ranges, allowing small gaps of 1 or 2 pages.
- Usually there will be one contiguous page range in the document focussed on sustainability/ESG/climate.
- It is possible that the entire document is focussed on sustainability/ESG/climate; this would be indicated by "Sustainability Report" in the first pages.

Output JSON in this schema:
{ "ranges": [ { "start": <int>, "end": <int> } ] }

Important formatting rules:
- Output only raw JSON, starting with '{{' and ending with '}}'.
- Do not include ```json or ``` fences.
- Do not include any preamble or explanatory text such as "I'll analyze the content..."
- Do not include any thinking or commentary, before or after the JSON.
""" }
]
		with fitz.open(stream=self.bytes_stream, filetype="pdf") as doc:
			for page_id in range(len(doc)):
				text = doc.load_page(page_id).get_text("text") or ""
				excerpt = text.replace('\n', ' ').replace('\r', '').replace('"', "'")
				
				# Get plain text per page (fast, no additional chunking)
				user_prompt[0]['text'] += f'\n{{ "page": {page_id+1}, "excerpt": "{text[:256]}" }}'
				
			# default
			page_ranges = [(1, len(doc))]

		system = [{"text": "You are an expert in climate sustainability, skilled in summarizing documents for RAG systems."}]
		messages = [{"role": "user", "content": user_prompt}]

		response = BOTO_BEDROCK.converse(
			modelId=LLM_MODEL,
			messages=messages,
			system=system,
			inferenceConfig={
				"temperature": 0.1
			}
		)

		# Converse output shape: resp["output"]["message"]["content"] -> list of blocks
		content_blocks = response.get("output", {}).get("message", {}).get("content", [])
		# Find the first text block
		model_text_parts = [b.get("text") for b in content_blocks if b.get("text")]
		model_text = "\n".join(model_text_parts).strip()

		try:
			match = re.search(r'\{.*\}', model_text, re.DOTALL)
			result = json.loads(match.group(0))
			page_ranges = result.get('ranges')

		except Exception as e:
			print(e)
			
		print('page_ranges', page_ranges)
		self.page_ranges = result.get('ranges')
	

	def parse_documents(self) -> str:
		"""Parse document to markdown with optimized settings"""

		try:

			"""
			ref. https://docling-project.github.io/docling/examples/advanced_chunking_and_serialization/#using-a-custom-strategy
			"""

			source = self._get_document_source()
			converter = self._configure_converter()

			# for amazon embedding
			chunker = HybridChunker(
				max_tokens=8191,
				merge_peers=True,
				serializer_provider=MDTableSerializerProvider()
			)
			
			def markdown_and_images_for_chunk(doc: DoclingDocument, chunk: DocChunk):

				markdown = chunker.contextualize(chunk)
			
				# collect PictureItems by walking the actual nodes covered by the chunk
				seen_refs, images = set(), {}
			
				for di in chunk.meta.doc_items:
					node = _resolve_node(di, doc)
					for n in _walk_descendants(node):
						if isinstance(n, PictureItem):
							cref = getattr(n, "self_ref", None)							
							print('cref', cref)
					
							pid = None
							if cref and cref.startswith("#/pictures/"):
								try: pid = int(cref.split("/")[-1])
								except Exception: pid = None

							if pid is None:
								# fall back to array index to keep map stable
								try: pid = doc.pictures.index(n)
								except ValueError: pass

							if pid and pid not in seen_refs:
								seen_refs.add(pid)
						
								pic = doc.pictures[pid]
								img = pic.get_image(doc)
						
								if img:
									buf = io.BytesIO()
									img.save(buf, format='PNG')
									raw = base64.b64encode(buf.getvalue()).decode("ascii")
									print('img', len(raw), 'bytes')
									images[str(pid)] = raw
								
								else:
									print('cannot derive image')

				return markdown, images

			chunks = []

			if hasattr(self, 'page_ranges'):
				# assemble from page_ranges
				for pr in self.page_ranges:
					conversion_result = converter.convert(source, page_range=(pr['start'], pr['end']))
					doc = conversion_result.document
					chunks += chunker.chunk(dl_doc=doc)

			else:
				# everything
				conversion_result = converter.convert(source)
				doc = conversion_result.document
				chunks = chunker.chunk(dl_doc=doc)

			for chunk_idx, chunk in enumerate(chunks):
				markdown, images = markdown_and_images_for_chunk(doc, chunk)
		
				pages = set()
				for item in chunk.meta.doc_items:
					for prov in getattr(item, "prov", []) or []:
						if hasattr(prov, "page_no") and prov.page_no is not None:
							pages.add(prov.page_no)
					
				params = {
					"chunk_idx": chunk_idx, 
					"pages": sorted(pages),
					"content_md": markdown, 
					"images_base64": images,
				}
				if self.source_params: params.update(self.source_params)
			
				payload = dietPayload(params)
		
				print('enqueue', payload[:512])
				#response = BOTO_SQS.send_message(QueueUrl=SQS_DOCLING_CHUNK, MessageBody=payload)
				logger.info(response)
	
		except Exception as e:
			logger.error(f"Error parsing document: {str(e)}")
			raise DocumentFormatError(f"Failed to parse document: {str(e)}")


# make sure it's under 1MB for sqs and to keep LLM reasonable
def dietPayload(params):

	DIET_FACTOR = 0.7
	# Nova Lite limit is 20:1
	MAX_ASPECT_RATIO = 18
	# so we skip decorations
	MIN_WIDTH = 32
	MIN_HEIGHT = 32

	def json_bytes(p):
		length = len(json.dumps(p).encode('utf-8'))
		print('payload', length, 'bytes')
		return length

	def validate_image_and_remove(images):
		"""
		Validates and removes invalid images from a dictionary based on dimensions and
		a non-inverted aspect ratio check (max_dim / min_dim).
		"""
		keys_to_remove = []
		for key, base64_string in images.items():
			try:
				raw = base64.b64decode(base64_string)
				im = Image.open(io.BytesIO(raw))
				width, height = im.size

				# Check for minimum dimensions
				if width < MIN_WIDTH or height < MIN_HEIGHT:
					print(f"Image '{key}' removed: Dimensions ({width}x{height}) too small.")
					keys_to_remove.append(key)
					continue

				# Calculate aspect ratio correctly for both landscape and portrait
				# Always divide the larger dimension by the smaller one
				if width > height:
					aspect_ratio = width / height
				else:
					aspect_ratio = height / width

				if aspect_ratio > MAX_ASPECT_RATIO:
					print(f"Image '{key}' removed: Aspect ratio ({aspect_ratio:.2f}) exceeds max allowed ({MAX_ASPECT_RATIO:.2f}).")
					keys_to_remove.append(key)

			except Exception as e:
				print(f"Could not validate image '{key}': {e}")
				keys_to_remove.append(key)

		for key in keys_to_remove:
			del images[key]

		return bool(images)

	def shrink_one(images):
		# pick largest image by base64 length
		key = max(images, key=lambda k: len(images[k]))
		raw = base64.b64decode(images[key])
		im = Image.open(io.BytesIO(raw))
		w, h = im.size
		new_w, new_h = int(w * DIET_FACTOR), int(h * DIET_FACTOR)
		im = im.resize((max(new_w, 1), max(new_h, 1)), Image.LANCZOS)
		buf = io.BytesIO()
		im.save(buf, format="PNG", optimize=True)
		images[key] = base64.b64encode(buf.getvalue()).decode("ascii")

	# Step 1: Validate and remove invalid images before processing
	if params.get("images_base64"):
		if not validate_image_and_remove(params["images_base64"]):
			print("No valid images found in payload.")
			return json.dumps(params) # Return early if no valid images remain

	# Step 2: Keep shrinking until under 1 MB
	while json_bytes(params) > 1048576 and params.get("images_base64"):
		shrink_one(params["images_base64"])

	return json.dumps(params)


def handle_single_event(event):

	try:
		if event.get('content'):
			content = event['content']

		elif event.get('presignedUrl'):
			logger.info(f"Fetching content...")
			response = requests.get(event['presignedUrl'])
			response.raise_for_status()
			content = response.content
	
			logger.info(f"Successfully downloaded content, size: {len(content)} bytes")

		else:
			logger.error("Missing content")
			return {
				'statusCode': 400,
				'body': 'Missing parameter'
			}

		# Initialize parser with bytes content directly
		source_params = {
			'source': event.get('s3_uri') or event.get('presignedUrl')
		}
		parser = DoclingParser(bytes_content=content, source_params=source_params)
	
		# page ranges only supported on pdf for now
		if parser.doc_type==".pdf":
			parser.detect_page_ranges()

		# parse, chunk and enqueue
		parser.parse_documents()

		return {
			'statusCode': 200,
			'body': 'done'
		}

	except requests.exceptions.RequestException as e:
		logger.error(f"Request error: {str(e)}")
		return {
			'statusCode': 502,
			'body': f'Error fetching document: {str(e)}'
		}

	except Exception as e:
		# Get full stack trace for debugging
		stack_trace = traceback.format_exc()
		logger.error(f"Error processing document: {str(e)}\nStack trace: {stack_trace}")
		return {
			'statusCode': 500,
			'body': f'Processing error: {str(e)}'
		}
