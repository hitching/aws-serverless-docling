# AWS Serverless Docling

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange.svg)](https://aws.amazon.com/lambda/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://www.docker.com/)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-blue.svg)](https://github.com/features/actions)

A serverless document processing solution that deploys [Docling](https://github.com/DS4SD/docling) on AWS Lambda for intelligent document parsing, extraction, and analysis at scale.

## 🚀 What is this?

This project demonstrates how to deploy IBM's Docling document AI on AWS Lambda, enabling you to:
- **Process PDFs, Word docs, PowerPoints** and more
- **Extract text, tables, images** with AI precision  
- **Scale automatically** from zero to thousands of documents
- **Pay only for what you use** with serverless architecture
- **Deploy in minutes** with our step-by-step guide
- **Automated CI/CD** with GitHub Actions for seamless updates

## 📋 Prerequisites

Before starting, make sure you have:

### Required Software
- **Python 3.13+** - [Download here](https://www.python.org/downloads/)
- **Docker** - [Download here](https://www.docker.com/get-started)
- **AWS CLI** - [Installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- **Git** - [Download here](https://git-scm.com/downloads)

### AWS Account Setup
- An AWS account with billing enabled
- AWS CLI configured with credentials:
  ```bash
  aws configure
  AKIA5QH5NBLLESRFLLAC
  2Ko6TOmrJ+27iYZVa3U57bUcyDcVTGaze7SI6djv
  ```
- Required AWS permissions:
  - Lambda functions (create, update, invoke)
  - ECR repositories (create, push images)
  - IAM roles (create execution roles)
  - CloudWatch logs (for monitoring)

## 🛠️ Quick Start (5 Minutes)

### Step 1: Clone and Setup

```bash
# Clone the repository
git clone https://github.com/nikhil/aws-serverless-docling.git
cd aws-serverless-docling

```

### Step 2: Build Docker Image

```bash
# Build the Lambda container for AMD64 architecture
docker buildx build --platform linux/amd64 --provenance=false -f docling/Dockerfile -t aws-serverless-docling:latest .

# Test locally (optional but recommended)
docker run --platform linux/amd64 -p 9000:8080 aws-serverless-docling:latest

# with mounting
docker run --platform linux/amd64 -p 9000:8080 --mount type=bind,source=./docling/lambda_function.py,target=/var/task/lambda_function.py --env-file ./local.env aws-serverless-docling:latest

# In another terminal, test the function:
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"presignedUrl": "https://arxiv.org/pdf/1706.03762"}'

curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"presignedUrl": "https://kesho-source.s3.ap-southeast-2.amazonaws.com/asx_reports/2924-02857465-2A1550794.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=ASIA5QH5NBLLGWCYPZKX%2F20250913%2Fap-southeast-2%2Fs3%2Faws4_request&X-Amz-Date=20250913T064033Z&X-Amz-Expires=300&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLXNvdXRoZWFzdC0yIkcwRQIgJ6zonJLRyfKV3Ij3ub9aw7f2b04BDTRE5i3cEHsyU8gCIQD3Jv8sfzoxC7w275J7k%2Frn3A9Zb0sYj1Q7fsDIPwlp1CrWAghAEAAaDDkyODI0NDM3MDEzNCIMPcIxtyHSZfX3L4kSKrMCXaTDB9cxrlXbyjU3%2BJTn7zjJ7IPENEJX6%2FpgnHXSWDLmso4TONKyqwI4nT3SPdUYzGSf8AkHtDja06I2Osc2lE%2FqG9pyRsPFbarhyi8ILFJIOwX7z8S07EuEI6IlB7P0kLC5aJGsH%2BPNILxEVi%2B1ukIEsyEe8Q6gmEyX%2B9bJH6aWq1NqpNDCKNmYuxFBBlwR6jzFpsaCbCNjlX9%2FwAWTPPp%2BEeMbWZuv4oAxIdDNtG6H0dPx0k89wLNbuqNUmZyI%2BKjZET0AWGew%2BXbFiIDVM9ay%2BCcZFkxS%2FsbW9OV1ZGm8HNDgOQC5RzW%2BHk%2Ft%2BI6ueNy%2Bt75pNaSpUVmfHo87J%2Fr8YHIl78%2BOl5%2FWzRXBPvR49WgULOP3Avay2cu4BHtNEZ5CfOUexEegHow4A%2FDXe3ql%2BjDq05PGBjqtAroVcJmY2Ka%2F6Au1PbUWoV1UDiCvfVyDIqdzedTww8J39Bb0fDauq9DPLW9uMSliGG4thw22cW%2BznxgOV82mIGmO98vDIi5Zkszp8f%2FfCeN9BpEfFqAv3Kk67V1WqvAQVE06hvMd7OYicllfHbcA3l1LLlwEizQ6e53ixQ4UqA9J2rT37Sawho7q6MQH9stKERGlUWFJQdU7USAVP0C%2B9dDg4XjQ1K7kshDlf0yPkdnnFaIeZwS8kQ9%2BfCYI1%2FsCLyhQf8dG58RCUc5eqH7kb%2BuoDHdJqu%2FFgO0L75etO5d2VRRCkQqe%2B8kDeRJpr%2Fw7ltuPdSgon2OZw38oxSgddOrsAy1mY%2FbWEHsJNzTP7LFxJihg3ln3YLLKha2RQ35dMDfBTXLS7yEllumixcw%3D&X-Amz-Signature=23319856d8b18e67d90c697412b0378707c15898a94b31dc2cd82854c22116ba&X-Amz-SignedHeaders=host&response-content-disposition=inline"}'

```

### Step 3: Deploy to AWS

You have two deployment options:

docker buildx build --platform linux/amd64 --provenance=false -f docling/Dockerfile -t aws-serverless-docling:latest .
docker tag aws-serverless-docling:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest
aws lambda update-function-code     --function-name aws-serverless-docling     --image-uri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest


#### Option A: Automated CI/CD (Recommended)

1. **Fork this repository** to your GitHub account

2. **Set up GitHub Secrets** in your repository settings:
   ```
   AWS_ACCESS_KEY_ID: Your AWS access key
   AWS_SECRET_ACCESS_KEY: Your AWS secret key
   AWS_REGION: us-east-1 (or your preferred region)
   ECR_DOCLING_REPOSITORY: aws-serverless-docling
   ```

3. **Create ECR repository** first:
   ```bash
   aws ecr create-repository --repository-name aws-serverless-docling --region us-east-1
   ```

4. **Push to IQA branch** to trigger deployment:
   ```bash
   git checkout -b iqa
   git push origin iqa
   ```

The GitHub Actions workflow will automatically:
- Build the Docker image
- Push to ECR
- Deploy to Lambda (if configured)

#### Option B: Manual Deployment

```bash
# Set your AWS account ID and region
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-1
export REPO_NAME=aws-serverless-docling

# Create ECR repository (mutable type for version updates)
aws ecr create-repository \
    --repository-name $REPO_NAME \
    --image-scanning-configuration scanOnPush=true \
    --image-tag-mutability MUTABLE \
    --region $AWS_REGION

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | \
docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Tag and push image to ECR
docker tag aws-serverless-docling:latest \
$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest

docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest

# Create IAM role for Lambda execution
aws iam create-role \
    --role-name lambda-docling-execution-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# Attach basic execution policy
aws iam attach-role-policy \
    --role-name lambda-docling-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create Lambda function with recommended settings
aws lambda create-function \
    --function-name aws-serverless-docling \
    --package-type Image \
    --code ImageUri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest \
    --role arn:aws:iam::$AWS_ACCOUNT_ID:role/lambda-docling-execution-role \
    --timeout 180 \
    --memory-size 3008 \
    --description "Serverless document processing with Docling"
```

## 🏗️ Project Structure

```
aws-serverless-docling/
├── 📁 docling/                    # Main application code
│   ├── Dockerfile                 # Container definition
│   ├── lambda_function.py         # Lambda handler
│   ├── requirements.txt           # Python dependencies
│   └── utils/                     # Helper functions
├── 📁 .github/workflows/          # CI/CD workflows
│   └── iqa_release.yml           # Automated deployment
├── 📁 tests/                     # Test files
│   ├── test_lambda.py            # Unit tests
│   └── test_integration.py       # Integration tests
├── README.md                     # This documentation
└── .env.example                  # Environment variables template
```

## 🔄 CI/CD Pipeline

The project includes an automated CI/CD pipeline using GitHub Actions:

### Workflow Features
- **Triggered on**: Push to `iqa` branch with changes in `docling/` directory
- **Path filtering**: Only builds when Docling code changes
- **Multi-platform**: Builds for AMD64 architecture
- **ECR integration**: Automatically pushes to Amazon ECR
- **Tagging**: Uses commit SHA and latest tags

### Setting Up CI/CD

1. **Fork the repository**
2. **Add GitHub Secrets**:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_REGION`
   - `ECR_DOCLING_REPOSITORY`

3. **Create ECR repository**:
   ```bash
   aws ecr create-repository --repository-name aws-serverless-docling
   ```

4. **Push to trigger deployment**:
   ```bash
   git checkout -b iqa
   # Make changes to docling/ directory
   git add .
   git commit -m "Update docling implementation"
   git push origin iqa
   ```

## 📖 How to Use

### Direct Lambda Invocation

```bash
# Test the deployed function
aws lambda invoke \
    --function-name aws-serverless-docling \
    --payload '{"presignedUrl": "s3://your-bucket/document.pdf"}' \
    response.json

# Check the response
cat response.json
```

### Python SDK Example

```python
import boto3
import json

# Initialize Lambda client
lambda_client = boto3.client('lambda', region_name='us-east-1')

# Invoke function
response = lambda_client.invoke(
    FunctionName='aws-serverless-docling',
    Payload=json.dumps({
        'presignedUrl': 's3://your-bucket/document.pdf'
    })
)

# Parse response
result = json.loads(response['Payload'].read())
print(f"Processing result: {result}")
```

### API Gateway Integration (Optional)

For HTTP API access, you can integrate with API Gateway:

```bash
# Example HTTP request after API Gateway setup
curl -X POST https://your-api-gateway-url/process \
  -H "Content-Type: application/json" \
  -d '{"presignedUrl": "s3://your-bucket/document.pdf"}'
```

## ⚙️ Configuration

### Environment Variables

Set these in your Lambda function configuration:

```bash
# Lambda environment variables
TORCH_HOME=/tmp/torch
LOG_LEVEL=INFO
MAX_DOCUMENT_SIZE=50000000     # 50MB in bytes
TIMEOUT_SECONDS=180           # 3 minutes
ENABLE_TABLE_EXTRACTION=true
ENABLE_IMAGE_EXTRACTION=true
OUTPUT_FORMAT=json            # json, markdown, or text
```

### Lambda Settings

Recommended configuration for optimal performance:

| Setting | Value | Reason |
|---------|-------|--------|
| Memory | 3008 MB | Docling requires significant RAM |
| Timeout | 3 minutes (180s) | Document processing time |
| Storage | 10 GB | For temporary file processing |
| Architecture | x86_64 | Better compatibility |

## 🚨 Troubleshooting

### Common Issues

**1. Memory Errors**
```
Error: Runtime exited with error: signal: killed
```
*Solution*: Increase Lambda memory allocation to 3008 MB

**2. Timeout Issues**
```
Task timed out after X seconds
```
*Solution*: Increase timeout to 180+ seconds for document processing

**3. Docker Build Issues**
```
Platform mismatch error
```
*Solution*: Use `--platform linux/amd64` flag when building

**4. Cold Start Performance**
- First invocation may take 30+ seconds
- Consider provisioned concurrency for production
- Implement warm-up strategies for consistent performance

**5. CI/CD Pipeline Issues**
```
Error: Could not assume role
```
*Solution*: Check AWS credentials in GitHub Secrets

### Debug Commands

```bash
# Check Lambda logs
aws logs filter-log-events \
    --log-group-name /aws/lambda/aws-serverless-docling \
    --start-time $(date -d '1 hour ago' +%s)000

# Update function configuration
aws lambda update-function-configuration \
    --function-name aws-serverless-docling \
    --memory-size 3008 \
    --timeout 300

# Check GitHub Actions logs
# Go to Actions tab in your GitHub repository
```

## 🧪 Testing

### Local Testing

```bash
# Build and test locally
docker buildx build --platform linux/amd64 --provenance=false -f docling/Dockerfile -t test-docling .
docker run -p 9000:8080 test-docling

# Test with sample document
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"body": "{\"presignedUrl\": \"s3://test-bucket/sample.pdf\"}"}'
```

### Integration Testing

```bash
# Test deployed Lambda function
aws lambda invoke \
    --function-name aws-serverless-docling \
    --payload '{"test": true}' \
    test-response.json
```

### Optimization Tips

1. **Right-size memory allocation** based on your document types
2. **Use appropriate timeout** settings
3. **Implement request batching** for multiple documents
4. **Monitor and optimize** cold start frequency
5. **Use provisioned concurrency** for production workloads

## 🤝 Contributing

We welcome contributions! Here's how to get started:

### Development Workflow

1. **Fork the repository**
2. **Create a feature branch**:
   ```bash
   git checkout -b feature/amazing-feature
   ```
3. **Make your changes** in the `docling/` directory
4. **Test locally** using Docker
5. **Push to your fork**:
   ```bash
   git push origin feature/amazing-feature
   ```
6. **Create a Pull Request**

### CI/CD for Contributors

- Changes to `docling/` directory trigger automated builds
- The `iqa` branch is used for integration testing
- Pull requests are automatically tested

### Development Guidelines

- Follow Python PEP 8 style guide
- Add unit tests for new features
- Update README for significant changes
- Test Docker builds locally before submitting
- Ensure CI/CD pipeline passes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
MIT License

Copyright (c) 2024 Nikhil

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## 🙏 Acknowledgments

- **[IBM Research](https://github.com/DS4SD/docling)** for creating Docling
- **AWS Lambda Team** for serverless infrastructure
- **Docker Community** for containerization tools
- **GitHub Actions** for CI/CD capabilities
- **Open Source Contributors** who make projects like this possible

---
---
