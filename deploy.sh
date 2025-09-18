# Set your AWS account ID and region
#export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_ACCOUNT_ID=928244370134
export AWS_REGION=ap-southeast-2
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
    
aws lambda update-function-code \
    --function-name aws-serverless-docling \
    --image-uri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:latest