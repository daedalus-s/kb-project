# Get AWS account ID
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text

# Get region
$REGION = aws configure get region

# Create unique suffix
$SUFFIX = "$REGION-$ACCOUNT_ID"

# Create S3 buckets
aws s3 mb "s3://kb-data-$SUFFIX" --region $REGION
aws s3 mb "s3://kb-intermediate-$SUFFIX" --region $REGION

# Create IAM role
aws iam create-role `
    --role-name kb-hierarchical-chunker-role `
    --assume-role-policy-document file://trust-policy.json

# Create and attach S3 policy
aws iam create-policy `
    --policy-name kb-s3-access-policy `
    --policy-document file://s3-policy.json

# Get policy ARN
$POLICY_ARN = "arn:aws:iam::${ACCOUNT_ID}:policy/kb-s3-access-policy"

# Attach policies
aws iam attach-role-policy `
    --role-name kb-hierarchical-chunker-role `
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam attach-role-policy `
    --role-name kb-hierarchical-chunker-role `
    --policy-arn $POLICY_ARN