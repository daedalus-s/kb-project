# Get account ID and role ARN
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$ROLE_ARN = "arn:aws:iam::${ACCOUNT_ID}:role/kb-hierarchical-chunker-role"

# Create the function
aws lambda create-function `
    --function-name kb-hierarchical-chunker `
    --runtime python3.9 `
    --handler lambda_function.lambda_handler `
    --role $ROLE_ARN `
    --zip-file fileb://function.zip `
    --timeout 300 `
    --memory-size 512