# Create test document and upload to source bucket
$REGION = aws configure get region
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$SOURCE_BUCKET = "kb-data-$REGION-$ACCOUNT_ID"
$INTERMEDIATE_BUCKET = "kb-intermediate-$REGION-$ACCOUNT_ID"

# Create test document
@'
This is a test document.
It contains multiple paragraphs that will be processed
using hierarchical chunking.

This is the second paragraph.
It will help demonstrate how the chunking works
across multiple sections of text.

And here is the final paragraph.
This will show how the parent-child relationships
are maintained in the chunk metadata.
'@ | Out-File -FilePath test-document.txt -Encoding utf8

# Upload to source bucket
aws s3 cp test-document.txt "s3://$SOURCE_BUCKET/test-document.txt"

# Create test event
@"
{
    "inputFiles": [
        {
            "fileMetadata": {
                "role": "document",
                "documentType": "text"
            },
            "originalFileLocation": {
                "uri": "s3://$SOURCE_BUCKET/test-document.txt"
            }
        }
    ],
    "bucketName": "$INTERMEDIATE_BUCKET"
}
"@ | Out-File -FilePath test-event.json -Encoding utf8

# Test the Lambda function
aws lambda invoke `
    --function-name kb-hierarchical-chunker `
    --payload (Get-Content test-event.json -Raw) `
    response.json

# Check the response
Get-Content response.json

# Verify the chunks in the intermediate bucket
aws s3 ls "s3://$INTERMEDIATE_BUCKET/processed/test-document.txt/" --recursive