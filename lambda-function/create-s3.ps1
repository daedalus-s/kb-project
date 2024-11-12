# Create a sample text file
@'
This is a test document.
It will be used to verify the hierarchical chunking functionality.
The Lambda function should split this into appropriate chunks
and maintain the hierarchical structure.
'@ | Out-File -FilePath test-document.txt -Encoding utf8

# Upload to S3
$BUCKET_NAME = "kb-data-$REGION-$ACCOUNT_ID"
aws s3 cp test-document.txt "s3://kb-data-us-east-1-824116678613/test-document.txt"