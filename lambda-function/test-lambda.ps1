# test-lambda.ps1
$ErrorActionPreference = "Stop"

# Get AWS configuration
$REGION = aws configure get region
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$SOURCE_BUCKET = "kb-data-$REGION-$ACCOUNT_ID"
$INTERMEDIATE_BUCKET = "kb-intermediate-$REGION-$ACCOUNT_ID"

Write-Host "Using source bucket: $SOURCE_BUCKET"
Write-Host "Using intermediate bucket: $INTERMEDIATE_BUCKET"

# Verify buckets exist
try {
    Write-Host "Checking source bucket..."
    aws s3api head-bucket --bucket $SOURCE_BUCKET
    Write-Host "Source bucket exists"
    
    Write-Host "Checking intermediate bucket..."
    aws s3api head-bucket --bucket $INTERMEDIATE_BUCKET
    Write-Host "Intermediate bucket exists"
} catch {
    Write-Host "Error checking buckets: $_"
    exit 1
}

# Create and upload test document
Write-Host "Creating test document..."
$testContent = @'
This is a test document.
It contains multiple paragraphs that will be processed
using hierarchical chunking.

This is the second paragraph.
It will help demonstrate how the chunking works
across multiple sections of text.

And here is the final paragraph.
This will show how the parent-child relationships
are maintained in the chunk metadata.
'@

$testContent | Out-File -FilePath "test-document.txt" -Encoding utf8
Write-Host "Test document created"

Write-Host "Uploading test document to S3..."
aws s3 cp test-document.txt "s3://$SOURCE_BUCKET/test-document.txt"
Write-Host "Test document uploaded"

# Create test event
Write-Host "Creating test event..."
$testEvent = @"
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
"@

$testEvent | Out-File -FilePath "test-event.json" -Encoding utf8
Write-Host "Test event created"

# Test the Lambda function
Write-Host "Invoking Lambda function..."
try {
    $result = aws lambda invoke `
        --function-name kb-hierarchical-chunker `
        --payload (Get-Content test-event.json -Raw) `
        --log-type Tail `
        response.json

    # Decode and display CloudWatch logs
    $logOutput = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($result.LogResult))
    Write-Host "`nCloud Watch Logs:"
    Write-Host $logOutput

    # Check if response file exists and display content
    if (Test-Path response.json) {
        Write-Host "`nLambda Response:"
        Get-Content response.json | ConvertFrom-Json | ConvertTo-Json -Depth 10
        
        # List the created chunks
        Write-Host "`nChunks created in intermediate bucket:"
        aws s3 ls "s3://$INTERMEDIATE_BUCKET/processed/test-document.txt/" --recursive
    } else {
        Write-Host "No response file was created!"
        Write-Host "Lambda invocation result:"
        $result | ConvertTo-Json
    }
} catch {
    Write-Host "Error invoking Lambda: $_"
    
    # Try to get the most recent error logs
    Write-Host "`nAttempting to get recent Lambda logs..."
    $logGroupName = "/aws/lambda/kb-hierarchical-chunker"
    
    aws logs get-log-events `
        --log-group-name $logGroupName `
        --log-stream-name (aws logs describe-log-streams `
            --log-group-name $logGroupName `
            --order-by LastEventTime `
            --descending `
            --limit 1 `
            --query 'logStreams[0].logStreamName' `
            --output text) `
        --query 'events[*].message' `
        --output text
    
    exit 1
}