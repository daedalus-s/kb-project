# test-simple.ps1
$ErrorActionPreference = "Stop"

# Get AWS configuration
$REGION = aws configure get region
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$SOURCE_BUCKET = "kb-data-$REGION-$ACCOUNT_ID"
$INTERMEDIATE_BUCKET = "kb-intermediate-$REGION-$ACCOUNT_ID"

Write-Host "Testing S3 permissions..."

# Test source bucket
Write-Host "`nTesting source bucket ($SOURCE_BUCKET)..."
"test content" | Out-File -FilePath "test.txt" -Encoding utf8
try {
    aws s3 cp test.txt "s3://$SOURCE_BUCKET/test.txt"
    Write-Host "✓ Write to source bucket successful"
    
    aws s3 cp "s3://$SOURCE_BUCKET/test.txt" "test-download.txt"
    Write-Host "✓ Read from source bucket successful"
} catch {
    Write-Host "✗ Error with source bucket: $_"
}

# Test intermediate bucket
Write-Host "`nTesting intermediate bucket ($INTERMEDIATE_BUCKET)..."
try {
    aws s3 cp test.txt "s3://$INTERMEDIATE_BUCKET/test.txt"
    Write-Host "✓ Write to intermediate bucket successful"
    
    aws s3 cp "s3://$INTERMEDIATE_BUCKET/test.txt" "test-download2.txt"
    Write-Host "✓ Read from intermediate bucket successful"
} catch {
    Write-Host "✗ Error with intermediate bucket: $_"
}

# Clean up
Remove-Item -Path "test.txt" -ErrorAction SilentlyContinue
Remove-Item -Path "test-download.txt" -ErrorAction SilentlyContinue
Remove-Item -Path "test-download2.txt" -ErrorAction SilentlyContinue
aws s3 rm "s3://$SOURCE_BUCKET/test.txt"
aws s3 rm "s3://$INTERMEDIATE_BUCKET/test.txt"

Write-Host "`nTesting complete!"