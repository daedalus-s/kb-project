# verify-setup.ps1
$ErrorActionPreference = "Stop"

# Get AWS configuration
$REGION = aws configure get region
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$SOURCE_BUCKET = "kb-data-$REGION-$ACCOUNT_ID"
$INTERMEDIATE_BUCKET = "kb-intermediate-$REGION-$ACCOUNT_ID"
$LAMBDA_FUNCTION = "kb-hierarchical-chunker"
$LAMBDA_ROLE = "kb-hierarchical-chunker-role"

Write-Host "=== Verifying AWS Configuration ==="
Write-Host "Region: $REGION"
Write-Host "Account ID: $ACCOUNT_ID"
Write-Host ""

Write-Host "=== Verifying S3 Buckets ==="
foreach ($bucket in @($SOURCE_BUCKET, $INTERMEDIATE_BUCKET)) {
    Write-Host "Checking bucket: $bucket"
    try {
        aws s3api head-bucket --bucket $bucket
        Write-Host "✓ Bucket exists"
        
        # Test write permission
        Write-Host "Testing write permission..."
        aws s3api put-object `
            --bucket $bucket `
            --key "test-write-permission.txt" `
            --body "test"
        
        Write-Host "Testing read permission..."
        aws s3api get-object `
            --bucket $bucket `
            --key "test-write-permission.txt" `
            test-read.txt
        
        Write-Host "Testing delete permission..."
        aws s3api delete-object `
            --bucket $bucket `
            --key "test-write-permission.txt"
        
        Remove-Item -Path test-read.txt -ErrorAction SilentlyContinue
        Write-Host "✓ Bucket permissions verified"
    } catch {
        Write-Host "✗ Error with bucket $bucket : $_"
    }
    Write-Host ""
}

Write-Host "=== Verifying Lambda Function ==="
try {
    $lambda = aws lambda get-function --function-name $LAMBDA_FUNCTION | ConvertFrom-Json
    Write-Host "✓ Lambda function exists"
    Write-Host "Runtime: $($lambda.Configuration.Runtime)"
    Write-Host "Memory: $($lambda.Configuration.MemorySize)MB"
    Write-Host "Timeout: $($lambda.Configuration.Timeout)s"
    Write-Host "Role: $($lambda.Configuration.Role)"
} catch {
    Write-Host "✗ Error with Lambda function: $_"
}
Write-Host ""

Write-Host "=== Verifying Lambda Role ==="
try {
    $role = aws iam get-role --role-name $LAMBDA_ROLE | ConvertFrom-Json
    Write-Host "✓ Role exists: $($role.Role.Arn)"
    
    Write-Host "Attached Policies:"
    $policies = aws iam list-attached-role-policies --role-name $LAMBDA_ROLE | ConvertFrom-Json
    foreach ($policy in $policies.AttachedPolicies) {
        Write-Host "- $($policy.PolicyName)"
    }
} catch {
    Write-Host "✗ Error with IAM role: $_"
}
Write-Host ""

Write-Host "=== Testing Lambda Function ==="
# Create test event
$testEvent = @"
{
    "inputFiles": [
        {
            "fileMetadata": {
                "role": "test",
                "documentType": "text"
            },
            "originalFileLocation": {
                "uri": "s3://$SOURCE_BUCKET/test.txt"
            }
        }
    ],
    "bucketName": "$INTERMEDIATE_BUCKET"
}
"@

$testEvent | Out-File -FilePath "test-event.json" -Encoding utf8

# Create test file
"This is a test document." | Out-File -FilePath "test.txt" -Encoding utf8
aws s3 cp test.txt "s3://$SOURCE_BUCKET/test.txt"

Write-Host "Invoking Lambda..."
try {
    $result = aws lambda invoke `
        --function-name $LAMBDA_FUNCTION `
        --payload (Get-Content test-event.json -Raw) `
        --log-type Tail `
        response.json
    
    Write-Host "Lambda Response:"
    Get-Content response.json
    
    Write-Host "`nLambda Logs:"
    [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($result.LogResult))
} catch {
    Write-Host "✗ Error invoking Lambda: $_"
}

# Cleanup
Remove-Item -Path "test.txt" -ErrorAction SilentlyContinue
Remove-Item -Path "test-event.json" -ErrorAction SilentlyContinue
aws s3 rm "s3://$SOURCE_BUCKET/test.txt"