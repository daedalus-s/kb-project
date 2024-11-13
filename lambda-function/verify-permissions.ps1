# verify-permissions.ps1
$ErrorActionPreference = "Stop"

# Get AWS configuration
$REGION = aws configure get region
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$LAMBDA_ROLE = "kb-hierarchical-chunker-role"

Write-Host "Checking Lambda execution role..."
try {
    $role = aws iam get-role --role-name $LAMBDA_ROLE | ConvertFrom-Json
    Write-Host "Found role: $($role.Role.Arn)"
    
    Write-Host "`nChecking attached policies..."
    $policies = aws iam list-attached-role-policies --role-name $LAMBDA_ROLE | ConvertFrom-Json
    
    foreach ($policy in $policies.AttachedPolicies) {
        Write-Host "`nPolicy: $($policy.PolicyName)"
        $policyContent = aws iam get-policy-version `
            --policy-arn $policy.PolicyArn `
            --version-id (aws iam get-policy --policy-arn $policy.PolicyArn | 
                ConvertFrom-Json).Policy.DefaultVersionId |
            ConvertFrom-Json
        
        $policyContent.PolicyVersion.Document | ConvertTo-Json -Depth 10
    }
} catch {
    Write-Host "Error checking role: $_"
    exit 1
}

Write-Host "`nVerifying S3 bucket permissions..."
$SOURCE_BUCKET = "kb-data-$REGION-$ACCOUNT_ID"
$INTERMEDIATE_BUCKET = "kb-intermediate-$REGION-$ACCOUNT_ID"

foreach ($bucket in @($SOURCE_BUCKET, $INTERMEDIATE_BUCKET)) {
    Write-Host "`nChecking bucket: $bucket"
    try {
        aws s3api get-bucket-policy --bucket $bucket
    } catch {
        Write-Host "No bucket policy found (this might be OK if using IAM roles)"
    }
    
    try {
        Write-Host "Checking bucket ACL..."
        aws s3api get-bucket-acl --bucket $bucket
    } catch {
        Write-Host "Error checking bucket ACL: $_"
    }
}