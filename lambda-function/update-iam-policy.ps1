# update-iam-policy.ps1
$ErrorActionPreference = "Stop"

# Get AWS configuration
$REGION = aws configure get region
$ACCOUNT_ID = aws sts get-caller-identity --query "Account" --output text
$LAMBDA_ROLE = "kb-hierarchical-chunker-role"

# Define policy content with proper escaping
$policyContent = @{
    Version = "2012-10-17"
    Statement = @(
        @{
            Effect = "Allow"
            Action = @(
                "s3:GetObject"
                "s3:PutObject"
                "s3:ListBucket"
            )
            Resource = @(
                "arn:aws:s3:::kb-data-$REGION-$ACCOUNT_ID/*"
                "arn:aws:s3:::kb-data-$REGION-$ACCOUNT_ID"
                "arn:aws:s3:::kb-intermediate-$REGION-$ACCOUNT_ID/*"
                "arn:aws:s3:::kb-intermediate-$REGION-$ACCOUNT_ID"
            )
        }
        @{
            Effect = "Allow"
            Action = @(
                "logs:CreateLogGroup"
                "logs:CreateLogStream"
                "logs:PutLogEvents"
            )
            Resource = @(
                "arn:aws:logs:$REGION:$ACCOUNT_ID:log-group:/aws/lambda/*"
            )
        }
    )
}

Write-Host "Creating policy document..."
$policyJson = $policyContent | ConvertTo-Json -Depth 10
$policyJson | Out-File -FilePath "lambda-policy.json" -Encoding utf8

# Create new policy
$POLICY_NAME = "kb-hierarchical-chunker-policy"
$POLICY_ARN = "arn:aws:iam::${ACCOUNT_ID}:policy/$POLICY_NAME"

Write-Host "Creating policy $POLICY_NAME..."
try {
    # Delete existing policy if it exists
    try {
        Write-Host "Checking for existing policy..."
        $existingPolicy = aws iam get-policy --policy-arn $POLICY_ARN
        
        Write-Host "Deleting existing policy versions..."
        $versions = aws iam list-policy-versions --policy-arn $POLICY_ARN | ConvertFrom-Json
        foreach ($version in $versions.Versions) {
            if (-not $version.IsDefaultVersion) {
                aws iam delete-policy-version --policy-arn $POLICY_ARN --version-id $version.VersionId
            }
        }
        
        Write-Host "Deleting existing policy..."
        aws iam delete-policy --policy-arn $POLICY_ARN
        Start-Sleep -Seconds 5  # Wait for deletion to propagate
    } catch {
        Write-Host "No existing policy found, proceeding with creation..."
    }

    Write-Host "Creating new policy..."
    $createPolicyResult = aws iam create-policy `
        --policy-name $POLICY_NAME `
        --policy-document file://lambda-policy.json | ConvertFrom-Json
    
    $POLICY_ARN = $createPolicyResult.Policy.Arn
    Write-Host "Created policy with ARN: $POLICY_ARN"
} catch {
    Write-Host "Error creating policy: $_"
    exit 1
}

# Detach existing policies
Write-Host "Detaching existing policies..."
$existingPolicies = aws iam list-attached-role-policies --role-name $LAMBDA_ROLE | ConvertFrom-Json
foreach ($policy in $existingPolicies.AttachedPolicies) {
    Write-Host "Detaching policy: $($policy.PolicyName)"
    aws iam detach-role-policy `
        --role-name $LAMBDA_ROLE `
        --policy-arn $policy.PolicyArn
}

# Wait for detachment to complete
Start-Sleep -Seconds 5

# Attach the new policy
Write-Host "Attaching new policy to role..."
aws iam attach-role-policy `
    --role-name $LAMBDA_ROLE `
    --policy-arn $POLICY_ARN

# Attach AWS Lambda basic execution role
Write-Host "Attaching AWSLambdaBasicExecutionRole..."
aws iam attach-role-policy `
    --role-name $LAMBDA_ROLE `
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

Write-Host "Waiting for changes to propagate..."
Start-Sleep -Seconds 10

Write-Host "`nVerifying final role configuration..."
$finalPolicies = aws iam list-attached-role-policies --role-name $LAMBDA_ROLE | ConvertFrom-Json
Write-Host "Attached policies:"
foreach ($policy in $finalPolicies.AttachedPolicies) {
    Write-Host "- $($policy.PolicyName) ($($policy.PolicyArn))"
}

# Display the created policy document
Write-Host "`nCreated policy document:"
Get-Content "lambda-policy.json"