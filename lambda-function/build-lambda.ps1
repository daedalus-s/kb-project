# Create build-lambda.ps1
$ErrorActionPreference = "Stop"

# Ensure we're in the right directory
$currentDir = Get-Location
Write-Host "Current directory: $currentDir"

# Create temporary build directory
New-Item -ItemType Directory -Force -Path .\build | Out-Null
Set-Location .\build

# Create a temporary requirements file
Copy-Item ..\requirements.txt .\requirements.txt

# Create python directory structure
New-Item -ItemType Directory -Force -Path .\python\lib\python3.9\site-packages | Out-Null

# Convert Windows path to Docker path format
$windowsPath = (Get-Location).Path.Replace('\', '/')
$dockerPath = $windowsPath.Replace('C:', '/c')

Write-Host "Installing dependencies using Docker..."
docker run --rm -v "${dockerPath}:/var/task" public.ecr.aws/lambda/python:3.9 `
    pip install -r requirements.txt -t python/lib/python3.9/site-packages/

# Copy the Lambda function
Write-Host "Copying Lambda function..."
Copy-Item ..\lambda_function.py .\python\lib\python3.9\site-packages\

# Create ZIP file
Write-Host "Creating ZIP file..."
Compress-Archive -Path .\python\lib\python3.9\site-packages\* -DestinationPath ..\function.zip -Force

# Clean up
Write-Host "Cleaning up..."
Set-Location ..
Remove-Item -Recurse -Force .\build

Write-Host "Build complete! Created function.zip"