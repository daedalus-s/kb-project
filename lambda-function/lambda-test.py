import boto3
import json
import os
from datetime import datetime

class LambdaTestEnvironment:
    def __init__(self, bucket_name_prefix="test-kb-bucket"):
        """Initialize test environment with unique bucket names"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.input_bucket_name = f"{bucket_name_prefix}-input-{timestamp}"
        self.s3_client = boto3.client('s3')
        self.lambda_client = boto3.client('lambda')
        
    def setup(self):
        """Set up test environment including S3 buckets"""
        # Create test buckets
        self.s3_client.create_bucket(
            Bucket=self.input_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': boto3.session.Session().region_name
            }
        )
        print(f"Created test bucket: {self.input_bucket_name}")
        
        # Create and upload test content
        self._upload_test_content()
        
    def cleanup(self):
        """Clean up test environment"""
        # Delete all objects in buckets
        self._delete_bucket_contents(self.input_bucket_name)
        # Delete buckets
        self.s3_client.delete_bucket(Bucket=self.input_bucket_name)
        print("Cleaned up test environment")
        
    def _delete_bucket_contents(self, bucket_name):
        """Delete all objects in a bucket"""
        try:
            objects = self.s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    self.s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=obj['Key']
                    )
        except Exception as e:
            print(f"Error cleaning up bucket {bucket_name}: {str(e)}")
            
    def _upload_test_content(self):
        """Upload test content to input bucket"""
        # Sample document content
        test_content = {
            "fileContents": [
                {
                    "contentType": "text/plain",
                    "contentMetadata": {
                        "role": "admin",
                        "document_type": "report",
                        "department": "finance"
                    },
                    "contentBody": """
                    First Quarter Financial Report 2024
                    
                    Executive Summary
                    Our company has shown strong performance in Q1 2024, with revenue growth of 15% year-over-year. 
                    Key highlights include expansion in international markets and launch of new product lines.
                    
                    Financial Performance
                    Revenue reached $50M, up from $43.5M in Q1 2023. Gross margin improved to 65%, 
                    compared to 62% in the previous year. Operating expenses remained well controlled at 40% of revenue.
                    
                    Market Analysis
                    Market share in key segments grew by 2.5 percentage points. Customer acquisition costs decreased by 12%, 
                    while customer lifetime value increased by 18%. Our competitive position strengthened in both enterprise and SMB segments.
                    
                    Future Outlook
                    We maintain our full-year guidance and expect continued strong performance in Q2. 
                    Strategic initiatives in AI and cloud services are on track and should contribute to growth in H2 2024.
                    """
                }
            ]
        }
        
        # Upload test content
        self.s3_client.put_object(
            Bucket=self.input_bucket_name,
            Key="test_document.json",
            Body=json.dumps(test_content)
        )
        print("Uploaded test content")
        
    def create_test_event(self):
        """Create a test event for the Lambda function"""
        return {
            "inputFiles": [
                {
                    "contentBatches": [
                        {
                            "key": "test_document.json"
                        }
                    ],
                    "fileMetadata": {
                        "source": "test",
                        "timestamp": datetime.now().isoformat()
                    },
                    "originalFileLocation": {
                        "uri": f"s3://{self.input_bucket_name}/test_document.json"
                    }
                }
            ],
            "bucketName": self.input_bucket_name
        }
        
    def invoke_lambda(self, function_name, test_event):
        """Invoke Lambda function with test event"""
        response = self.lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(test_event)
        )
        return json.loads(response['Payload'].read())
        
    def verify_results(self):
        """Verify the results of the Lambda function"""
        try:
            # List all objects in the bucket
            response = self.s3_client.list_objects_v2(
                Bucket=self.input_bucket_name,
                Prefix='processed/'
            )
            
            if 'Contents' not in response:
                print("No processed content found!")
                return False
                
            # Check processed content
            for obj in response['Contents']:
                content = json.loads(
                    self.s3_client.get_object(
                        Bucket=self.input_bucket_name,
                        Key=obj['Key']
                    )['Body'].read()
                )
                
                print(f"\nVerifying processed content in {obj['Key']}:")
                print(f"Number of chunks: {len(content['fileContents'])}")
                
                # Verify first chunk
                if content['fileContents']:
                    first_chunk = content['fileContents'][0]
                    print("\nSample chunk metadata:")
                    print(json.dumps(first_chunk['contentMetadata'], indent=2))
                    
            # Check metadata files
            metadata_response = self.s3_client.list_objects_v2(
                Bucket=self.input_bucket_name,
                Prefix='metadata/'
            )
            
            if 'Contents' in metadata_response:
                print(f"\nFound {len(metadata_response['Contents'])} metadata files")
                
                # Check first metadata file
                first_metadata = json.loads(
                    self.s3_client.get_object(
                        Bucket=self.input_bucket_name,
                        Key=metadata_response['Contents'][0]['Key']
                    )['Body'].read()
                )
                print("\nSample metadata file content:")
                print(json.dumps(first_metadata, indent=2))
                
            return True
            
        except Exception as e:
            print(f"Error verifying results: {str(e)}")
            return False

# Example usage
if __name__ == "__main__":
    # Initialize test environment
    test_env = LambdaTestEnvironment()
    
    try:
        # Setup test environment
        print("Setting up test environment...")
        test_env.setup()
        
        # Create test event
        test_event = test_env.create_test_event()
        print("\nTest event created:")
        print(json.dumps(test_event, indent=2))
        
        # Invoke Lambda function
        print("\nInvoking Lambda function...")
        lambda_function_name = "kb-hierarchical-chunker"  # Replace with your function name
        result = test_env.invoke_lambda(lambda_function_name, test_event)
        print("\nLambda function response:")
        print(json.dumps(result, indent=2))
        
        # Verify results
        print("\nVerifying results...")
        test_env.verify_results()
        
    finally:
        # Cleanup
        print("\nCleaning up test environment...")
        test_env.cleanup()  