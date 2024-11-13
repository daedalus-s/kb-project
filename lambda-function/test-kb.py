import boto3
import json
import logging
from botocore.exceptions import ClientError
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KnowledgeBaseTestSuite:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.lambda_client = boto3.client('lambda')
        self.iam_client = boto3.client('iam')
        self.source_bucket = "company-data-rag"
        self.intermediate_bucket = "kb-intermediate-us-east-1-824116678613"
        self.lambda_function_name = "kb-hierarchical-chunker"
        
    def test_bucket_permissions(self):
        """Test S3 bucket access permissions"""
        logger.info("Testing S3 bucket permissions...")
        
        try:
            # Test source bucket read access
            logger.info(f"Testing read access to source bucket: {self.source_bucket}")
            self.s3_client.head_bucket(Bucket=self.source_bucket)
            logger.info("✓ Source bucket exists and is accessible")
            
            # Test specific file read access
            try:
                self.s3_client.head_object(Bucket=self.source_bucket, Key="file3.txt")
                logger.info("✓ Test file is accessible in source bucket")
            except ClientError as e:
                logger.error(f"✗ Cannot access test file: {str(e)}")
            
            # Test intermediate bucket write access
            logger.info(f"Testing write access to intermediate bucket: {self.intermediate_bucket}")
            test_key = "test_write_permission.txt"
            try:
                self.s3_client.put_object(
                    Bucket=self.intermediate_bucket,
                    Key=test_key,
                    Body="test content"
                )
                logger.info("✓ Successfully wrote to intermediate bucket")
                # Clean up test file
                self.s3_client.delete_object(Bucket=self.intermediate_bucket, Key=test_key)
            except ClientError as e:
                logger.error(f"✗ Cannot write to intermediate bucket: {str(e)}")
                
        except ClientError as e:
            logger.error(f"✗ Bucket access error: {str(e)}")
            return False
        
        return True
    
    def test_lambda_role(self):
        """Test Lambda function role and permissions"""
        logger.info("Testing Lambda function role...")
        
        try:
            # Get Lambda function configuration
            lambda_config = self.lambda_client.get_function(
                FunctionName=self.lambda_function_name
            )
            role_arn = lambda_config['Configuration']['Role']
            role_name = role_arn.split('/')[-1]
            
            logger.info(f"Found Lambda role: {role_name}")
            
            # Get role policies
            attached_policies = self.iam_client.list_attached_role_policies(
                RoleName=role_name
            )
            inline_policies = self.iam_client.list_role_policies(
                RoleName=role_name
            )
            
            logger.info("Checking policies...")
            for policy in attached_policies['AttachedPolicies']:
                logger.info(f"Attached policy: {policy['PolicyName']}")
            
            for policy_name in inline_policies['PolicyNames']:
                policy = self.iam_client.get_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name
                )
                logger.info(f"Inline policy: {policy_name}")
                
            return True
            
        except ClientError as e:
            logger.error(f"✗ Error checking Lambda role: {str(e)}")
            return False
    
    def test_lambda_invocation(self):
        """Test Lambda function invocation"""
        logger.info("Testing Lambda function invocation...")
        
        test_event = {
            "inputFiles": [
                {
                    "contentBatches": [
                        {
                            "key": "file3.txt"
                        }
                    ],
                    "fileMetadata": {
                        "role": "admin"
                    },
                    "originalFileLocation": {
                        "uri": f"s3://{self.source_bucket}/file3.txt"
                    }
                }
            ],
            "bucketName": self.source_bucket
        }
        
        try:
            response = self.lambda_client.invoke(
                FunctionName=self.lambda_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            # Read the response payload
            response_payload = json.loads(response['Payload'].read())
            
            if 'FunctionError' in response:
                logger.error(f"✗ Lambda function returned an error: {response_payload}")
                return False
            
            logger.info("✓ Lambda function invoked successfully")
            logger.info(f"Response payload: {json.dumps(response_payload, indent=2)}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error invoking Lambda function: {str(e)}")
            return False
    
    def run_all_tests(self):
        """Run all tests"""
        logger.info("Starting comprehensive tests...")
        
        tests = [
            ("S3 Bucket Permissions", self.test_bucket_permissions),
            ("Lambda Role", self.test_lambda_role),
            ("Lambda Invocation", self.test_lambda_invocation)
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\nRunning test: {test_name}")
            logger.info("=" * 50)
            results[test_name] = test_func()
            logger.info("=" * 50)
        
        # Print summary
        logger.info("\nTest Summary:")
        logger.info("=" * 50)
        for test_name, result in results.items():
            status = "✓ PASSED" if result else "✗ FAILED"
            logger.info(f"{test_name}: {status}")
            
        return all(results.values())

# Run the tests
if __name__ == "__main__":
    test_suite = KnowledgeBaseTestSuite()
    success = test_suite.run_all_tests()
    
    if success:
        logger.info("\nAll tests passed successfully!")
    else:
        logger.error("\nSome tests failed. Please check the logs above for details.")