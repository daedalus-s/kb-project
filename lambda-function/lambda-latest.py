import json
from abc import abstractmethod, ABC
from typing import List
from urllib.parse import urlparse
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class Chunker(ABC):
    @abstractmethod
    def chunk(self, text: str) -> List[str]:
        raise NotImplementedError()
        
class SimpleChunker(Chunker):
    def chunk(self, text: str) -> List[str]:
        words = text.split()
        return [' '.join(words[i:i+100]) for i in range(0, len(words), 100)]

def get_source_role(s3_client, uri):
    try:
        # Remove s3:// prefix and split into bucket/key
        uri = uri.replace('s3://', '')
        bucket, key = uri.split('/', 1)
        
        # Get object metadata
        response = s3_client.head_object(Bucket=bucket, Key=key)
        logger.debug(f"Source object metadata: {json.dumps(response.get('Metadata', {}))}")
        return response.get('Metadata', {}).get('role', '')
    except Exception as e:
        logger.error(f"Error fetching source role from {uri}: {str(e)}")
        return ''

def lambda_handler(event, context):
    logger.debug('input={}'.format(json.dumps(event)))
    s3 = boto3.client('s3')

    # Extract relevant information from the input event
    input_files = event.get('inputFiles')
    input_bucket = event.get('bucketName')

    if not all([input_files, input_bucket]):
        raise ValueError("Missing required input parameters")
    
    output_files = []
    chunker = SimpleChunker()

    for input_file in input_files:
        content_batches = input_file.get('contentBatches', [])
        file_metadata = input_file.get('fileMetadata', {})
        original_file_location = input_file.get('originalFileLocation', {})

        processed_batches = []
        
        for batch in content_batches:
            input_key = batch.get('key')

            if not input_key:
                raise ValueError("Missing uri in content batch")
            
            # Read file from S3
            file_content = read_s3_file(s3, input_bucket, input_key)
            
            # Process content (chunking) with source metadata
            chunked_content = process_content(file_content, chunker, s3)
            
            output_key = f"Output/{input_key}"
            
            # Write processed content back to S3
            write_to_s3(s3, input_bucket, output_key, chunked_content)
            
            # Add processed batch information
            processed_batches.append({
                'key': output_key
            })
        
        # Prepare output file information
        output_file = {
            'originalFileLocation': original_file_location,
            'fileMetadata': file_metadata,
            'contentBatches': processed_batches
        }
        output_files.append(output_file)
    
    result = {'outputFiles': output_files}
    return result

def read_s3_file(s3_client, bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))

def write_to_s3(s3_client, bucket, key, content):
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(content)
    )

def process_content(file_content: dict, chunker: Chunker, s3_client) -> dict:
    chunked_content = {
        'fileContents': []
    }
    
    for content in file_content.get('fileContents', []):
        content_body = content.get('contentBody', '')
        content_type = content.get('contentType', '')
        content_metadata = content.get('contentMetadata', {})
        
        # Debug logging for content metadata
        logger.debug(f"Processing content with metadata: {json.dumps(content_metadata)}")
        
        # Get source URI and role
        source_uri = content_metadata.get('x-amz-bedrock-kb-source-uri', '')
        if source_uri:
            role = get_source_role(s3_client, source_uri)
            if role:
                content_metadata['role'] = role
                logger.debug(f"Added role '{role}' from source URI {source_uri}")
        
        words = content['contentBody']
        chunks = chunker.chunk(words)
        
        for chunk in chunks:
            chunked_content['fileContents'].append({
                'contentType': content_type,
                'contentMetadata': content_metadata, 
                'contentBody': chunk
            })
    
    return chunked_content