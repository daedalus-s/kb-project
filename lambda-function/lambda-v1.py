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

def lambda_handler(event, context):
    logger.debug('input={}'.format(json.dumps(event)))
    s3 = boto3.client('s3')

    # Extract relevant information from the input event
    input_files = event.get('inputFiles')
    source_bucket = event.get('bucketName')
    intermediate_bucket = "kb-intermediate-us-east-1-824116678613"
    
    if not all([input_files, source_bucket]):
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
            text_content = read_s3_file(s3, source_bucket, input_key)
            
            # Process content (chunking)
            chunks = chunker.chunk(text_content)
            
            # Write chunks and metadata
            base_filename = input_key.rsplit('.', 1)[0]
            
            for i, chunk in enumerate(chunks):
                # Create filenames for chunk and metadata
                chunk_filename = f"{base_filename}_chunk_{i}.txt"
                metadata_filename = f"{input_key}.metadata.json"  # Metadata file named after original file
                
                # Write chunk content to intermediate bucket
                write_to_s3(
                    s3,
                    intermediate_bucket,
                    chunk_filename,
                    chunk,
                    is_text=True
                )
                
                # Write metadata to source bucket
                metadata_content = {
                    "metadataAttributes": {
                        "role": file_metadata.get('role', 'default'),
                        "chunk_type": "content",
                        "chunk_number": i,
                        "total_chunks": len(chunks),
                        "original_file": input_key
                    }
                }
                
                # Write metadata to source bucket
                write_to_s3(
                    s3,
                    source_bucket,  # Write metadata to source bucket
                    metadata_filename,
                    metadata_content,
                    is_text=False
                )
                
                processed_batches.append({
                    'key': chunk_filename,
                    'metadata_key': metadata_filename
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
    """Read raw text content from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        logger.error(f"Error reading from {bucket}/{key}: {str(e)}")
        raise

def write_to_s3(s3_client, bucket, key, content, is_text=False):
    """Write content to S3, either as text or JSON"""
    try:
        if is_text:
            body = content
        else:
            body = json.dumps(content)
        
        logger.debug(f"Writing to bucket: {bucket}, key: {key}")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body
        )
    except Exception as e:
        logger.error(f"Error writing to {bucket}/{key}: {str(e)}")
        raise