import json
import boto3
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

MAX_CHUNK_LENGTH = 45000

def read_s3_file(s3_client, bucket: str, key: str) -> str:
    try:
        logger.debug(f"Reading from {bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        logger.error(f"Error reading from {bucket}/{key}: {str(e)}")
        raise

def chunk_text(text: str, max_length: int = MAX_CHUNK_LENGTH) -> List[str]:
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0
    
    for word in words:
        word_length = len(word) + (1 if current_chunk else 0)
        
        if current_length + word_length > max_length and current_chunk:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_length = len(word)
        else:
            current_chunk.append(word)
            current_length += word_length
    
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    return chunks

def create_vector_metadata(source_file: str, chunk_index: int, total_chunks: int, 
                         file_metadata: Dict) -> Dict:
    """Create metadata object including role information"""
    return {
        "source": f"s3://{source_file}",
        "format": "text",
        "chunkIndex": chunk_index,
        "totalChunks": total_chunks,
        "timestamp": datetime.utcnow().isoformat(),
        "role": file_metadata.get('role', 'default'),  # Including role in metadata
        "x-amz-bedrock-kb-source-uri": f"s3://{source_file}",
        "x-amz-bedrock-kb-data-source-id": file_metadata.get('dataSourceId', '')
    }

def lambda_handler(event, context):
    try:
        logger.debug(f"Received event: {json.dumps(event, indent=2)}")
        s3 = boto3.client('s3')

        if not event.get('inputFiles'):
            raise ValueError("Missing inputFiles in event")

        output_files = []
        
        for input_file in event['inputFiles']:
            try:
                content_batches = input_file.get('contentBatches', [])
                file_metadata = input_file.get('fileMetadata', {})
                source_bucket = event.get('bucketName')
                
                if not source_bucket:
                    raise ValueError("Missing source bucket")

                processed_batches = []
                
                for batch in content_batches:
                    input_key = batch.get('key')
                    if not input_key:
                        continue

                    # Read source content
                    source_content = read_s3_file(s3, source_bucket, input_key)
                    content_chunks = chunk_text(source_content)
                    
                    # Process each chunk
                    for chunk_index, chunk_content in enumerate(content_chunks):
                        # Create chunk filename
                        base_key = input_key.rsplit('.', 1)[0]
                        chunk_key = f"{base_key}_chunk_{chunk_index}.json"
                        
                        # Create output with metadata including role
                        output_content = {
                            "fileContents": [
                                {
                                    "contentBody": chunk_content,
                                    "contentType": "text/plain",
                                    "contentMetadata": create_vector_metadata(
                                        f"{source_bucket}/{input_key}",
                                        chunk_index,
                                        len(content_chunks),
                                        file_metadata
                                    )
                                }
                            ]
                        }

                        # Write to S3
                        s3.put_object(
                            Bucket=source_bucket,
                            Key=chunk_key,
                            Body=json.dumps(output_content),
                            ContentType='application/json'
                        )

                        processed_batches.append({
                            'key': chunk_key
                        })

                output_files.append({
                    'originalFileLocation': input_file.get('originalFileLocation', {}),
                    'fileMetadata': file_metadata,
                    'contentBatches': processed_batches
                })

            except Exception as e:
                logger.error(f"Error processing file: {str(e)}")
                logger.error(traceback.format_exc())
                raise

        result = {'outputFiles': output_files}
        return result

    except Exception as e:
        logger.error(f"Lambda function error: {str(e)}")
        logger.error(traceback.format_exc())
        raise