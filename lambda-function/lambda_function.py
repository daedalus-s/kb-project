import json
import boto3
from typing import List, Dict, Any
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class HierarchicalChunker:
    def __init__(self, parent_chunk_size: int = 1500, child_chunk_size: int = 300, overlap: int = 60):
        self.parent_chunk_size = parent_chunk_size
        self.child_chunk_size = child_chunk_size
        self.overlap = overlap
    
    def create_chunks(self, text: str) -> Dict[str, List[str]]:
        """
        Creates hierarchical chunks from text with parent and child chunks
        """
        words = text.split()
        parent_chunks = []
        
        for i in range(0, len(words), self.parent_chunk_size):
            end_idx = min(i + self.parent_chunk_size, len(words))
            parent_chunk = ' '.join(words[i:end_idx])
            parent_chunks.append(parent_chunk)
        
        all_child_chunks = []
        parent_to_children = {}
        
        for parent_idx, parent_chunk in enumerate(parent_chunks):
            parent_words = parent_chunk.split()
            child_chunks = []
            
            for i in range(0, len(parent_words), self.child_chunk_size - self.overlap):
                end_idx = min(i + self.child_chunk_size, len(parent_words))
                child_chunk = ' '.join(parent_words[i:end_idx])
                child_chunks.append(child_chunk)
                all_child_chunks.append(child_chunk)
            
            parent_to_children[f"parent_{parent_idx}"] = child_chunks
        
        return {
            "parents": parent_chunks,
            "children": all_child_chunks,
            "hierarchy": parent_to_children
        }

def get_source_bucket_and_key(uri: str) -> tuple:
    """Extract bucket and key from S3 URI"""
    # Remove 's3://' prefix
    path = uri.replace('s3://', '')
    # Split into bucket and key
    parts = path.split('/', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid S3 URI format: {uri}")
    return parts[0], parts[1]

def write_chunk_and_metadata(s3_client: boto3.client, 
                           bucket: str,
                           chunk_info: Dict[str, Any],
                           base_key: str,
                           chunk_num: int) -> Dict[str, str]:
    """Write chunk content and its metadata to S3"""
    # Write the raw chunk content
    chunk_key = f"{base_key}/chunk_{chunk_num}"
    s3_client.put_object(
        Bucket=bucket,
        Key=chunk_key,
        Body=chunk_info['content']
    )
    
    # Create metadata file
    metadata_key = f"{base_key}/chunk_{chunk_num}.metadata.json"
    metadata_content = {
        "metadataAttributes": {
            **chunk_info['metadata']
        }
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata_content)
    )
    
    return {
        'key': chunk_key,
        'metadata_key': metadata_key
    }

def process_content(content: str, metadata: Dict[str, Any], chunker: HierarchicalChunker) -> List[Dict[str, Any]]:
    """Process content and create hierarchical chunks with metadata"""
    chunks = chunker.create_chunks(content)
    processed_chunks = []
    
    # Process parent chunks
    for idx, parent_chunk in enumerate(chunks['parents']):
        chunk_id = f"parent_{idx}"
        
        # Store parent chunk and metadata
        chunk_info = {
            'content': parent_chunk,
            'metadata': {
                **metadata,
                'chunkType': 'parent',
                'chunkId': chunk_id,
                'childChunks': [f"child_{i}" for i in range(len(chunks['hierarchy'][chunk_id]))]
            }
        }
        processed_chunks.append(chunk_info)
        
        # Process child chunks for this parent
        child_chunks = chunks['hierarchy'][chunk_id]
        for child_idx, child_chunk in enumerate(child_chunks):
            child_id = f"child_{child_idx}"
            child_info = {
                'content': child_chunk,
                'metadata': {
                    **metadata,
                    'chunkType': 'child',
                    'chunkId': child_id,
                    'parentChunk': chunk_id
                }
            }
            processed_chunks.append(child_info)
    
    return processed_chunks

def lambda_handler(event, context):
    try:
        logger.debug('Input event: %s', json.dumps(event))
        
        if not event.get('inputFiles') or not event.get('bucketName'):
            raise ValueError("Missing required input parameters")
        
        s3 = boto3.client('s3')
        output_files = []
        
        for input_file in event['inputFiles']:
            file_metadata = input_file.get('fileMetadata', {})
            original_location = input_file.get('originalFileLocation', {}).get('uri')
            
            if not original_location:
                logger.warning("Missing original file location")
                continue
                
            try:
                # Get source bucket and key
                source_bucket, source_key = get_source_bucket_and_key(original_location)
                
                # Read the original file
                response = s3.get_object(Bucket=source_bucket, Key=source_key)
                content = response['Body'].read().decode('utf-8')
                
                # Process content
                chunker = HierarchicalChunker()
                processed_chunks = process_content(content, file_metadata, chunker)
                
                # Write chunks to intermediate bucket
                processed_batches = []
                base_key = f"processed/{source_key}"
                
                for idx, chunk_info in enumerate(processed_chunks):
                    keys = write_chunk_and_metadata(
                        s3_client=s3,
                        bucket=event['bucketName'],
                        chunk_info=chunk_info,
                        base_key=base_key,
                        chunk_num=idx
                    )
                    processed_batches.append({'key': keys['key']})
                
                # Add to output files
                output_files.append({
                    'originalFileLocation': input_file['originalFileLocation'],
                    'fileMetadata': file_metadata,
                    'contentBatches': processed_batches
                })
                
            except Exception as e:
                logger.error(f"Error processing file {original_location}: {str(e)}")
                continue
        
        return {
            'outputFiles': output_files
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        raise