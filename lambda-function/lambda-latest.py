import json
import boto3
import logging
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_original_key_from_location(file_location: dict) -> str:
    """Extract original file name from the originalFileLocation"""
    try:
        if file_location.get('type') == 'S3':
            s3_uri = file_location.get('s3_location', {}).get('uri', '')
            if s3_uri:
                parsed = urlparse(s3_uri)
                key = parsed.path.lstrip('/')
                logger.debug(f"Extracted original key: {key} from URI: {s3_uri}")
                return key
    except Exception as e:
        logger.error(f"Error extracting original key from location: {str(e)}")
    return ''

def get_source_metadata(s3_client, object_key: str) -> str:
    """Get role metadata from original source object"""
    try:
        logger.debug(f"Attempting to get metadata for object: {object_key}")
        
        response = s3_client.head_object(
            Bucket="company-data-rag",
            Key=object_key
        )
        
        logger.debug(f"Full S3 response metadata: {json.dumps(response.get('Metadata', {}))}")
        
        role = response.get('Metadata', {}).get('role')
        if role is None:
            logger.warning(f"No 'role' found in metadata for {object_key}")
            return ''
        elif role == '':
            logger.warning(f"Empty role value found in metadata for {object_key}")
            return ''
        
        logger.info(f"Successfully retrieved role '{role}' for {object_key}")
        return role
        
    except Exception as e:
        logger.error(f"Error getting metadata for {object_key}: {str(e)}")
        return ''

def process_content(file_content: dict, role: str) -> dict:
    """Add role to metadata JSON string in each content item"""
    processed_content = {
        'fileContents': []
    }
    
    for content in file_content.get('fileContents', []):
        content_metadata = content.get('contentMetadata', {})
        
        try:
            logger.debug(f"Original metadata: {content_metadata}")
            
            metadata_str = content_metadata.get('metadata', '{}')
            metadata_dict = json.loads(metadata_str)
            
            if role:
                metadata_dict['role'] = role
                logger.debug(f"Added role '{role}' to metadata")
            else:
                logger.warning("Skipping empty role value")
            
            content_metadata['metadata'] = json.dumps(metadata_dict)
            logger.debug(f"Updated metadata JSON: {content_metadata['metadata']}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing metadata JSON: {str(e)}")
            content_metadata['metadata'] = json.dumps({'role': role}) if role else '{}'
        
        processed_content['fileContents'].append({
            'contentType': content.get('contentType', ''),
            'contentMetadata': content_metadata,
            'contentBody': content.get('contentBody', '')
        })
    
    return processed_content

def lambda_handler(event, context):
    logger.debug(f'Input event: {json.dumps(event)}')
    s3 = boto3.client('s3')

    input_files = event.get('inputFiles')
    input_bucket = event.get('bucketName')

    if not all([input_files, input_bucket]):
        raise ValueError("Missing required input parameters")
    
    output_files = []

    for input_file in input_files:
        content_batches = input_file.get('contentBatches', [])
        original_file_location = input_file.get('originalFileLocation', {})

        # Get original key from originalFileLocation
        original_key = get_original_key_from_location(original_file_location)
        if not original_key:
            logger.error("Could not determine original file key")
            continue
            
        logger.debug(f"Original file key: {original_key}")

        processed_batches = []
        
        for batch in content_batches:
            input_key = batch.get('key')
            if not input_key:
                logger.error("Missing key in content batch")
                continue
            
            # Get role from source object
            role = get_source_metadata(s3, original_key)
            if not role:
                logger.warning(f"No role metadata found for {original_key}")
            
            # Read and process intermediate file
            file_content = read_s3_file(s3, input_bucket, input_key)
            processed_content = process_content(file_content, role)
            
            output_key = f"Output/{input_key}"
            write_to_s3(s3, input_bucket, output_key, processed_content)
            
            processed_batches.append({
                'key': output_key
            })
        
        output_file = {
            'originalFileLocation': original_file_location,
            'fileMetadata': input_file.get('fileMetadata', {}),
            'contentBatches': processed_batches
        }
        output_files.append(output_file)
    
    result = {'outputFiles': output_files}
    logger.debug(f"Output result: {json.dumps(result)}")
    return result

def read_s3_file(s3_client, bucket, key):
    """Read JSON content from S3"""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))

def write_to_s3(s3_client, bucket, key, content):
    """Write content to S3"""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(content)
    )