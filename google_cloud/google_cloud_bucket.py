from google.cloud import storage
import os
import dotenv

dotenv.load_dotenv()

# set key credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'google_cloud/gcp_config.json'

# def list_blob_from_bucket(bucket_name = os.environ["BUCKET_NAME"] ):
#     """Lists all buckets."""

#     storage_client = storage.Client()

#     blobs = storage_client.list_blobs(bucket_name)
#     return blobs

# define function that uploads a file from the bucket
async def upload_blob( source_file_name, destination_file_name , bucket_name = os.environ["BUCKET_NAME"] ): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)

    return True

def upload_an_object ( filename,file_content , content_type, bucket_name = os.environ["BUCKET_NAME"] ) :
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename) 
    blob.upload_from_string(file_content, content_type=content_type)
    return {"message": "File uploaded successfully to GCS"}

async def download_blob( file_name, destination_file_name , bucket_name = os.environ["BUCKET_NAME"]): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)

    return True
def delete_blob(blob_name , bucket_name = os.environ["BUCKET_NAME"]):
    """Deletes a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    generation_match_precondition = None

    blob.reload() 
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    print(f"Blob {blob_name} deleted.")

