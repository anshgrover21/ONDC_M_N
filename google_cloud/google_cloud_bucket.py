from google.cloud import storage
import os
import pickle
import sys 

from exception import CustomException

# set key credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'google_cloud/gcp_config.json'

async def upload_blob( obj, destination_file_name , bucket_name = "mxndatabucket" ): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    serialized_obj = pickle.dumps(obj)
    blob.upload_from_string(serialized_obj , content_type="application/octet-stream")

    return True

def upload_an_object ( filename,file_content , content_type, bucket_name =  "mxndatabucket") :
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename) 
    blob.upload_from_string(file_content, content_type=content_type)
    return {"message": "File uploaded successfully to GCS"}

async def download_blob( file_name, destination_file_name , bucket_name = "mxndatabucket"): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)

    return True
def read_from_blob(blob_name : str , bucket_name : str = "mxndatabucket"):
    try : 
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name = blob_name)
        serialized_data = blob.download_as_string()
        if not serialized_data:
            return None 
        
        # Deserialize the pickled object
        deserialized_obj = pickle.loads(serialized_data)
        
        return deserialized_obj
    except Exception as e : 
        raise CustomException(e , sys )


def delete_blob(blob_name , bucket_name = "mxndatabucket"):
    """Deletes a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    generation_match_precondition = None

    blob.reload() 
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    print(f"Blob {blob_name} deleted.")

