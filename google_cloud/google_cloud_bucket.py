from google.cloud import storage
import os
import dotenv

dotenv.load_dotenv()

# set key credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/aryangoel/Documents/GitHub/ONDC_M_N/google_cloud/calm-bliss-413606-debad2afa679.json'

def list_buckets():
    """Lists all buckets."""

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        print(bucket.name)
# define function that uploads a file from the bucket
def upload_blob( source_file_name, destination_file_name , bucket_name = os.environ["BUCKET_NAME"] ): 
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

def download_blob( file_name, destination_file_name , bucket_name = os.environ["BUCKET_NAME"]): 
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)

    return True
def delete_blob(blob_name , bucket_name = os.environ["BUCKET_NAME"]):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    generation_match_precondition = None

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to delete is aborted if the object's
    # generation number does not match your precondition.
    blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    print(f"Blob {blob_name} deleted.")
# if __name__ == "__main__" : 
#     # list_buckets()
#     # upload_cs_file ( "/Users/aryangoel/Documents/GitHub/ONDC_M_N/dataset.csv" , "data/data.csv" ) 
#     # print ( download_cs_file("data/data.csv" , "artifacts/data.csv" )  )
#     list_buckets()
#     delete_blob("data/data.csv") 
