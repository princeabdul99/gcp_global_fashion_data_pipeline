from google.cloud import storage
from pathlib import Path
from google.api_core.exceptions import GoogleAPIError
import sys


PROJECT_ID = "<your-project_id>"
BUCKET_NAME = "<your-bucket-name>"


def load_data_to_gcs(source_file_path:str, folder_name:str, destination_filename:str | None = None):

    """
    Upload a CSV file to a specific folder in a GCS bucket.

    :param source_file_path: Local CSV file path
    :param folder_name: Target folder in GCS (e.g. 'raw/customers')
    :param destination_filename: Optional override for filename in GCS
    """

    try :
        source_path = Path(source_file_path)
        if not source_path:
            raise FileNotFoundError(f"Source file not found: {source_path}")

        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(BUCKET_NAME)

        print(f"Connected to Bucket {bucket.name}")

        
        filename = destination_filename or source_path.name
        destination_blob_name = f"{folder_name.rstrip('/')}/{filename}"

        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_path)

        print(
            f"Uploaded {source_file_path} "
            f"to gs://{BUCKET_NAME}/{destination_blob_name}"
        ) 

    except FileNotFoundError as e:
        print(f"FILE ERROR: {e}", file=sys.stderr)  
        raise

    except GoogleAPIError as e:
        print(f"GCS API ERROR: {e}", file=sys.stderr)  
        raise


    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)  
        raise



if __name__ == '__main__':
    load_data_to_gcs(
        source_file_path="/data/transactions/transaction-2023-01.csv.gz",
        folder_name="transactions"    
    )

