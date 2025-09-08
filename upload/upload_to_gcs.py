from google.cloud import storage

def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}.")

if __name__ == "__main__":
    bucket_name = "weather-pipeline-bucket"
    source_file_path = "extraction/output/weather_data.json"
    destination_blob_name = "extraction/weather_data.json"
    upload_to_gcs(bucket_name, source_file_path, destination_blob_name)
