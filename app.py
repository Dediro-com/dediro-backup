import os
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import uvicorn
import boto3
import csv
import json
import re
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get values from environment variables
mongo_uri = os.getenv('MONGO_URI')
bucket_name = os.getenv('BUCKET_NAME')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize the S3 client with credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# CSV file to save the metadata
csv_file_path = 's3_objects_metadata.csv'

# Function to list all objects in the S3 bucket
def list_s3_objects(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return response.get('Contents', [])

# Function to save S3 metadata to CSV
def save_s3_metadata_to_csv(objects, csv_file_path):
    header = ['Key', 'LastModified', 'ETag', 'Size', 'StorageClass']
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        writer.writeheader()
        for obj in objects:
            writer.writerow({
                'Key': obj.get('Key'),
                'LastModified': obj.get('LastModified'),
                'ETag': obj.get('ETag'),
                'Size': obj.get('Size'),
                'StorageClass': obj.get('StorageClass')
            })
    print(f"Metadata of S3 objects saved to {csv_file_path}")

# Function to load existing S3 metadata from CSV
def load_existing_metadata(csv_file_path):
    if not os.path.exists(csv_file_path):
        return []
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        return [row['Key'] for row in reader]

# Function to extract collection name from file name
def get_collection_name(file_name):
    return re.sub(r'_\d+\.json$', '', file_name)

# Function to upload JSON file contents to MongoDB
def upload_json_to_mongodb(file_path, mongo_uri, database_name):
    client = MongoClient(mongo_uri)
    db = client[database_name]

    base_name = os.path.basename(file_path)
    collection_name = get_collection_name(base_name)

    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    collection = db[collection_name]
    result = collection.insert_one(data)
    print(f"Document from {file_path} inserted into collection '{collection_name}' with ID: {result.inserted_id}")


# Function to check for new S3 objects and process them
async def check_s3_and_process():
    try:
        print("Executing check_s3_and_process...")
        existing_objects = load_existing_metadata(csv_file_path)
        current_objects = list_s3_objects(bucket_name)
        print(f"Found {len(current_objects)} objects in S3 bucket.")
        new_objects = [obj for obj in current_objects if obj['Key'] not in existing_objects]

        if new_objects:
            print(f"Found {len(new_objects)} new objects.")
        else:
            print("No new objects found.")

        for new_object in new_objects:
            file_name = new_object['Key']
            local_file_path = os.path.join('/tmp', file_name)  # Temporary local path
            print(f"Downloading {file_name}...")
            s3_client.download_file(bucket_name, file_name, local_file_path)
            print(f"Uploading {file_name} to MongoDB...")
            upload_json_to_mongodb(local_file_path, mongo_uri, database_name)
            os.remove(local_file_path)  # Clean up the local file after uploading

        print("S3 check and processing completed.")
    except Exception as e:
        print(f"Error occurred: {e}")



@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting scheduler setup...")
    scheduler = AsyncIOScheduler(timezone='UTC')

    # Schedule the task every 30 seconds
    scheduler.add_job(check_s3_and_process, 'interval', seconds=10)
    print("Scheduled check_s3_and_process job every 10 seconds.")
    
    scheduler.start()
    print("Scheduler started.")

    yield

    print("Shutting down scheduler...")
    scheduler.shutdown()
    print("Scheduler shut down.")

app = FastAPI(lifespan=lifespan)
@app.get("/")
async def read_root():
    return {"message": "Server is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
