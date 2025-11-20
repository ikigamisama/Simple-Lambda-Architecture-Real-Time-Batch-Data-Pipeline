import os
import boto3
import json
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()


class S3:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            service_name="s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            region_name="us-east-1",
        )

    def create_bucket(self):
        try:
            self.s3.create_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' created")
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise e

    def bucket_exists(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            return False

    def upload_file(self, file_path, object_name=None):
        object_name = object_name or os.path.basename(file_path)
        try:
            self.s3.upload_file(file_path, self.bucket_name, object_name)
        except ClientError as e:
            print(f"Upload failed: {e}")

    def download_file(self, object_name, file_path=None):
        file_path = file_path or object_name
        try:
            self.s3.download_file(self.bucket_name, object_name, file_path)
        except ClientError as e:
            print(f"Download failed: {e}")

    def list_objects(self, prefix=''):
        try:
            resp = self.s3.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix)
            return [obj['Key'] for obj in resp.get('Contents', [])]
        except ClientError as e:
            print(f"List objects failed: {e}")
            return []

    def object_exists(self, object_name):
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=object_name)
            return True
        except ClientError:
            return False

    def read_json(self, object_name):
        try:
            resp = self.s3.get_object(Bucket=self.bucket_name, Key=object_name)
            return json.loads(resp['Body'].read())
        except ClientError as e:
            print(f"Read JSON failed: {e}")
            return None

    def write_file(self, object_name, data, contentType='application/json'):
        try:
            if isinstance(data, BytesIO):
                body = data.getvalue()  # Extract bytes from BytesIO
            elif isinstance(data, bytes):
                body = data
            else:
                body = json.dumps(data, indent=2)

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=object_name,
                Body=body,
                ContentType=contentType
            )
        except ClientError as e:
            print(f"Write File failed: {e}")

    def get_object(self, object_name):
        try:
            return self.s3.get_object(Bucket=self.bucket_name, Key=object_name)
        except ClientError as e:
            print(f"Get object failed: {e}")
            return None

    def delete_object(self, object_name):
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=object_name)
        except ClientError as e:
            print(f"Delete object failed: {e}")
