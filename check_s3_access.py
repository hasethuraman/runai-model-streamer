#!/usr/bin/env python3
"""
Check S3 access and verify the file exists.
"""
import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configuration
S3_BUCKET = 'amzn-haritest-bucket'
S3_PATH = 'DD-vector-v2.safetensors'

print("=" * 60)
print("S3 Access Diagnostic")
print("=" * 60)

# Check credentials
aws_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-1')

if not aws_key or not aws_secret:
    print("✗ AWS credentials not found in environment variables")
    print("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
    sys.exit(1)

print(f"✓ AWS_ACCESS_KEY_ID: {aws_key[:10]}...")
print(f"✓ AWS_SECRET_ACCESS_KEY: {'*' * 20}")
print(f"✓ AWS_REGION: {aws_region}")
print()

# Try to access S3
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=aws_region
    )
    
    # Test 1: List buckets
    print("Test 1: Listing buckets...")
    try:
        response = s3_client.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        print(f"✓ Found {len(buckets)} buckets")
        
        if S3_BUCKET in buckets:
            print(f"✓ Bucket '{S3_BUCKET}' exists")
        else:
            print(f"✗ Bucket '{S3_BUCKET}' NOT found")
            print(f"Available buckets: {', '.join(buckets)}")
    except ClientError as e:
        print(f"✗ Failed to list buckets: {e}")
    
    print()
    
    # Test 2: Get bucket region
    print(f"Test 2: Getting bucket region for '{S3_BUCKET}'...")
    try:
        location = s3_client.get_bucket_location(Bucket=S3_BUCKET)
        bucket_region = location['LocationConstraint'] or 'us-east-1'
        print(f"✓ Bucket region: {bucket_region}")
        
        if bucket_region != aws_region:
            print(f"⚠️  WARNING: Bucket is in {bucket_region} but using region {aws_region}")
            print(f"   Set: export AWS_REGION={bucket_region}")
    except ClientError as e:
        print(f"✗ Failed to get bucket location: {e}")
    
    print()
    
    # Test 3: Check if file exists
    print(f"Test 3: Checking if file exists: s3://{S3_BUCKET}/{S3_PATH}")
    try:
        response = s3_client.head_object(Bucket=S3_BUCKET, Key=S3_PATH)
        file_size = response['ContentLength']
        print(f"✓ File exists!")
        print(f"  Size: {file_size / 1e6:.2f} MB")
        print(f"  Last Modified: {response['LastModified']}")
        print(f"  Content Type: {response.get('ContentType', 'N/A')}")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"✗ File NOT found at s3://{S3_BUCKET}/{S3_PATH}")
            print()
            print("Listing files in bucket...")
            try:
                response = s3_client.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=20)
                if 'Contents' in response:
                    print(f"Found {len(response['Contents'])} files:")
                    for obj in response['Contents']:
                        print(f"  - {obj['Key']} ({obj['Size'] / 1e6:.2f} MB)")
                else:
                    print("  Bucket is empty")
            except Exception as list_err:
                print(f"  Could not list bucket: {list_err}")
        else:
            print(f"✗ Error checking file: {e}")
    
    print()
    print("=" * 60)
    print("Summary:")
    print("=" * 60)
    print("If all tests passed, S3 access is working correctly.")
    print("If file was not found, check the bucket name and file path.")
    
except NoCredentialsError:
    print("✗ No AWS credentials found")
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
