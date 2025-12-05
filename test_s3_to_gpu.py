#!/usr/bin/env python3
"""
Test script to stream a model file from S3 directly to GPU memory.
"""
import os
import sys
import torch
from runai_model_streamer import SafetensorsStreamer

# Configuration - Update these with your S3 details
S3_BUCKET = 'amzn-haritest-bucket'
S3_PATH = 'DD-vector-v2.safetensors'  # e.g., 'models/model.safetensors'

# AWS credentials (or set these as environment variables)
# os.environ['AWS_ACCESS_KEY_ID'] = 'your-access-key'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'your-secret-key'
# os.environ['AWS_REGION'] = 'us-east-1'  # Optional

def test_s3_to_gpu_streaming():
    """Test streaming from S3 to GPU memory."""
    
    # Check if CUDA is available
    if not torch.cuda.is_available():
        print("ERROR: CUDA is not available. GPU streaming requires a CUDA-enabled GPU.")
        sys.exit(1)
    
    print(f"CUDA available: {torch.cuda.is_available()}")
    print(f"GPU device: {torch.cuda.get_device_name(0)}")
    print(f"GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    print()
    
    # Construct S3 URI
    file_path = f"s3://{S3_BUCKET}/{S3_PATH}"
    print(f"Streaming from: {file_path}")
    print()
    
    # Stream file from S3 to GPU
    try:
        with SafetensorsStreamer() as streamer:
            print("Initiating stream...")
            streamer.stream_file(file_path)
            
            tensor_count = 0
            total_size = 0
            
            print("Streaming tensors to GPU:")
            for name, tensor in streamer.get_tensors():
                # Transfer tensor to GPU
                gpu_tensor = tensor.to('cuda:0')
                
                tensor_size = tensor.numel() * tensor.element_size()
                total_size += tensor_size
                tensor_count += 1
                
                print(f"  [{tensor_count}] {name}: shape={tuple(tensor.shape)}, "
                      f"dtype={tensor.dtype}, size={tensor_size / 1e6:.2f} MB, "
                      f"device={gpu_tensor.device}")
                
                # Optional: Verify tensor is on GPU
                assert gpu_tensor.is_cuda, f"Tensor {name} is not on GPU!"
                
            print()
            print(f"✓ Success! Streamed {tensor_count} tensors ({total_size / 1e6:.2f} MB total) from S3 to GPU")
            
    except Exception as e:
        print(f"✗ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Verify AWS credentials are set
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        print("WARNING: AWS credentials not found in environment variables.")
        print("Make sure to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or")
        print("uncomment the credential lines in the script.")
        print()
    
    test_s3_to_gpu_streaming()
