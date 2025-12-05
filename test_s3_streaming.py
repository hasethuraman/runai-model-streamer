#!/usr/bin/env python3
"""
Test script to stream a model file from S3 to CPU memory.
This version works without GPU/CUDA.
"""
import os
import sys
import torch
from runai_model_streamer import SafetensorsStreamer

# Configuration - Update these with your S3 details
S3_BUCKET = 'amzn-haritest-bucket'
S3_PATH = 'DD-vector-v2.safetensors'

# AWS credentials (or set these as environment variables)
# os.environ['AWS_ACCESS_KEY_ID'] = 'your-access-key'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'your-secret-key'
# os.environ['AWS_REGION'] = 'us-east-1'  # Optional

def test_s3_streaming():
    """Test streaming from S3 to memory."""
    
    # Check device availability
    device = 'cuda:0' if torch.cuda.is_available() else 'cpu'
    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")
    print(f"Target device: {device}")
    
    if torch.cuda.is_available():
        print(f"GPU device: {torch.cuda.get_device_name(0)}")
        print(f"GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    else:
        print("Running on CPU (no GPU detected)")
    print()
    
    # Construct S3 URI
    file_path = f"s3://{S3_BUCKET}/{S3_PATH}"
    print(f"Streaming from: {file_path}")
    print()
    
    # Stream file from S3
    try:
        with SafetensorsStreamer() as streamer:
            print("Initiating stream...")
            streamer.stream_file(file_path)
            
            tensor_count = 0
            total_size = 0
            
            print(f"Streaming tensors to {device.upper()}:")
            for name, tensor in streamer.get_tensors():
                # Transfer tensor to target device
                device_tensor = tensor.to(device)
                
                tensor_size = tensor.numel() * tensor.element_size()
                total_size += tensor_size
                tensor_count += 1
                
                print(f"  [{tensor_count}] {name}: shape={tuple(tensor.shape)}, "
                      f"dtype={tensor.dtype}, size={tensor_size / 1e6:.2f} MB, "
                      f"device={device_tensor.device}")
                
                # Verify tensor is on the correct device
                expected_device_type = 'cuda' if torch.cuda.is_available() else 'cpu'
                assert device_tensor.device.type == expected_device_type, \
                    f"Tensor {name} is not on {expected_device_type}!"
                
            print()
            print(f"✓ Success! Streamed {tensor_count} tensors ({total_size / 1e6:.2f} MB total) from S3 to {device.upper()}")
            print(f"✓ S3 streaming is working correctly!")
            
    except Exception as e:
        print(f"✗ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Verify AWS credentials are set
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        print("WARNING: AWS credentials not found in environment variables.")
        print("Make sure to set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print()
    
    test_s3_streaming()
