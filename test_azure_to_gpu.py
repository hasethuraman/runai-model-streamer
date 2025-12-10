#!/usr/bin/env python3
"""
Test script to stream a model file from Azure Blob Storage directly to GPU memory.

Prerequisites:
    1. Build the Azure streamer library:
       cd /home/hsethuraman/ws/runai-model-streamer
       make -C cpp build
       
    2. Install the Azure streamer package:
       pip install py/runai_model_streamer_azure
       
    3. Set Azure credentials (one of the following):
       - AZURE_STORAGE_CONNECTION_STRING
       - AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY
       - AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_SAS_TOKEN
"""
import os
import sys
import torch

# Set LD_LIBRARY_PATH to find Azure backend library before importing
lib_path = os.path.join(os.path.expanduser("~"), ".local/lib/python3.10/site-packages/runai_model_streamer/libstreamer/lib")
if 'LD_LIBRARY_PATH' not in os.environ or lib_path not in os.environ['LD_LIBRARY_PATH']:
    # Re-exec ourselves with LD_LIBRARY_PATH set
    print(f"Setting LD_LIBRARY_PATH to {lib_path} and re-executing...")
    os.environ['LD_LIBRARY_PATH'] = f"{lib_path}:{os.environ.get('LD_LIBRARY_PATH', '')}"
    os.execv(sys.executable, [sys.executable] + sys.argv)

# Debug: confirm LD_LIBRARY_PATH is set
if __name__ == "__main__":
    print(f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH', 'NOT SET')}\n")

from runai_model_streamer import SafetensorsStreamer

# Configuration - Update these with your Azure details
AZURE_STORAGE_ACCOUNT = 'testrunai'
AZURE_CONTAINER = 'runai'
AZURE_BLOB_PATH = 'DD-vector-v2.safetensors'  # e.g., 'models/model.safetensors'

# Azure credentials (or set these as environment variables)
# os.environ['AZURE_STORAGE_ACCOUNT_NAME'] = 'your-storage-account'
# os.environ['AZURE_STORAGE_ACCOUNT_KEY'] = 'your-storage-key'
# Or use SAS token:
# os.environ['AZURE_STORAGE_SAS_TOKEN'] = 'your-sas-token'
# Or use connection string (remove BlobEndpoint if it includes container name):
os.environ['AZURE_STORAGE_CONNECTION_STRING'] = 'DefaultEndpointsProtocol=https;AccountName=testrunai;AccountKey=<TODO-ACCOUNT_KEY>;EndpointSuffix=core.windows.net'

def test_azure_to_gpu_streaming():
    """Test streaming from Azure Blob Storage to GPU memory."""
    
    # Check if CUDA is available
    if not torch.cuda.is_available():
        print("ERROR: CUDA is not available. GPU streaming requires a CUDA-enabled GPU.")
        sys.exit(1)
    
    print(f"CUDA available: {torch.cuda.is_available()}")
    print(f"GPU device: {torch.cuda.get_device_name(0)}")
    print(f"GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    print()
    
    # Construct Azure Blob Storage URI
    # Format: azure://<container>/<blob-path>
    # Note: Storage account comes from credentials, not the URI
    file_path = f"azure://{AZURE_CONTAINER}/{AZURE_BLOB_PATH}"
    
    print(f"Streaming from: {file_path}")
    print(f"Storage Account: {AZURE_STORAGE_ACCOUNT}")
    print()
    
    # Stream file from Azure Blob Storage to GPU
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
            print(f"✓ Success! Streamed {tensor_count} tensors ({total_size / 1e6:.2f} MB total) from Azure to GPU")
            
    except Exception as e:
        print(f"✗ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Verify Azure credentials are set
    has_credentials = (
        os.getenv('AZURE_STORAGE_CONNECTION_STRING') or
        (os.getenv('AZURE_STORAGE_ACCOUNT') and 
         (os.getenv('AZURE_STORAGE_KEY') or os.getenv('AZURE_STORAGE_SAS_TOKEN')))
    )
    
    if not has_credentials:
        print("WARNING: Azure credentials not found in environment variables.")
        print("Set one of the following:")
        print("  1. AZURE_STORAGE_CONNECTION_STRING")
        print("  2. AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY")
        print("  3. AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_SAS_TOKEN")
        print("Or uncomment the credential lines in the script.")
        print()
    
    test_azure_to_gpu_streaming()
