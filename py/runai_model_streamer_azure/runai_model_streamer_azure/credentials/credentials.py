from typing import Optional
import os

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
except ImportError:
    raise ImportError(
        "Azure Storage packages are not installed. "
        "Install them with: pip install azure-storage-blob azure-identity"
    )


class AzureCredentials:
    """
    Azure Blob Storage credentials configuration.
    
    Credentials can be provided in the following ways (in order of precedence):
    1. Connection string (connection_string parameter)
    2. Account name with SAS token (account_name + sas_token)
    3. Environment variables:
       - AZURE_STORAGE_CONNECTION_STRING
       - AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_SAS_TOKEN
    4. Default Azure credential chain (managed identity, Azure CLI, etc.)
    """
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        account_name: Optional[str] = None,
        sas_token: Optional[str] = None,
        endpoint: Optional[str] = None
    ):
        self.connection_string = connection_string
        self.account_name = account_name
        self.sas_token = sas_token
        self.endpoint = endpoint


def get_credentials(credentials: Optional[AzureCredentials] = None) -> AzureCredentials:
    """
    Resolves Azure credentials from various sources.
    
    Args:
        credentials: Optional AzureCredentials object with explicit credentials
        
    Returns:
        AzureCredentials object with resolved credentials
    """
    
    if credentials is None:
        credentials = AzureCredentials()
    
    # Check environment variables if not provided
    if not credentials.connection_string:
        credentials.connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    
    if not credentials.account_name:
        credentials.account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    
    if not credentials.sas_token:
        credentials.sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    
    if not credentials.endpoint:
        credentials.endpoint = os.environ.get("AZURE_STORAGE_ENDPOINT")
    
    return credentials


def create_blob_service_client(credentials: Optional[AzureCredentials] = None) -> BlobServiceClient:
    """
    Creates an Azure BlobServiceClient using the provided or resolved credentials.
    
    Args:
        credentials: Optional AzureCredentials object
        
    Returns:
        BlobServiceClient instance
        
    Raises:
        ValueError: If no valid credentials are found
    """
    
    creds = get_credentials(credentials)
    
    # Priority 1: Connection string
    if creds.connection_string:
        return BlobServiceClient.from_connection_string(creds.connection_string)
    
    # Priority 2: Account name + SAS token
    if creds.account_name and creds.sas_token:
        account_url = creds.endpoint or f"https://{creds.account_name}.blob.core.windows.net"
        sas = creds.sas_token if creds.sas_token.startswith("?") else f"?{creds.sas_token}"
        return BlobServiceClient(account_url=f"{account_url}{sas}")
    
    # Priority 4: Default credential chain
    if creds.account_name:
        account_url = creds.endpoint or f"https://{creds.account_name}.blob.core.windows.net"
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=account_url, credential=credential)
    
    raise ValueError(
        "No valid Azure credentials found. Please provide one of:\n"
        "- connection_string\n"
        "- account_name + sas_token\n"
        "- account_name (will use default Azure credential chain)\n"
        "or set corresponding environment variables."
    )
