"""This is the asynchronous Azure blob helper."""

__version__ = "0.1.0"

import logging

from azure.core.exceptions import (
    AzureError,
    ResourceExistsError,
    ResourceNotFoundError
)

logger = logging.getLogger(__name__)


class AzBlobManagerAsync:
    """A utility class to help working with Azure Blob Storage.
        This class implements asynchronous methods based on the
        Microsoft Python SDK azure.storage.blob.aio
    See:
        https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.aio?view=azure-python

    Available:
        - Basic methods to work with containers and blobs

    """

    @classmethod
    def create(cls, connection_string=None, account_url=None, credential=None):
        """Instantiate an asynchronous AzBlobManagerAsync object.

        Args:
            connection_string (str): A connection string to an Azure Storage account.
            account_url (str): The URL to the blob storage account. Any other entities included in
                the URL path (e.g. container or blob) will be discarded. This URL can be
                optionally authenticated with a SAS token.
            credential (str):  The credentials with which to authenticate. This is optional
                if the account URL already has a SAS token, or the connection string already
                has shared access key values. The value can be a SAS token string, an account
                shared access key, or an instance of a TokenCredentials class from azure.identity.
                Credentials provided here will take precedence over those in the connection string.

        Returns:
            AzBlobManagerAsync object

        Examples:
            Creating the AzBlobManagerAsync with account url and a shared access key:
            azStorageManager = AzBlobManagerAsync.create(account_url=self.url, credential=self.shared_access_key)

            Creating the AzBlobManagerAsync with a connection string that has the shared access key:
            azStorageManager = AzBlobManagerAsync.create(connection_string='DefaultEndpointsProtocol=http;...')

        """
        self = AzBlobManagerAsync()
        self.connection_string = connection_string
        self.account_url = account_url
        self.credential = credential

        from azure.storage.blob.aio import BlobServiceClient
        self.blob_service_client = BlobServiceClient
        if (self.connection_string is not None):
            # Create BlobServiceClient from a Connection String
            self.blob_service_client = BlobServiceClient.from_connection_string(
                conn_str=self.connection_string, credential=self.credential)
        else:
            # Creating the BlobServiceClient with account url and credential.
            self.blob_service_client = BlobServiceClient(
                account_url=self.account_url, credential=self.credential)

        return self

    def _logAzureError(self, err=AzureError):
        msg = err.message.split('\n')[0]
        logger.error(f'AzureError error: {msg}')

    async def create_container(self, container_name):
        """Creates a new container.

        Args:
            container_name (str): The name of the container.
            See https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata
                for naming convention

        Returns:
         bool: The return value. True for success, False otherwise.
        """
        success = False
        try:
            new_container = await self.blob_service_client.create_container(container_name)
            properties = await new_container.get_container_properties()
            success = properties is not None and properties.name == container_name
        except ResourceExistsError:
            logger.info(f'Container \"{container_name}\" already exists.')
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def delete_container(self, container_name):
        """Deletes a container.

        Args:
            container_name (str): The name of the container.

        Returns:
         bool: The return value. True for success, False otherwise.
        """
        success = False
        try:
            await self.blob_service_client.delete_container(container_name)
            success = True
        except ResourceNotFoundError:
            logger.info(f'Container \"{container_name}\" doesn not exist.')
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def _list_containers(self, name_starts_with=None, include_metadata=False):
        """Lists containers.

        Args:
            name_starts_with (str): Filters the results to return only containers whose names
                begin with the specified prefix.
            include_metadata (bool): Specifies that container metadata to be returned in the response.

        Returns:
            ItemPaged[ContainerProperties]: An iterable (auto-paging) of ContainerProperties.
        """

        try:
            containers = []
            async for container in self.blob_service_client.list_containers(
                    name_starts_with=name_starts_with, include_metadata=include_metadata):
                containers.append(container)
            return containers
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return None

    async def list_containers_name(self, name_starts_with=None):
        """Lists containers' name.

        Args:
           name_starts_with (str): Filters the results to return only containers whose names
               begin with the specified prefix.

        Returns:
           list: A list of strings representing the container names.
        """

        containers_list = []
        containers = await self._list_containers(
            name_starts_with=name_starts_with, include_metadata=False)
        if (containers is None):
            return containers_list
        for container in containers:
            containers_list.append(container['name'])
        return containers_list

    async def create_append_blob(self, container_name, blob_name, replace_blob=False):
        """Creates an append blob in an existing container.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            replace_blob (bool): If True, deletes existing blob with same name

        Returns:
         bool: The return value. True for success, False otherwise.
        """
        success = False
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container_name, blob_name)
            # raise ResourceNotFoundError if blob does not exist
            await blob_client.get_blob_properties()
            # blob exists already
            if (replace_blob is True):
                await blob_client.create_append_blob()
            success = True
        except ResourceNotFoundError:
            await blob_client.create_append_blob()
            success = True
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def create_page_blob(self,
                               container_name, blob_name, size=1024, content_settings=None,
                               metadata=None, premium_page_blob_tier=None):
        """Creates a page blob in an existing container.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            size (int): This specifies the maximum size for the page blob, up to 1 TB.
                The page blob size must be aligned to a 512-byte boundary
            content_settings (ContentSettings): ContentSettings object used to set blob properties.
                Used to set content type, encoding, language, disposition, md5, and cache control.
            metadata (dict(str, str)): Name-value pairs associated with the blob as metadata
            premium_page_blob_tier (PremiumPageBlobTier): A page blob tier value to set the blob to
        Returns:
         bool: The return value. True for success, False otherwise.
        """
        success = False
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container_name, blob_name)
            await blob_client.create_page_blob(size, content_settings, metadata, premium_page_blob_tier)
            success = True
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def delete_blob(self, container_name, blob_name):
        """Deletes a blob.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.

        Returns:
         bool: The return value. True for success, False otherwise.
        """
        success = False
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container_name, blob_name)
            await blob_client.delete_blob()
            success = True
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def list_blobs(self, container_name):
        """Lists the blobs in the specified container.

        Args:
            container_name (str): The name of the container.

        Returns:
            list: A list of strings representing the blob names.
        """

        blobs_list = []
        try:
            container_client = self.blob_service_client.get_container_client(
                container_name)
            async for blob in container_client.list_blobs():
                blobs_list.append(blob)
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception(f'Fatal error')
        return blobs_list

    async def upload_data(self, data, container_name, blob_name, blob_type='BlockBlob'):
        """Creates a new blob from a data source with automatic chunking.

        Args:
            data: The blob data to upload.
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            blob_typr (str): The type of the blob. This can be either BlockBlob, PageBlob or AppendBlob.

        Returns:
            bool: The return value. True for success, False otherwise.
        """

        success = False
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container_name, blob_name)
            await blob_client.upload_blob(data)
            success = True
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def append_block(self, data, container_name, blob_name):
        """Commits a new block of data to the end of the existing append blob.

        Args:
            data: Content of the block.
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.

        Returns:
            bool: The return value. True for success, False otherwise.
        """

        success = False
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container_name, blob_name)
            await blob_client.append_block(data)
            success = True
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
        return success

    async def download_data(self, container_name, blob_name):
        """Downloads a blob.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
        Returns:
            stream: The data stream
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container_name, blob_name)
            stream = await blob_client.download_blob()
            return await stream.readall()
        except AzureError as err:
            self._logAzureError(err=err)
        except Exception:
            logger.exception('')
