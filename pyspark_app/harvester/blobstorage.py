import logging

from azure.storage.blob import ContainerClient

from .base import ResourceHarvester
from .exceptions import ResourceNotFound

logger = logging.getLogger(__name__)

class AzureBlobStorageHarvester(ResourceHarvester):
    def __init__(self,chunk_size = 1024 * 1024,**kwargs):
        self._container_client = ContainerClient(**kwargs)
        self._path = None
        self._client = None
        self._chunk_size = chunk_size

    def _get_blob_client(self,path):
        if not self._client or self._path != path:
            self._client = self._container_client.get_blob_client(path)
            self._path = path
        return self._client

    def saveas(self,path,filename):
        """
        Download the blob resource to a file
        """
        logger.debug("Try to download the file({}) from blob storage to local file({})".format(path,filename))
        if not self._get_blob_client(path).exists():
            raise ResourceNotFound(path)

        offset = 0
        blob_size = self._get_blob_client(path).get_blob_properties().size
        length = self._chunk_size

        with open(filename,'wb') as f:
            while True:
                size  = self._get_blob_client(path).download_blob(offset=offset,length=length).readinto(f)
                logger.debug("Offset = {}, length={},readed data={}, blob size={}".format(offset,length,size,blob_size))
                if size < self._chunk_size:
                    break
                else:
                    offset += self._chunk_size
                    length =  blob_size - offset
                    if length >= self._chunk_size:
                        length = self._chunk_size
                    elif length == 0:
                        break



