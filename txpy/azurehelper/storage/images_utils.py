import logging
import cv2


logger = logging.getLogger(__name__)


class AzImageManager:

    async def upload_image(self, azStorageManager, container_name, blob_name, image):
        """Uploads an image in a blob.

        Args:
            azStorageManager (AzStorageManager): The Azure Storage Manager instance
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            image: The image
        Returns:
            bool: The return value. True for success, False otherwise.
        """
        try:
            success = False
            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile() as temp:
                iName = "".join([str(temp.name), ".jpg"])
                cv2.imwrite(iName, image)
            with open(iName, 'rb') as data:
                success = await azStorageManager.upload_data(data, container_name, blob_name)
        except Exception:
            logger.exception('')
        return success
