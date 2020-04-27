import uuid
import unittest
import warnings

from .context import AzBlobManagerSync

from . import read_azure_config, read_logging_config

# help(AzBlobManagerSync)

mylogging = read_logging_config()
logger = mylogging.getLogger(__name__)

AZ_STORAGE_BLOB_CONN_STR, AZ_STORAGE_BLOB_SAS_URL, AZ_STORAGE_BLOB_SAS_TOKEN =\
    read_azure_config()


def get_blobs_name(blobs_list):
    blobs_name = []
    if (blobs_list is not None and len(blobs_list) > 0):
        for blob in blobs_list:
            blobs_name.append(blob.name)
    return blobs_name


class TestAzBlobManagerSync(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # ignore unclosed socket resource warnings from the Azure SDK
        warnings.filterwarnings(action="ignore", message="unclosed",
                                category=ResourceWarning)

        cls.container_name = "quickstart" + str(uuid.uuid4())
        cls.blobs_name = ["appendblob1",
                          "appendblob2", "pageblob1", "pageblob2"]
        cls.blobs_name1 = ["appendblob1",
                           "appendblob2", "pageblob2"]
        cls.blobMgr = AzBlobManagerSync(
            connection_string=AZ_STORAGE_BLOB_CONN_STR)
        # account_url=AZ_STORAGE_BLOB_SAS_URL)

    def setUp(self):
        pass

    def test_00_create_container(self):
        success = self.blobMgr.create_container(self.container_name)
        self.assertEqual(
            success, True, f'Could not create container {self.container_name}')

    def test_01_create_append_blobs(self):
        blob_name = self.blobs_name[0]
        success = self.blobMgr.create_append_blob(
            self.container_name, blob_name)
        self.assertEqual(
            success, True, f'Could not create append blob {self.container_name}/{blob_name}')

        blob_name = self.blobs_name[1]
        success = self.blobMgr.create_append_blob(
            self.container_name, blob_name)
        self.assertEqual(
            success, True, f'Could not create append blob {self.container_name}/{blob_name}')

    def test_02_create_page_blobs(self):
        blob_name = self.blobs_name[2]
        success = self.blobMgr.create_page_blob(self.container_name, blob_name)
        self.assertEqual(
            success, True, f'Could not create page blob {self.container_name}/{blob_name}')

        blob_name = self.blobs_name[3]
        success = self.blobMgr.create_page_blob(self.container_name, blob_name)
        self.assertEqual(
            success, True, f'Could not create page blob {self.container_name}/{blob_name}')

    def test_03_list_blobs(self):
        blobs_list = self.blobMgr.list_blobs(self.container_name)
        blobs_name = get_blobs_name(blobs_list)
        self.assertListEqual(blobs_name, self.blobs_name)

    def test_04_list_blobs_ex(self):
        # trigger an exception because the name is invalid
        blobs_list = self.blobMgr.list_blobs('input_objects')
        blobs_name = get_blobs_name(blobs_list)
        self.assertListEqual(blobs_name, [])

    def test_05_append_block(self):
        blob_name = self.blobs_name[0]
        for _ in range(10):
            data = "data" + str(uuid.uuid4())
            success = self.blobMgr.append_block(
                data, self.container_name, blob_name)
            self.assertEqual(
                success, True, f'Could not append data to {self.container_name}/{blob_name}')

    def test_06_delete_blobs(self):
        blob_name = self.blobs_name[2]
        success = self.blobMgr.delete_blob(self.container_name, blob_name)
        self.assertEqual(
            success, True, f'Could not delete page blob {self.container_name}/{blob_name}')

        blob_name = 'page_blob'
        success = self.blobMgr.delete_blob(self.container_name, blob_name)
        self.assertEqual(
            success, False, f'Weird... I could delete a blob that was not created: {self.container_name}/{blob_name}')

        blobs_list = self.blobMgr.list_blobs(self.container_name)
        blobs_name = get_blobs_name(blobs_list)
        self.assertListEqual(blobs_name, self.blobs_name1)

    def test_07_delete_containers(self):
        # clean-up
        containers_list = self.blobMgr.list_containers_name(
            name_starts_with='quickstart')
        for container in containers_list:
            success = self.blobMgr.delete_container(container)
            self.assertEqual(
                success, True, f'Could not delete container {container}')
