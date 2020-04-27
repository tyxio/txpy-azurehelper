import asyncio
import uuid
import warnings
import unittest

from .context import AzBlobManagerAsync

from . import read_azure_config, read_logging_config

# help(AzBlobManagerAsync)

mylogging = read_logging_config()
logger = mylogging.getLogger("foo")

AZ_STORAGE_BLOB_CONN_STR, AZ_STORAGE_BLOB_SAS_URL, AZ_STORAGE_BLOB_SAS_TOKEN =\
    read_azure_config()


def get_blobs_name(blobs_list):
    blobs_name = []
    if (blobs_list is not None and len(blobs_list) > 0):
        for blob in blobs_list:
            blobs_name.append(blob.name)
    return blobs_name


class TestAzBlobManagerAsync(unittest.TestCase):

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

    def setUp(self):
        self.blobMgr = AzBlobManagerAsync.create(
            connection_string=AZ_STORAGE_BLOB_CONN_STR)
        # account_url=AZ_STORAGE_BLOB_SAS_URL)

    def run_async_test(self, func):
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)

        coro = asyncio.coroutine(func)
        event_loop.run_until_complete(coro())
        event_loop.close()

    def test_00_create_container(self):
 
        async def run_test_00_create_container():
            success = await self.blobMgr.create_container(self.container_name)
            self.assertEqual(
                success, True, f'Could not create container {self.container_name}')

        # Run the async test
        self.run_async_test(run_test_00_create_container)

    def test_01_create_append_blobs(self):

        async def run_test_01_create_append_blobs():
            blob_name = self.blobs_name[0]
            success = await self.blobMgr.create_append_blob(self.container_name, blob_name)
            self.assertEqual(
                success, True, f'Could not create append blob {self.container_name}/{blob_name}')

            blob_name = self.blobs_name[1]
            success = await self.blobMgr.create_append_blob(self.container_name, blob_name)
            self.assertEqual(
                success, True, f'Could not create append blob {self.container_name}/{blob_name}')

        # Run the async test
        self.run_async_test(run_test_01_create_append_blobs)

    def test_02_create_page_blobs(self):

        async def run_test_02_create_page_blobs():
            blob_name = self.blobs_name[2]
            success = await self.blobMgr.create_page_blob(self.container_name, blob_name)
            self.assertEqual(
                success, True, f'Could not create page blob {self.container_name}/{blob_name}')

            blob_name = self.blobs_name[3]
            success = await self.blobMgr.create_page_blob(self.container_name, blob_name)
            self.assertEqual(
                success, True, f'Could not create page blob {self.container_name}/{blob_name}')

        # Run the async test
        self.run_async_test(run_test_02_create_page_blobs)

    def test_03_list_blobs(self):

        async def run_test_03_list_blobs():
            blobs_list = await self.blobMgr.list_blobs(self.container_name)
            blobs_name = get_blobs_name(blobs_list)
            self.assertListEqual(blobs_name, self.blobs_name)

        # Run the async test
        self.run_async_test(run_test_03_list_blobs)

    def test_04_list_blobs_ex(self):

        async def run_test_04_list_blobs_ex():
            # trigger an exception because the name is invalid
            blobs_list = await self.blobMgr.list_blobs('input_objects')
            blobs_name = get_blobs_name(blobs_list)
            self.assertListEqual(blobs_name, [])

        # Run the async test
        self.run_async_test(run_test_04_list_blobs_ex)

    def test_05_append_block(self):

        async def run_test_05_append_block():
            blob_name = self.blobs_name[0]
            for _ in range(10):
                data = "data" + str(uuid.uuid4()) + "\n"
                success = await self.blobMgr.append_block(data, self.container_name, blob_name)
                self.assertEqual(
                    success, True, f'Could not append data to {self.container_name}/{blob_name}')

        # Run the async test
        self.run_async_test(run_test_05_append_block)

    def test_06_delete_blobs(self):

        async def run_test_06_delete_blobs():
            blob_name = self.blobs_name[2]
            success = await self.blobMgr.delete_blob(self.container_name, blob_name)
            self.assertEqual(
                success, True, f'Could not delete page blob {self.container_name}/{blob_name}')

            blob_name = 'page_blob'
            success = await self.blobMgr.delete_blob(self.container_name, blob_name)
            self.assertEqual(
                success, False, f'Weird... I could delete a blob that was not created: {self.container_name}/{blob_name}')

            blobs_list = await self.blobMgr.list_blobs(self.container_name)
            blobs_name = get_blobs_name(blobs_list)
            self.assertListEqual(blobs_name, self.blobs_name1)

        # Run the async test
        self.run_async_test(run_test_06_delete_blobs)

    def test_07_delete_containers(self):

        async def run_test_07_delete_containers():
            # clean-up
            containers_list = await self.blobMgr.list_containers_name(name_starts_with='quickstart')
            for container in containers_list:
                success = await self.blobMgr.delete_container(container)
                self.assertEqual(
                    success, True, f'Could not delete container {container}')

        # Run the async test
        self.run_async_test(run_test_07_delete_containers)


async def main():
    await unittest.main()

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except SystemExit:
        pass
