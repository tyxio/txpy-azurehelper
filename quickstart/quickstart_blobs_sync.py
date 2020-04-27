# pip install txpy-azureutils
import uuid

from txpy.azureutils.storage import AzBlobManagerSync

AZ_STORAGE_BLOB_CONN_STR = "your_storage_connection_string"

blobMgr = AzBlobManagerSync(connection_string=AZ_STORAGE_BLOB_CONN_STR)

container_name = "quickstart" + str(uuid.uuid4())
success = blobMgr.create_container(container_name)
print(f'create_container {container_name}: {success}')

append_blob_name = 'anewappendblob'
success = blobMgr.create_append_blob(container_name, append_blob_name)
print(f'create_append_blob: {success}')

page_blob_name = 'anewpageblob'
success = blobMgr.create_page_blob(container_name, page_blob_name)
print(f'create_page_blob: {success}')

blobs_list = blobMgr.list_blobs(container_name)
blobs_name = []
if (blobs_list is not None and len(blobs_list) > 0):
    for blob in blobs_list:
        blobs_name.append(blob.name)
    print(blobs_name)

for _ in range(10):
    data = "data" + str(uuid.uuid4())
    success = blobMgr.append_block(data, container_name, append_blob_name)
    print(f'append_block: {success}')

success = blobMgr.delete_blob(container_name, append_blob_name)
print(f'delete_blob {append_blob_name}: {success}')
success = blobMgr.delete_blob(container_name, page_blob_name)
print(f'delete_blob {page_blob_name}: {success}')

containers_list = blobMgr.list_containers_name(name_starts_with='quickstart')
for container in containers_list:
    success = blobMgr.delete_container(container)
    print(f'delete_container {container}: {success}')
