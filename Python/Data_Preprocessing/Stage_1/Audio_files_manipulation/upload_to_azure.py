import os
from azure.storage.blob import BlockBlobService
import data.secrets.credentials as credentials

def upload_to_azure(rootdir):
    # Obtain all the subfolders
    def fast_scandir(dirname):
        subfolders = [f.path for f in os.scandir(dirname) if f.is_dir()]
        for dirname in list(subfolders):
            subfolders.extend(fast_scandir(dirname))
        return subfolders

    folders = fast_scandir(rootdir)
    subfolders = folders[2:]

    # Upload to blob
    path_remove = rootdir.replace(
        os.path.basename(os.path.normpath(rootdir)), '')
    container_name = 'earningcall'
    block_blob_service = BlockBlobService(
        account_name=credentials.login['account_name'],
        account_key=credentials.login['account_key'])

    for subfolder_i in subfolders:
        print("\nUploading from: " + subfolder_i)
        for file_i in os.listdir(subfolder_i):
            file_path_on_azure = os.path.join(subfolder_i, file_i).replace(path_remove,'')
            file_path_on_local = os.path.join(subfolder_i, file_i)

            # upload file to blob
            print("\nUploading to Blob storage as blob: " + file_i)
            block_blob_service.create_blob_from_path(
                container_name=container_name,
                blob_name=file_path_on_azure,
                file_path=file_path_on_local)




if __name__ =='__main__':
    rootdir = 'C:/Users/marriane-f2sh391ja/Downloads/SP1500_EarningsCall_set_tiny'  # change with your local folder
    upload_to_azure(rootdir=rootdir)