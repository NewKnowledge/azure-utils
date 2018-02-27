import os
from os.path import dirname, isfile, join

from dotenv import load_dotenv

from azure.datalake.store import core, lib, multithread


def upload_files_to_datalake(datalake_client, source_filepaths, destination_dir='uploads'):
    ''' uploads files to the data lake given the local filepaths '''
    for source in source_filepaths:
        # datalake_client.put(source, target_dir)  # non-multithread alternative
        filename = source.split('/')[-1]
        multithread.ADLUploader(datalake_client,
                                lpath=source,
                                rpath=join(destination_dir, filename),
                                nthreads=64,
                                overwrite=True,
                                buffersize=4194304,
                                blocksize=4194304)


def download_files_from_datalake(datalake_client, source_filepaths, destination_dir='datalake-downloads'):
    ''' downloads files from the data lake given the remote filepaths '''
    for source in source_filepaths:
        filename = source.split('/')[-1]
        multithread.ADLDownloader(datalake_client,
                                  lpath=join(destination_dir, filename),
                                  rpath=source,
                                  nthreads=64,
                                  overwrite=True,
                                  buffersize=4194304,
                                  blocksize=4194304)


def get_datalake_client(store_name=None, tenant_id=None, client_id=None, client_secret=None, envfile='datalake.env'):
    ''' returns an Azure data lake filesystem client '''
    # unless all vars are passed in, load env vars
    if not (store_name and tenant_id and client_id and client_secret):
        # load azure data lake environment variables
        dotenv_path = join(dirname(__file__), envfile)
        load_dotenv(dotenv_path)

    # set vars to env values by default
    store_name = store_name if store_name else os.getenv('STORE_NAME')
    tenant_id = tenant_id if tenant_id else os.getenv('TENANT_ID')
    client_id = client_id if client_id else os.getenv('CLIENT_ID')
    client_secret = client_secret if client_secret else os.getenv('CLIENT_SECRET')

    token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return core.AzureDLFileSystem(token, store_name=store_name)

# Create a remote directory:
# datalake_client.mkdir('/mysampledirectory')

# list data in remote directory
# datalake_client.ls('/data')
