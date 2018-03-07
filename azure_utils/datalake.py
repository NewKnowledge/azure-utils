import os
from os.path import dirname, isfile, join

from dotenv import load_dotenv

from azure.datalake.store import core, lib, multithread


def datalake_upload(datalake_client, source_filepaths, destination_dir='uploads', verbose=False):
    ''' uploads files to the data lake given the local filepaths '''

    n_files = len(source_filepaths)
    print('uploading', n_files, 'and writing to remote', destination_dir)

    for i, source in enumerate(source_filepaths):
        filename = source.split('/')[-1]
        if verbose:
            print('uploading', source, 'number', i, 'of', n_files)
        # datalake_client.put(source, target_dir)  # non-multithread alternative
        multithread.ADLUploader(datalake_client,
                                lpath=source,
                                rpath=join(destination_dir, filename),
                                nthreads=64,
                                overwrite=True,
                                buffersize=4194304,
                                blocksize=4194304)
    print('upload complete')


def datalake_download(datalake_client, source_filepaths, destination_dir='datalake-downloads', verbose=False):
    ''' downloads files from the data lake given the remote filepaths '''

    n_files = len(source_filepaths)
    print('downloading', n_files, 'and writing to local', destination_dir)

    for i, source in enumerate(source_filepaths):
        filename = source.split('/')[-1]
        if verbose:
            print('downloading', source, 'number', i, 'of', n_files)

        # datalake_client.get  # non-multithread alternative
        multithread.ADLDownloader(datalake_client,
                                  lpath=join(destination_dir, filename),
                                  rpath=source,
                                  nthreads=64,
                                  overwrite=True,
                                  buffersize=4194304,
                                  blocksize=4194304)
    print('download complete')


def get_datalake_client(store_name=None, tenant_id=None, client_id=None, client_secret=None, envfile='datalake.env'):
    ''' returns an Azure data lake filesystem client '''
    # unless all vars are passed in, load env vars
    if not (store_name and tenant_id and client_id and client_secret):
        # load azure data lake environment variables
        dotenv_path = join(os.getcwd(), envfile)
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
