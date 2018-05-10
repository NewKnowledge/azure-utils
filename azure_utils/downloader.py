''' Download data for Azure Data Lake '''
import os
import json
from datetime import date, timedelta

from dotenv import load_dotenv

from azure.datalake.store import core, lib, multithread


def get_datalake_client(store_name=None, tenant_id=None, client_id=None, client_secret=None, envfile='datalake.env'):
    ''' returns an Azure data lake filesystem client '''
    # unless all vars are passed in, load env vars
    if not (store_name and tenant_id and client_id and client_secret):
        # load azure data lake environment variables
        dotenv_path = os.path.join(os.getcwd(), envfile)
        load_dotenv(dotenv_path)

    # set vars to env values by default
    store_name = store_name if store_name else os.getenv('STORE_NAME')
    tenant_id = tenant_id if tenant_id else os.getenv('TENANT_ID')
    client_id = client_id if client_id else os.getenv('CLIENT_ID')
    client_secret = client_secret if client_secret else os.getenv('CLIENT_SECRET')

    token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return core.AzureDLFileSystem(token, store_name=store_name)


def get_data(date=None, index=None, store_name='sociallake', envfile='/social_datalake.env'):
    return get_social_data(date=date, index=index, store_name=store_name, envfile=envfile)

def get_social_data(date=None, index=None, store_name='sociallake', envfile='/social_datalake.env', prefix='/streamsets/prod'):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''
    if date is None or index is None:
        raise Exception("Please provide keyword args 'date'=datetime.date object and 'index'")

    # validate date input
    date = date.isoformat()

    # validate index/folder name
    client = get_datalake_client(store_name=store_name, envfile=envfile)

    # list out files for that day
    files = client.ls("{0}/{1}/{2}".format(prefix, index, date))

    # read files in as JSON array
    data = []
    for filename in files:
        try:
            if '_tmp' in filename:
                continue
            with client.open(filename, 'rb') as fh:
                tmp_string = str(fh.read().decode(encoding='utf-8'))
                tmp = '[' + ','.join(f for f in tmp_string.splitlines()) + ']'
                data.extend(json.loads(tmp))
        except RuntimeError as err:
            print(err)
            break
    return data

def list_social_indices(store_name='sociallake', envfile='/social_datalake.env'):
    client = get_datalake_client(store_name=store_name, envfile=envfile)
    return client.ls("/streamsets/prod")

def get_datalake_file_handle(path, mode, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    client = get_datalake_client(store_name=store_name, envfile=envfile)
    return client.open(path, mode)

def get_datalake_file_handle_read(path, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    return get_datalake_file_handle(path, 'rb', store_name=store_name, envfile=envfile)

def get_datalake_file_handle_write(path, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    return get_datalake_file_handle(path, 'wb', store_name=store_name, envfile=envfile)

def get_datalake_file_handle_append(path, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    return get_datalake_file_handle(path, 'ab', store_name=store_name, envfile=envfile)

def list_datalake_files(path, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    client = get_datalake_client(store_name=store_name, envfile=envfile)
    return client.ls(path)

def download_datalake_file(remote_path='test.txt', local_path='./test.txt.', store_name='nkdsdevdatalake', 
                        envfile='/data_science_datalake.env', overwrite=True):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''

    # validate index/folder name
    client = get_datalake_client(store_name=store_name, envfile=envfile)

    multithread.ADLDownloader(client, lpath=local_path, rpath=remote_path,
                                nthreads=4, overwrite=overwrite, buffersize=2**24, blocksize=2**24)

def upload_datalake_file(remote_path='test.txt', local_path='./test.txt', store_name='nkdsdevdatalake', 
                        envfile='/data_science_datalake.env', overwrite=True):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''

    # validate index/folder name
    client = get_datalake_client(store_name=store_name, envfile=envfile)

    multithread.ADLUploader(client, lpath=local_path, rpath=remote_path,
                                nthreads=4, overwrite=overwrite, buffersize=2**24, blocksize=2**24)



if __name__ == '__main__':
    #get_social_data(date=date.today() - timedelta(days=1), index='disney')
    print(list_social_indices())
    #with get_datalake_file_handle('/test.txt', 'ab') as f:
    #    f.write("\nDOUBLE SUCCESS!!".encode('utf-8'))
    #write_datalake_file(remote_path='/this/is/a/test.txt', local_path='./test.txt')
    #get_datalake_file(remote_path='/this/is/a/test.txt', local_path='/test_out.txt')

