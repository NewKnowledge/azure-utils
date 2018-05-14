''' Download data for Azure Data Lake '''
import json
import os
import time
from datetime import date, datetime

import pandas as pd
from azure.datalake.store import core, lib, multithread
from dotenv import load_dotenv

# default keys to keep from adl json records
KEEP_KEYS = ['content', 'collectedAt', 'publishedAt', 'createdAt', 'contentId', 'authorScreenName', 'authorUserId',
             'connectionType', 'parentScreenName', 'parentUserId', 'parentContentId']


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

def get_data(read_date=None, index=None, keep_keys=None, client=None, store_name='sociallake', envfile='/social_datalake.env'):
    return get_social_data(read_date=read_date, index=index, client=client, store_name=store_name, envfile=envfile)

def get_social_data(read_date=None, index=None, keep_keys=None, include_retweets=False, client=None, 
        store_name='sociallake',  envfile='/social_datalake.env', streamset='prod', verbose=False):
    ''' Generator function for reading all data for the given read_date from
    the given index and streamset in ADL. Yields a single document at a time as a
    dictionary. '''

    vprint = print if verbose else lambda x: None
    if isinstance(read_date, date):
        read_date = read_date.isoformat()
    elif not isinstance(read_date, str):
        raise Exception('date must be a string or datetime.date object')

    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile) 

    files = client.ls("/streamsets/{0}/{1}/{2}".format(streamset, index, read_date))
    for filename in files:
        vprint("Reading file {0}".format(filename))

        if '_tmp' in filename:
            vprint("Skipping temp file {0}".format(filename))
            continue

        with client.open(filename, 'rb') as adl_file:
            # read json string from adl file, parse into docs and yield rows
            json_string = adl_file.read().decode(encoding='utf-8')

            json_string = '[' + ','.join(line for line in json_string.splitlines()) + ']'
            documents = json.loads(json_string)

            # parse each row according to parameters
            for row in documents:
                if not include_retweets and row['connectionType'] == 'retweet':
                    continue
                if not keep_keys == None:
                    yield {k: v for k, v in row.items() if k in keep_keys}
                else:
                    yield row

def list_social_indices(client=None, streamset='prod', store_name='sociallake', envfile='/social_datalake.env'):
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile) 
    return client.ls("/streamsets/{0}".format(streamset))


def get_datalake_file_handle(path, mode, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    if not mode in ['rb', 'wb', 'ab']:
        raise Exception('Must use a binary file mode ("rb", "wb", or "ab")')
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile) 
    return client.open(path, mode)


def get_datalake_file_handle_read(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile) 
    return get_datalake_file_handle(path, 'rb', client=client, store_name=store_name, envfile=envfile)


def get_datalake_file_handle_write(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile) 
    return get_datalake_file_handle(path, 'wb', client=client, store_name=store_name, envfile=envfile)


def get_datalake_file_handle_append(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile) 
    return get_datalake_file_handle(path, 'ab', client=client, store_name=store_name, envfile=envfile)


def list_datalake_files(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile)
    return client.ls(path)


def download_datalake_file(remote_path, local_path, client=None, 
                            store_name='nkdsdevdatalake', envfile='/data_science_datalake.env', 
                            overwrite=True):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''

    # validate index/folder name
    if client == None:
        client = get_datalake_client(store_name=store_name, envfile=envfile)

    multithread.ADLDownloader(client, lpath=local_path, rpath=remote_path,
                              nthreads=4, overwrite=overwrite, buffersize=2**24, blocksize=2**24)


def upload_datalake_file(remote_path, local_path, client=None,
                        store_name='nkdsdevdatalake', envfile='/data_science_datalake.env', 
                        overwrite=True):
    ''' Upload data from a local file to Azure Data Lake '''

    # validate index/folder name

    client = get_datalake_client(store_name=store_name, envfile=envfile)

    multithread.ADLUploader(client, lpath=local_path, rpath=remote_path,
                            nthreads=4, overwrite=overwrite, buffersize=2**24, blocksize=2**24)


def test_data_clean(index='disney',
                    store_name='sociallake',
                    envfile='datalake.env',
                    prefix='/streamsets/prod'):

    read_date = date.today()
    client = get_datalake_client(store_name=store_name, envfile=envfile)
    vprint = print
    vprint('finding files')
    # list out files for that day
    files = client.ls("{0}/{1}/{2}".format(prefix, index, read_date))[:6]
    json_strings = []
    for i, filename in enumerate(files):
        vprint('file', i+1, 'of', len(files))

        if '_tmp' in filename:
            vprint('skipping temp file', filename)
            continue

        with client.open(filename, 'rb') as adl_file:
            # read json string from adl file, parse into docs and yield rows
            json_string = adl_file.read().decode(encoding='utf-8')
            json_strings.append(json_string)

    start_time = time.time()
    full_df = pd.DataFrame(row for row in yield_documents(json_string) for json_string in json_strings)
    print(full_df.shape)
    print('rows without df took time', time.time()-start_time, '\n\n')

    start_time = time.time()
    full_df = pd.DataFrame(row for row in yield_documents_df(json_string, return_batches=False)
                           for json_string in json_strings)
    print(full_df.shape)
    print('df rows took time', time.time()-start_time, '\n\n')

    start_time = time.time()
    full_df = pd.concat(row for row in yield_documents_df(json_string, return_batches=True)
                        for json_string in json_strings)
    print(full_df.shape)
    print('df batches took time', time.time()-start_time, '\n\n')

    # full_df = pd.DataFrame(df for df in data_batch_gen)
def test_file_handles():
    with get_datalake_file_handle("path/on/datalake/remote_file.txt","wb") as f:
        f.write("So incredibly".encode("utf-8"))

    with get_datalake_file_handle("path/on/datalake/remote_file.txt","ab") as f:
        f.write(" helpful".encode("utf-8"))

    with get_datalake_file_handle("path/on/datalake/remote_file.txt", "rb") as f:
        print(f.read().decode("utf-8"))

def test_upload_download():
    with get_datalake_file_handle("path/on/datalake/remote_file.txt", "wb") as f:
        f.write("So incredibly".encode("utf-8"))
    download_datalake_file(local_path="./local_file.txt", remote_path="/path/on/datalake/remote_file.txt")
    upload_datalake_file(local_path="./local_file.txt", remote_path="/path/on/datalake/remote_file.txt")
    print(list_datalake_files("path/on/datalake"))

def test_social_download():
    import logging
    print("Getting content")
    data = [d for d in get_social_data(read_date=date.today(), index='discovermex')]
    print("Got content")
    try:
        print(data[0])
    except:
        logging.exception("THE SYSTEM IS DOWN")

if __name__ == '__main__':
    #test_data_clean()
    # auto-generated client
    test_file_handles()
    test_upload_download()

