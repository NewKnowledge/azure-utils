''' Download data for Azure Data Lake '''
import json
import os
from datetime import date

from azure.datalake.store import core, lib, multithread
from dotenv import load_dotenv

# default keys to keep from adl json records
KEEP_KEYS = ['content', 'collectedAt', 'publishedAt', 'createdAt', 'contentId', 'authorScreenName', 'authorUserId',
             'connectionType', 'parentScreenName', 'parentUserId', 'parentContentId']


def get_datalake_client(store_name=None, tenant_id=None, client_id=None, client_secret=None, envfile='/data_science_datalake.env'):
    ''' returns an Azure data lake filesystem client '''
    # unless all vars are passed in, load env vars
    if not (store_name and tenant_id and client_id and client_secret):
        # load azure data lake environment variables
        load_dotenv(envfile)

    # set vars to env values by default
    store_name = store_name if store_name else os.getenv('STORE_NAME')
    tenant_id = tenant_id if tenant_id else os.getenv('TENANT_ID')
    client_id = client_id if client_id else os.getenv('CLIENT_ID')
    client_secret = client_secret if client_secret else os.getenv('CLIENT_SECRET')

    token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return core.AzureDLFileSystem(token, store_name=store_name)


def get_data(read_date=None, index=None, client=None, store_name='sociallake', envfile='/data_science_datalake.env'):
    ''' Get all data from the given store, index, and date '''
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

    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)

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
                if keep_keys:
                    yield {k: v for k, v in row.items() if k in keep_keys}
                else:
                    yield row


def list_social_indices(client=None, streamset='prod', store_name='sociallake', envfile='/social_datalake.env'):
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)
    return client.ls("/streamsets/{0}".format(streamset))


def get_datalake_file_handle(path, mode, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    if not mode in ['rb', 'wb', 'ab']:
        raise Exception('Must use a binary file mode ("rb", "wb", or "ab")')
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)
    return client.open(path, mode)


def get_datalake_file_handle_read(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)
    return get_datalake_file_handle(path, 'rb', client=client, store_name=store_name, envfile=envfile)


def get_datalake_file_handle_write(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)
    return get_datalake_file_handle(path, 'wb', client=client, store_name=store_name, envfile=envfile)


def get_datalake_file_handle_append(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)
    return get_datalake_file_handle(path, 'ab', client=client, store_name=store_name, envfile=envfile)


def list_datalake_files(path, client=None, store_name='nkdsdevdatalake', envfile='/data_science_datalake.env'):
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)
    return client.ls(path)


def download_datalake_file(remote_path, local_path, client=None,
                           store_name='nkdsdevdatalake', envfile='/data_science_datalake.env',
                           overwrite=True):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''

    # validate index/folder name
    client = client if client else get_datalake_client(store_name=store_name, envfile=envfile)

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


def test_file_handles():
    with get_datalake_file_handle("path/on/datalake/remote_file.txt", "wb") as f:
        f.write("So incredibly".encode("utf-8"))

    with get_datalake_file_handle("path/on/datalake/remote_file.txt", "ab") as f:
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
    test_file_handles()
    test_upload_download()
