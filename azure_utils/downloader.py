''' Download data for Azure Data Lake '''
import json
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
from azure.datalake.store import core, lib, multithread
from dotenv import load_dotenv


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


def dataframe_generator(date=date.today(), index='disney', store_name='sociallake', envfile='datalake.env', prefix='/streamsets/prod', keep_cols=None, remove_duplicates=True):
    ''' generator function for data from Azure Data Lake files. converts json files into pandas dataframes '''

    if not isinstance(date, str):
        date = date.isoformat()

    client = get_datalake_client(store_name=store_name, envfile=envfile)

    print('finding files')
    # list out files for that day
    files = client.ls("{0}/{1}/{2}".format(prefix, index, date))
    for i, filename in enumerate(files):
        print('file', i+1, 'of', len(files))

        if '_tmp' in filename:
            print('skipping temp')
            continue

        try:
            with client.open(filename, 'rb') as adl_file:
                print('reading adl file')
                json_string = adl_file.read().decode(encoding='utf-8')
                print('parsin json string into pandas dataframe')
                file_df = pd.read_json(json_string, lines=True, orient='records')
                # print('splitting lines')
                file_df = file_df[keep_cols] if keep_cols else file_df
                # json_string = '[' + ','.join(json_string.splitlines()) + ']'
                # print('pandas reading json')
                #  = pd.read_json(json_string)

                if remove_duplicates:
                    # remove duplicates treating all non-dict, non-list col values as unique id
                    hashable_cols = [col for col in file_df.columns
                                     if not isinstance(file_df[col][0], list)
                                     and not isinstance(file_df[col][0], dict)
                                     ]
                    n_rows = len(file_df)
                    file_df.drop_duplicates(inplace=True, subset=hashable_cols)
                    print('dropped', n_rows - len(file_df))

                yield file_df

        except RuntimeError as err:
            print(err)
            break


def get_data(date=None, index=None, store_name='sociallake', envfile='/social_datalake.env'):
    return get_social_data(date=date, index=index, store_name=store_name, envfile=envfile)


def get_social_data(date=None, index=None, store_name='sociallake', envfile='/social_datalake.env', prefix='/streamsets/prod'):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''
    if date is None or index is None:
        raise Exception("Please provide keyword args 'date'=datetime.date object and 'index'")

    # validate date input
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d')

    date = date.isoformat()

    # validate index/folder name
    client = get_datalake_client(store_name=store_name, envfile=envfile)

    # list out files for that day
    files = client.ls("{0}/{1}/{2}".format(prefix, index, date))

    # read files in as JSON array
    data = []
    for i, filename in enumerate(files):
        print('file', i+1, 'of', len(files))
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


def test_dataframe_generator():

    data_gen = dataframe_generator(
        date=date.today(),
        index='disney',
        store_name='sociallake',
        envfile='datalake.env',
        prefix='/streamsets/prod',
        keep_cols=['content', 'createdAt'])

    full_df = pd.concat(next(data_gen) for _ in range(2))
    # full_df = pd.concat(df for df in data_gen)

    print('pre dupe:', full_df.shape)
    n_rows = len(full_df)
    str_cols = [col for col in full_df.columns
                if not isinstance(full_df[col][0], list)
                and not isinstance(full_df[col][0], dict)
                ]
    print('include cols', str_cols)
    full_df.drop_duplicates(inplace=True, subset=str_cols)

    print('dropped:', n_rows - len(full_df))
    # TODO dedupe with more column types


if __name__ == '__main__':
    test_dataframe_generator()
    # get_social_data(date=date.today() - timedelta(days=1), index='disney')
    # print(list_social_indices())

    # write_datalake_file(remote_path='/this/is/a/test.txt', local_path='./test.txt')
    # get_datalake_file(remote_path='/this/is/a/test.txt', local_path='/test_out.txt')
