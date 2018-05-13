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


def yield_documents_df(json_string, include_retweets=False, return_batches=False, keep_cols=KEEP_KEYS):
    # vprint = print
    file_df = pd.read_json(json_string, lines=True, orient='records')
    # if keep_cols specified, drop all other cols
    file_df = file_df[keep_cols] if keep_cols else file_df

    if not include_retweets:
        n_rows = len(file_df)
        # vprint('dropping retweets')
        file_df = file_df[file_df['connectionType'] != 'retweet']
        # vprint('dropped', n_rows - len(file_df), 'retweets')

    # vprint('yielding', len(file_df), 'samples')
    if return_batches:
        yield file_df
    else:
        for row in file_df.iterrows():
            yield row[1]  # iterrows returns (row_num, Series) pair


def yield_documents(json_string, include_retweets=False, keep_cols=KEEP_KEYS):
    json_string = '[' + ','.join(line for line in json_string.splitlines()) + ']'
    documents = json.loads(json_string)
    for row in documents:
        if not include_retweets and row['connectionType'] == 'retweet':
            continue
        if keep_cols:
            yield {k: v for k, v in row.items() if k in keep_cols}
        else:
            yield row


def dataframe_generator(read_date=date.today(),
                        index='disney',
                        store_name='sociallake',
                        envfile='datalake.env',
                        prefix='/streamsets/prod',
                        keep_cols=None,
                        include_retweets=False,
                        verbose=False,
                        return_batches=False,
                        use_df=False,
                        ):
    ''' Generator function for reading all data for given date from Azure Data Lake given store and index.
        Converts json files into rows or batches of pandas dataframes. '''

    vprint = print if verbose else lambda x: None
    if isinstance(read_date, date):
        read_date = read_date.isoformat()
    elif not isinstance(read_date, str):
        raise Exception('date must be a string or datetime.date object')

    client = get_datalake_client(store_name=store_name, envfile=envfile)

    vprint('finding files')
    # list out files for that day
    files = client.ls("{0}/{1}/{2}".format(prefix, index, read_date))[:3]
    for i, filename in enumerate(files):
        vprint('file', i+1, 'of', len(files))

        if '_tmp' in filename:
            vprint('skipping temp file', filename)
            continue

        try:
            with client.open(filename, 'rb') as adl_file:
                # read json string from adl file, parse into docs and yield rows
                json_string = adl_file.read().decode(encoding='utf-8')

                if use_df:
                    file_df = pd.read_json(json_string, lines=True, orient='records')
                    # if keep_cols specified, drop all other cols
                    file_df = file_df[keep_cols] if keep_cols else file_df

                    if not include_retweets:
                        n_rows = len(file_df)
                        vprint('dropping retweets')
                        file_df = file_df[file_df['connectionType'] != 'retweet']
                        vprint('dropped', n_rows - len(file_df), 'retweets')

                    vprint('yielding', len(file_df), 'samples')
                    if return_batches:
                        yield file_df
                    else:
                        for row in file_df.iterrows():
                            yield row[1]  # iterrows returns (row_num, Series) pair
                else:
                    json_string = '[' + ','.join(line for line in json_string.splitlines()) + ']'
                    documents = json.loads(json_string)
                    for row in documents:
                        if not include_retweets and row['connectionType'] == 'retweet':
                            continue
                        if keep_cols:
                            yield {k: v for k, v in row.items() if k in keep_cols}
                        else:
                            yield row

                # TODO remove nan, do other common prep (in batches before yielding)?

        except RuntimeError as err:
            vprint(err)
            break


def get_data(date=None, index=None, store_name='sociallake', envfile='/social_datalake.env'):
    return get_social_data(date=date, index=index, store_name=store_name, envfile=envfile)


def get_social_data(date=None, index=None, store_name='sociallake', envfile='/social_datalake.env', prefix='/streamsets/prod', verbose=True):
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


def test_dataframe_generator(read_date):
    data_row_gen = dataframe_generator(
        read_date=date.today(),
        index='disney',
        store_name='sociallake',
        envfile='datalake.env',
        prefix='/streamsets/prod',
        keep_cols=KEEP_KEYS,
        include_retweets=False,
        verbose=True,
        use_df=True,
    )

    print('create dataframe from use_df row generator')
    start_time = time.time()
    full_df = pd.DataFrame(row for row in data_row_gen)
    print('rows took time', time.time()-start_time)

    data_batch_gen = dataframe_generator(
        read_date=date.today(),
        index='disney',
        store_name='sociallake',
        envfile='datalake.env',
        prefix='/streamsets/prod',
        keep_cols=KEEP_KEYS,
        include_retweets=False,
        verbose=True,
        return_batches=True,
        use_df=True,
    )

    # print(full_df.shape)
    print('concat batches from df generator')
    start_time = time.time()
    full_df = pd.concat(df for df in data_batch_gen)
    print('batches took time', time.time()-start_time)

    data_batch_gen = dataframe_generator(
        read_date=date.today(),
        index='disney',
        store_name='sociallake',
        envfile='datalake.env',
        prefix='/streamsets/prod',
        keep_cols=KEEP_KEYS,
        include_retweets=False,
        verbose=True,
        # return_batches=True,
        # use_df=True,
    )

    # print(full_df.shape)
    print('rows not using dataframe from generator')
    start_time = time.time()
    # full_df = pd.DataFrame(df for df in data_batch_gen)
    full_df = pd.DataFrame(row for row in data_row_gen)
    print('rows without df took time', time.time()-start_time)

    print(full_df.shape)

    print('removing duplicates')
    n_rows = len(full_df)
    str_cols = [col for col in full_df.columns
                if not isinstance(full_df[col][0], list)
                and not isinstance(full_df[col][0], dict)
                ]
    full_df.drop_duplicates(inplace=True, subset=str_cols)

    print('dropped:', n_rows - len(full_df), 'duplicates')
    full_df.to_csv(read_date.isoformat() + '_test.csv')
    # TODO dedupe with more column types (list, dict?)


if __name__ == '__main__':
    # test_dataframe_generator(date.today())
    test_data_clean()
    # get_social_data(date=date.today() - timedelta(days=1), index='disney')
    # print(list_social_indices())

    # write_datalake_file(remote_path='/this/is/a/test.txt', local_path='./test.txt')
    # get_datalake_file(remote_path='/this/is/a/test.txt', local_path='/test_out.txt')
