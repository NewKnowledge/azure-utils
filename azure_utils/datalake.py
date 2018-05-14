import json
import os
from datetime import date

from azure.datalake.store import core, lib, multithread
from dotenv import load_dotenv

''' File manipulation for Azure Data Lake '''


def get_datalake_client(store_name=None, tenant_id=None, client_id=None, client_secret=None, envfile='/data_science_datalake.env'):
    ''' returns an Azure data lake filesystem client '''
    # unless all vars are passed in, load env vars
    if not (store_name and tenant_id and client_id and client_secret):
        load_dotenv(envfile)

    # set vars to env values by default
    store_name = store_name if store_name else os.getenv('STORE_NAME')
    tenant_id = tenant_id if tenant_id else os.getenv('TENANT_ID')
    client_id = client_id if client_id else os.getenv('CLIENT_ID')
    client_secret = client_secret if client_secret else os.getenv('CLIENT_SECRET')

    token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    return core.AzureDLFileSystem(token, store_name=store_name)


def list_datalake_files(client, path):
    return client.ls(path)


def list_indices(client, streamset='prod'):
    ''' List indices available in the given streamset. '''
    return client.ls(os.path.join('/streamsets', streamset))


def open_datalake_file(client, path, mode='rb'):
    ''' Open a datalake file, returning the file handle. '''
    if not mode in ['rb', 'wb', 'ab']:
        raise Exception('Must use a binary file mode ("rb", "wb", or "ab")')
    return client.open(path, mode)


def download_datalake_file(client, remote_path, local_path,
                           overwrite=True,
                           nthreads=4,
                           buffersize=2**24,
                           blocksize=2**24,
                           ):
    ''' Download a file from ADL and write it to a local path. '''
    multithread.ADLDownloader(client, remote_path, local_path,
                              overwrite=overwrite,
                              nthreads=nthreads,
                              buffersize=buffersize,
                              blocksize=blocksize)


def upload_datalake_file(client, remote_path, local_path,
                         overwrite=True,
                         nthreads=4,
                         buffersize=2**24,
                         blocksize=2**24,
                         ):
    ''' Upload a local file to a remote path in the ADL. '''
    multithread.ADLUploader(client, remote_path, local_path,
                            overwrite=overwrite,
                            nthreads=nthreads,
                            buffersize=buffersize,
                            blocksize=blocksize)


def create_datalake_directory(client, dirpath):
    ''' Create a directory in the datalake at the given dirpath. '''
    client.mkdir(os.path.join('/', dirpath))


# default keys to keep from adl json records
KEEP_KEYS = ['content', 'collectedAt', 'publishedAt', 'createdAt', 'contentId', 'authorScreenName', 'authorUserId',
             'connectionType', 'parentScreenName', 'parentUserId', 'parentContentId']


def document_generator(client,
                       read_date=date.today(),
                       index='disney',
                       streamset='prod',
                       keep_keys=KEEP_KEYS,
                       include_retweets=False,
                       ):
    ''' Generator function for reading all data for the given read_date from
    the given index and streamset in ADL. Yields a single document at a time as a
    dictionary. '''

    # vprint = print if verbose else lambda x: None
    if isinstance(read_date, date):
        read_date = read_date.isoformat()
    elif not isinstance(read_date, str):
        raise Exception('date must be a string or datetime.date object')

    # vprint('finding files')
    # list out files for that day
    files = client.ls("/streamsets/{0}/{1}/{2}".format(streamset, index, read_date))
    for filename in files:
        # for i, filename in enumerate(files):
        # vprint('file', i+1, 'of', len(files))

        if '_tmp' in filename:
            # vprint('skipping temp file', filename)
            continue

        with client.open(filename, 'rb') as adl_file:
            # read json string from adl file, parse into docs and yield rows
            json_string = adl_file.read().decode(encoding='utf-8')
            json_string = '[' + ','.join(line for line in json_string.splitlines()) + ']'
            documents = json.loads(json_string)
            for row in documents:
                if not include_retweets and row['connectionType'] == 'retweet':
                    continue
                if keep_keys:
                    yield {k: v for k, v in row.items() if k in keep_keys}
                else:
                    yield row


class DatalakeFileManager:
    ''' Wrapper class for adl file manipulation that stores the adl client to be used in all functions '''

    def __init__(self, store_name=None, tenant_id=None, client_id=None, client_secret=None, envfile='/data_science_datalake.env'):
        self.client = get_datalake_client(store_name=store_name, tenant_id=tenant_id,
                                          client_id=client_id, client_secret=client_secret, envfile=envfile)

    def document_generator(self, **kwargs):
        return document_generator(self.client, **kwargs)

    def list_datalake_files(self, path):
        return list_datalake_files(self.client, path)

    def list_indices(self, streamset='prod'):
        return list_indices(self.client, streamset=streamset)

    def open_datalake_file(self, path, mode='rb'):
        return open_datalake_file(self.client, path, mode=mode)

    def create_datalake_directory(self, dirpath):
        return create_datalake_directory(self.client, dirpath)

    def download_datalake_file(self, remote_path, local_path, **kwargs):
        return download_datalake_file(self.client, remote_path, local_path, **kwargs)

    def upload_datalake_file(self, remote_path, local_path, **kwargs):
        return upload_datalake_file(self.client, remote_path, local_path, **kwargs)


# TODO crawl directory tree or specify date range to get more than one directory/day at a time
# TODO use logger?
