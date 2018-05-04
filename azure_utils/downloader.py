''' Download data for Azure Data Lake '''
import os
import json
from datetime import date, timedelta

from dotenv import load_dotenv

from azure.datalake.store import core, lib


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


def get_data(date=None, index=None, store_name='sociallake', envfile='/datalake.env'):
    ''' Download data for Azure Data Lake into memory and return list of parsed json objects '''
    if date is None or index is None:
        raise Exception("Please provide keyword args 'date' and 'index'")

    # validate date input
    date = date.isoformat()

    # validate index/folder name
    client = get_datalake_client(store_name=store_name, envfile=envfile)

    # list out files for that day
    files = client.ls("/streamsets/prod/{0}/{1}".format(index, date))

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


if __name__ == '__main__':
    get_data(date=date.today() - timedelta(days=1), index='disney')
