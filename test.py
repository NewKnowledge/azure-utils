import subprocess
from os.path import isfile

from azure_utils import (download_files_from_datalake, get_datalake_client,
                         upload_files_to_datalake)


def test_datalake_file_management():
    ''' test that we can connect to the azure data lake, then upload and download files '''
    subprocess.call(["touch", "test.txt"])  # create empty test file
    print('initializing datalake client')
    client = get_datalake_client()
    print('uploading')
    upload_files_to_datalake(client, ['test.txt'])
    print('downloading')
    download_files_from_datalake(client, ['uploads/test.txt'])

    assert isfile('datalake-downloads/test.txt')

    # clean up
    subprocess.call(["rm", "test.txt"])
    subprocess.call(["rm", "datalake-downloads/test.txt"])


if __name__ == '__main__':
    test_datalake_file_management()
