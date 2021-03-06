# Azure Utils

A set of utilities for interacting with Azure Cloud from Python. Most of the functionality is provided by open source libraries written by Microsoft. But because these libraries are new and still being documented, and Azure concepts can be confusing to users of other cloud services, we put together this little toolbelt to help you get up and running faster.

## Installation

Clone from Github (this isn't on pypi yet), then install the package:

`git clone https://github.com/NewKnowledge/azure-utils.git`

`cd azure-utils && python setup.py install`

## Azure Setup and Authentication

Access Azure resources (like a data lake, or an instance) is granted to applications. You'll use the application id (also known as the "client id") and a key (also known as the "client secret") to authenticate the application. 

[Here are instructions for registering an application inside Azure](https://www.netiq.com/communities/cool-solutions/creating-application-client-id-client-secret-microsoft-azure-new-portal/), and generating a key.

You'll also need your subscription's directory id (also known as the "tenant id"). [Here's how to find that inside Azure](http://stackoverflow.com/a/41028320).

After you create the application and give it a key, you need to assign it to a role that has permission to access the resources you need (for example, a "Contibutor" role in Azure Data Lake can read and write objects to that resource). [Here are instructions for assigning the application to a role](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal#assign-application-to-role).

Keeping track of all these client/application/directory/tenant ids and secrets is a pain, so by convention we've added a `config.py` file to the repo where you can store these values. The authentication utilities in `client.py` allow you to pass these credentials, or fall back to the values in your config. There are values our auth utils allow you to set as configuration options. 

| Config Option | Description  |
|---|---|
| ADL_CLIENT_ID | Application/client id of the application with Contributor access to Azure Data Lake  |
| ADL_CLIENT_SECRET | Key/client secret of the application with Contributor access to Azure Data Lake  |
| TENANT_ID | Directory/tenant id of your Azure subscription account |

## Doing Stuff

### Azure Data Lake

The [Azure Data Lake Store library](https://github.com/Azure/azure-data-lake-store-python/blob/master/azure/datalake/store/core.py) on pypi and Github provides most of the functionality you need. We've added a few utility functions: 

#### Getting a client

This gets a client instance using the auth values and in your .env file (`datalake.env` by default).

```python
from azure_utils import get_datalake_client
adl = get_datalake_client(envfile='datalake.env')
```

#### Using the client

The `adl` object in the code snippet above is an instance of [AzureDLFileSystem](https://github.com/Azure/azure-data-lake-store-python/blob/master/azure/datalake/store/core.py#L38).

```python
# list contents
adl.ls('')

# copy a local file to the lake
adl.put('your-local-file.txt','some-dir/your-lake-file.txt')

# get a file from the lake
adl.get('some-dir/your-lake-file.txt', 'your-local-file.txt')
```

Most of the `AzureDLFileSystem` methods mirror bash file system directives, and treat the data lake as a file system. `ls` to list directory contents, `mkdir` to create a directory, `rm` to remove a file, etc. 

#### Utilities 

Download/Upload a list of files to/from the datalake:

```python
from azure_utils import datalake_upload, datalake_download
datalake_upload(adl, ['list/of/local', 'file/paths'], destination_dir='remote/dir')
datalake_download(adl, ['list/of/remote', 'file/paths'], destination_dir='local/dir')
```

 