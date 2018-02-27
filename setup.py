import sys
from os.path import dirname, join, realpath

from setuptools import find_packages, setup


def setup_package():

    dir_path = dirname(realpath(__file__))
    sys.path.append(join(dir_path, 'azure_utils'))  # TODO maybe not the best to append to sys.path...

    setup(
        name='azure-utils',
        version='0.1.0',
        packages=find_packages(),
        install_requires=[
            'azure-datalake-store >= 0.0.18',
            'python-dotenv >= 0.7.1',
        ],
    )


if __name__ == '__main__':
    setup_package()
