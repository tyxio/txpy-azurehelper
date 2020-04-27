=======================
Azure Helper for Python
=======================
A wrapper around the Azure SDK for Python developed with the goal to hide the 
Azure SDK complexity, and so to allow easier use of Azure resources.

Introduction
------------
This library provides a suite of helper functions for Azure. It is built around the
`Microsoft SDK for Python <https://github.com/Azure/azure-sdk-for-python>`_. See also
`Azure for Python Developers <https://docs.microsoft.com/en-us/azure/developer/python/>`_ 
for additional information about the Azure support for Python. 

This library has been developed and tested with Python 3.7, Azure Core 1.3.0 and 
AzureStorage Blob 12.3.0.

Currently (V 0.1), only helper functions for the blob storage SDK are available. 
More functions will come in future releases.

Installing
----------

You can install the txpy-azurehelper package using:

$ pip install txpy-azurehelper
 
Getting the code
----------------

The code is hosted at https://github.com/tyxio/txpy-azurehelper

Check out the latest development version anonymously with::

    $ git clone https://github.com/tyxio/txpy-azurehelper.git
    $ cd txpy-azurehelper

To install dependencies::

    $ pip install -Ur requirements.testing.txt
    $ pip install -Ur requirements.txt


To install the minimal dependencies for production use (i.e., what is installed
with ``pip install txpy-azurehelper``) run::

    $ pip install -Ur requirements.txt

Running Tests
-------------

Note that tests require ```pip install unittest``` and ```pip install pyyaml``` 
(these are included if you have installed dependencies from ```requirements.testing.txt```)

To run the unit tests ::

    $ python -m unittest

Documentation
-------------

Read the documentation on `readthedocs.io <https://txpy-azurehelper.readthedocs.io/en/latest/>`_

If you want more information about Azure Storage Blobs for Python, see 
`Manage blobs with Python v12 SDK <https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python>`_.
