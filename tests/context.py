import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

from txpy.azurehelper.storage import AzBlobManagerSync
from txpy.azurehelper.storage import AzBlobManagerAsync
