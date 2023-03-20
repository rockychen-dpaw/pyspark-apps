from .. import settings
import logging

from collections import OrderedDict
from .localfs import LocalResourceHarvester
from .blobstorage import AzureBlobStorageHarvester

logger = logging.getLogger(__name__)

_harvester_map = OrderedDict()
harvesters = []

for h in [LocalResourceHarvester,AzureBlobStorageHarvester]:
    _harvester_map[h.__name__] = h
    harvesters.append((h.__name__,h.__name__))

def get_harvester(name,**kwargs):
    """
    Return a resource harvester instance
    """
    try:
        return _harvester_map[name](**kwargs)
    except KeyError as ex:
        raise Exception("ResourceHarvester({}) does not exist".format(name))



