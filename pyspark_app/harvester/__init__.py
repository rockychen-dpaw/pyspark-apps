from .. import settings
import logging

from collections import OrderedDict
from .localfs import LocalResourceHarvester

logger = logging.getLogger(__name__)

_harvester_map = OrderedDict()
harvesters = []

for h in [LocalResourceHarvester]:
    _harvester_map[h.__name__] = h
    harvesters.append((h.__name__,h.__name__))

def get_harvester(name,**kwargs):
    """
    Return a resource harvester instance
    """
    try:
        logger.debug("harvester map = {}".format( [k for k in _harvester_map.keys()]))
        return _harvester_map[name](**kwargs)
    except KeyError as ex:
        raise Exception("ResourceHarvester({}) does not exist".format(name))



