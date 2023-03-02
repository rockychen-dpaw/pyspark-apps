import shutil
import logging

from .base import ResourceHarvester

logger = logging.getLogger(__name__)

class LocalResourceHarvester(ResourceHarvester):
    def __init__(self,home=None):
        """
        Parameters
        home: the home folder where to get the resoruces
        """
        home = home.strip() if home else None
        if not home:
            raise Exception("Empty home folder for LocalResourceHarvester")

        if home.endswith("/"):
            self._home = home[:-1]
        else:
            self._home = home

    def saveas(self,resource,f):
        shutil.copyfile(self.get_abs_path(resource),f)

    def is_local(self):
        """
        Return True if the resource is stored locally and can be read directly.
        """
        return True

    def get_abs_path(self,resource):
        """
        return the absolute resource path, 
        """
        return "{}/{}".format(self._home,resource)
