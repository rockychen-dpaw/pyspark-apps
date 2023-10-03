class ResourceHarvester(object):
    def saveas(self,resource,f,columns=None,starttime=None,endtime=None):
        """
        Save the resource to the file 
        Parameters:
           resource: the resource path
           f: the file path where to save the resource
        """
        raise NotImplementedError("Please implement the method(saveas) in the subclass")


    def is_local(self):
        """
        Return True if the resource is stored locally and can be read directly.
        """
        return False

    def get_abs_path(self,resource):
        """
        return the absolute resource path, 
        """
        if self.is_local():
            raise NotImplementedError("Please implement the method(get_abs_path) in the subclass")
        else:
            raise AttributeError("The method(get_abs_path) is not supported.")
