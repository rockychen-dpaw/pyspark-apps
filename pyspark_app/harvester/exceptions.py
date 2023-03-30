class ResourceNotFound(Exception):
    def __init__(self,resource):
        super().__init__("The resource({}) does not exist".format(resource))
