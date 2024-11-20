import logging
import types
import inspect
from datetime import timedelta

from . import timezone
from .. import utils

logger = logging.getLogger("pyspark_app.utils.dynamicfunction")

class DynamicFunction(object):
    """
    Two formats: list of declared function configures or the declared function configures 
    Each function configures is a tuple with 3 members
        1. function name
        2. the number of position arguments(required arguments)
    """
    f_name = None
    modules = None
    expiretime = 3600
    functions = None # {name:[refreshtime, func, [k=v,k=v], [k,k] ]
    f_module_name = lambda self:"{}_{}".format(self.__class__.__name__,self.name.replace(" ","_"))

    def __init__(self,databaseurl,name):
        self.databaseurl = databaseurl
        self.name = name

    def unload(self):
        if self.name in self.functions:
            del self.functions[self.name]
            logger.debug("Unload the function({0}.{1}.{2})".format(self.__class__.__name__,self.name,self.f_name[0]))

        if self.modules.get(self.name):
            module_name = self.f_module_name()
            del self.modules[self.name]
            logger.debug("Unload the module({0}.{1}.{2})".format(self.__class__.__name__,self.name,module_name))

    @property
    def function_metadata(self):
        """
        Return (function  metadata,cached)
        """
        return self._compile_code()
        
    @property
    def function(self):
        """
        Return (the function ,cached)
        """
        metadata,cached = self.function_metadata
        return (metadata[1],cached)
        
    @property
    def kwargs(self):
        """
        Return the keyword parameters as key=value separated by "," if have; otherwise return None
        """
        metadata,cached = self.function_metadata
        return (metadata[3],cached)

    @property
    def parameters(self):
        """
        Return the list of keywork parameter name if have; otherwise return None
        """
        metadata,cached = self.function_metadata
        return (metadata[2],cached)

    @property
    def f_decodes(self):
        """
        Return a map between parameter name and decode function if have;otherwise return None
        """
        metadata,cached = self.function_metadata
        return (metadata[4],cached)

    def _compile_code(self):
        """
        compile the code if necessary
        Throw exception if code is empty
        The function metadata is a list of function metadata which is a tuple with 5 members
            1. function name
            2. function
            3. the list of parameter name if have; otherwise is None
            4. the keyword parameter string which is a key=value separated by "," if have, otherwise is None
            5. the decode function map if have; otherwise is None
        return a tuple(function metadata, True if cached else False)
        
        """
        if self.name in self.functions and timezone.localtime() > self.functions[self.name][2]:
            #cached function metadata is up to date.
            return (self.functions[self.name][0],True)

        code,modified = self.load()
        if self.name in self.functions and modified < self.functions[self.name][1]:
            #cached function metadata is up to date.
            self.functions[self.name][2] = timezone.localtime() + timedelta(seconds=self.expiretime)
            return (self.functions[self.name][0],True)
        logger.debug("Load the function({}.{})".format(self.__class__.__name__,self.name))

        #code isn't initialize or the parsed function metadata is outdated, unload the module first if have one
        self.unload() 

        #parse the code 
        code  = code.strip() if code else None
        if not code :
            raise Exception("Code is required.")

        if code.startswith("lambda"):
            #lambda string
            try:
                f_func = eval(code)
                logger.debug("Successfully parse the lambda expression({1}) for {0}".format(self.name,code))
            except Exception as ex:
                raise Exception("Invalid lambda expression({}).{}".format(code,str(ex)))
            parameters,kwargs,f_decodes,supported_context_parameters = utils.get_kwargs(f_func,self.f_name[1])
            function_metadata = (self.f_name,f_func,parameters,kwargs,f_decodes)
        else:
            #code 
            #load the code as module
            module_name = self.f_module_name()
            try:
                codemodule = types.ModuleType(module_name)
                exec(code, codemodule.__dict__)
            except Exception as ex:
                raise Exception("Invalid code.{}".format(str(ex)))

            name,required_parameters = self.f_name
            #find the function metadata from module
            try:
                f_func = getattr(codemodule,name)
            except Exception as ex:
                raise Exception("Method '{}.{}' is not declared.".format(self.name,name))

            if not callable(f_func):
                raise Exception("Porperty '{}.{}' is not a method".format(self.name,name))

            argspec = inspect.getfullargspec(f_func)
            if argspec.varargs or argspec.varkw or argspec.kwonlyargs or ((len(argspec.args) if argspec.args else 0) - required_parameters) != (len(argspec.defaults) if argspec.defaults else 0):
                raise Exception("Function '{0}' should only have {} positional parameters and optional multiple keyword parameters.".format(name,required_parameters))

            parameters,kwargs,f_decodes = utils.get_kwargs(f_func,required_parameters)
            function_metadata = (name,f_func,parameters,kwargs,f_decodes)

            self.modules[self.name] = codemodule

        self.functions[self.name] = [function_metadata,timezone.localtime(),timezone.localtime() + timedelta(seconds=self.expiretime)]

        return (function_metadata,False)

    def decode_arguments(self,arguments):
        """
        Parse the argument from string to expected type
        """
        for k,f in self.f_decodes.items():
            try:
                arguments[k] = f(arguments[k])
            except:
                raise Exception("The parameter({0}) has an invalid value({1})".format(k,arguments[k]))

        return arguments

    def __call__(self,value,**kwargs):
        if kwargs:
            return self.function[0](value,**self.decode_arguments(kwargs))
        else:
            return self.function[0](value)

