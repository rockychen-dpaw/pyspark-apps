from .. import utils

_optional_params = ("databaseurl","columnid","context","record","columnname","return_id")
transformer_method_pattern1 = "lambda f,val,{0},**kwargs:f(val,{1},**kwargs)"
transformer_method_pattern2 = "lambda f,val,{0},**kwargs:f(val,**kwargs)"
        
def transformer_factory(f):
    kwargs = utils.get_kwargs(f,1)[0]
    if kwargs and any(p for p in _optional_params if p in kwargs):
        _func =  eval(transformer_method_pattern1.format((",".join("{}=None".format(p) for p in _optional_params )),(",".join("{0}={0}".format(p) for p in _optional_params if p in kwargs))))
    else:
        _func =  eval(transformer_method_pattern2.format((",".join("{}=None".format(p) for p in _optional_params )) ))

    return (_func,f)

