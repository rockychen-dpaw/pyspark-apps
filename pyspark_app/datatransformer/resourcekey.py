
_configs = {}
def resourcekey(domain,databaseurl=None,columnid=None,columnname=None,context=None,record=None,path_index=None,queryparameters_index=None,configs=None):
    if not configs or not path_index or not queryparameters_index:
        return raise Exception("Some of the parameters('configs','path_index','queryparameters_index') are missing")
   
    path = record[path_index] or "/" 
    queryparameters = record[queryparameters_index] or ""
    try:
        func = _configs[domain]
    except KeyError as ex:
        func = None
        for k,v in _configs:
            if callable(k):
                if k(domain):
                    func = v
                    break
            elif k == domain:
                func = v
                break
        if func is None:
            func = lambda domain,path,queryparameters:None

        _configs[domain] = func

    val = func(domain,path,queryparameters)
    if not val:
        return 0
    if isinstance(val,int):
        return val
    


                

