import h5py

BOOL = 1

INT8 = 101
UINT8 = 102
INT16 = 103
UINT16 = 104
INT32 = 105
UINT32 = 106
INT64 = 107
UNIXTIME = 109

FLOAT16 = 201
FLOAT32 = 202
FLOAT64 = 203
FLOAT96 = 204
FLOAT128 = 205

STRING = 301
EMAIL = 302


LIST_BOOL = 1001

LIST_INT8 = 1101
LIST_UINT8 = 1102
LIST_INT16 = 1103
LIST_UINT16 = 1104
LIST_INT32 = 1105
LIST_UINT32 = 1106
LIST_INT64 = 1107
LIST_UNIXTIME = 1109

LIST_FLOAT16 = 1201
LIST_FLOAT32 = 1202
LIST_FLOAT64 = 1203
LIST_FLOAT96 = 1204
LIST_FLOAT128 = 1205

LIST_STRING = 1301
LIST_EMAIL = 1302

def _string_type(params):
    try:
        return "S{}".format(params["size"])
    except:
        raise "Missing the parameter 'size' of the type 'STRING'"

_DATA_TYPES = {
    BOOL:("bool","bool",False,None),
    STRING:(_string_type,h5py.string_dtype(encoding='utf-8'),b'',None),
    EMAIL:("S64",h5py.string_dtype(encoding='ascii'),b'',None),
    INT8:("int8","int8",0,INT16),
    UINT8:("uint8","uint8",0,INT16),
    INT16:("int16","int16",0,INT32),
    UINT16:("uint16","uint16",0,INT32),
    INT32:("int32","int32",0,INT64),
    UINT32:("uint32","uint32",0,INT64),
    INT64:("int64","int64",0,None),
    UNIXTIME:("int64","int64",0,None),
    FLOAT16:("float16","float16",0,FLOAT32),
    FLOAT32:("float32","float32",0,FLOAT64),
    FLOAT64:("float64","float64",0,FLOAT96),
    FLOAT96:("float96","float96",0,FLOAT128),
    FLOAT128:("float128","float128",0,None),

    LIST_BOOL:("bool","bool",False,None),
    LIST_STRING:(_string_type,h5py.string_dtype(encoding='utf-8'),b'',None),
    LIST_EMAIL:("S64",h5py.string_dtype(encoding='ascii'),b'',None),
    LIST_INT8:("int8","int8",0,INT16),
    LIST_UINT8:("uint8","uint8",0,INT16),
    LIST_INT16:("int16","int16",0,INT32),
    LIST_UINT16:("uint16","uint16",0,INT32),
    LIST_INT32:("int32","int32",0,INT64),
    LIST_UINT32:("uint32","uint32",0,INT64),
    LIST_INT64:("int64","int64",0,None),
    LIST_UNIXTIME:("int64","int64",0,None),
    LIST_FLOAT16:("float16","float16",0,FLOAT32),
    LIST_FLOAT32:("float32","float32",0,FLOAT64),
    LIST_FLOAT64:("float64","float64",0,FLOAT96),
    LIST_FLOAT96:("float96","float96",0,FLOAT128),
    LIST_FLOAT128:("float128","float128",0,None),
}

def get_default_value(t):
    return _DATA_TYPES[t][2]

def get_list_size(t,params):
    if t < 1000:
        raise Exception("{} is not a list type".format(t))
    dimension = params.get("dimension") if params else None
    if not dimension:
        raise Exception("Please configure the dimension of data type '{}'".format(get_np_type_desc(t,params)))

    if isinstance(dimension,int):
        return dimension
    else:
        raise Exception("Don't support multi dimensions of data type '{}'".format(get_np_type_desc(t,params)))


def get_type_shape(t,params,datasize):
    if t < 1000:
        return (datasize,)
    dimension = params.get("dimension") if params else None
    if not dimension:
        raise Exception("Please configure the dimension of data type '{}'".format(get_np_type_desc(t,params)))

    if isinstance(dimension,int):
        return (datasize,dimension)
    else:
        raise Exception("Don't support multi dimensions of data type '{}'".format(get_np_type_desc(t,params)))

def get_np_type(t,params=None):
    if callable(_DATA_TYPES[t][0]):
        return _DATA_TYPES[t][0](params)
    else:
        return _DATA_TYPES[t][0]

def get_np_type_desc(t,params=None):
    basetype = get_np_type(t,params)
    if is_list_type(t):
        return "list<{}>".format(basetype)
    else:
        return basetype


def get_hdf5_type(t,params=None):
    if callable(_DATA_TYPES[t][1]):
        return _DATA_TYPES[t][1](params)
    else:
        return _DATA_TYPES[t][1]

def is_bool_type(t):
    t = t % 1000
    return t == 1

def is_int_type(t):
    t = t % 1000
    return t >= 100 and t < 200

def is_float_type(t):
    t = t % 1000
    return t >= 200 and t < 300

def is_number_type(t):
    t = t % 1000
    return t >= 100 and t < 300

def is_string_type(t):
    t = t % 1000
    return t >= 300 and t < 400

def is_list_type(t):
    return t > 1000

def ceiling_type(t1,t2):
    """
    Raise Exception if t1 and t2 are not compatible.
    Return the ceiling type; if can't be shared, return None;  
    """
    if is_number_type(t1[0]) and is_number_type(t2[0]):
        if t1 == t2:
            result = t1
        t1_np = get_np_type(*t1)
        t2_np = get_np_type(*t2)

        if t1_np.endswith(t2_np):
            #use the next bigger integer as the ceiling type
            if _DATA_TYPES[t1[0]][3]:
                result = (_DATA_TYPES[t1[0]][3],t1[1])
            else:
                return None
        elif t2_np.endswith(t1_np):
            #use the next bigger integer as the ceiling type
            if _DATA_TYPES[t2[0]][3]:
                result = (_DATA_TYPES[t2[0]][3],t2[1])
            else:
                return None
        else:
            result =  t2 if t2[0] > t1[0] else t1
    elif is_string_type(t1[0]) and is_string_type(t2[0]):
        t1_size = int(get_np_type(*t1)[1:])
        t2_size = int(get_np_type(*t2)[1:])
        result = t1 if t1_size >= t2_size else t2
    elif is_bool_type(t1[0]) and is_bool_type(t2[0]):
        result = t1
    else:
        raise Exception("ceiling_type: Not support,t1={},t2={}".format(get_np_type_desc(*t1),get_np_type_desc(*t2)))

    if is_list_type(t1[0]) and is_list_type(t2[0]):
        t1dimension = t1[1].get("dimension") if t1[1] else None
        if not t1dimension:
            raise Exception("Missing the dimension of data type '{}')".format(get_np_type_desc(*t1)))
        elif not isinstance(t1dimension,int):
            raise Exception("Don't support multi dimensions of data type '{}')".format(get_np_type_desc(*t1)))

        t2dimension = t2[1].get("dimension") if t2[1] else None
        if not t2dimension:
            raise Exception("Missing the dimension of data type '{}')".format(get_np_type_desc(*t2)))
        elif not isinstance(t2dimension,int):
            raise Exception("Don't support multi dimensions of data type '{}')".format(get_np_type_desc(*t2)))
        
        if t1dimension == t2dimension:
            return result
        else:
            #array with different dimension can't be shared
            return None
    elif not is_list_type(t1[0]) and not is_list_type(t2[0]):
        return result

    raise Exception("ceiling_type: Not support,t1={},t2={}".format(get_np_type_desc(*t1),get_np_type_desc(*t2)))
    
def bigger_type(t1,t2):
    """
    Raise Exception if t1 and t2 are not compatible.
    Return the bigger type; if can't be shared, return None;  
    """
    if is_number_type(t1[0]) and is_number_type(t2[0]):
        result = t1 if t1[0] >= t2[0] else t2
    elif is_string_type(t1[0]) and is_string_type(t2[0]):
        t1_size = int(get_np_type(*t1)[1:])
        t2_size = int(get_np_type(*t2)[1:])
        result = t1 if t1_size >= t2_size else t2
    elif is_bool_type(t1[0]) and is_bool_type(t2[0]):
        result = t1 
    else:
        raise Exception("bigger_type: Not support,t1={},t2={}".format(get_np_type_desc(*t1),get_np_type_desc(*t2)))

    if is_list_type(t1[0]) and is_list_type(t2[0]):
        t1dimension = t1[1].get("dimension") if t1[1] else None
        if not t1dimension:
            raise Exception("Missing the dimension of data type '{}')".format(get_np_type_desc(*t1)))
        elif not isinstance(t1dimension,int):
            raise Exception("Don't support multi dimensions of data type '{}')".format(get_np_type_desc(*t1)))

        t2dimension = t2[1].get("dimension") if t2[1] else None
        if not t2dimension:
            raise Exception("Missing the dimension of data type '{}')".format(get_np_type_desc(*t2)))
        elif not isinstance(t2dimension,int):
            raise Exception("Don't support multi dimensions of data type '{}')".format(get_np_type_desc(*t2)))
        
        if t1dimension == t2dimension:
            return result
        else:
            #array with different dimension can't be shared
            return None

    elif not is_list_type(t1[0]) and not is_list_type(t2[0]):
        return result

    raise Exception("ceiling_type: Not support,t1={},t2={}".format(get_np_type_desc(*t1),get_np_type_desc(*t2)))

    
