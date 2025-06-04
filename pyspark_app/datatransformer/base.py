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

_DATA_TYPES = {
    BOOL:("bool","bool",False,),
    STRING:(lambda params:"S{}".format(params["size"]),h5py.string_dtype(encoding='utf-8'),b''),
    EMAIL:("S64",h5py.string_dtype(encoding='ascii'),b''),
    INT8:("int8","int8",0),
    UINT8:("uint8","uint8",0),
    INT16:("int16","int16",0),
    UINT16:("uint16","uint16",0),
    INT32:("int32","int32",0),
    UINT32:("uint32","uint32",0),
    INT64:("int64","int64",0),
    UNIXTIME:("int64","int64",0),
    FLOAT16:("float16","float16",0),
    FLOAT32:("float32","float32",0),
    FLOAT64:("float64","float64",0),
    FLOAT96:("float96","float96",0),
    FLOAT128:("float128","float128",0),

    LIST_BOOL:("bool","bool",False,),
    LIST_STRING:(lambda params:"S{}".format(params["size"]),h5py.string_dtype(encoding='utf-8'),b''),
    LIST_EMAIL:("S64",h5py.string_dtype(encoding='ascii'),b''),
    LIST_INT8:("int8","int8",0),
    LIST_UINT8:("uint8","uint8",0),
    LIST_INT16:("int16","int16",0),
    LIST_UINT16:("uint16","uint16",0),
    LIST_INT32:("int32","int32",0),
    LIST_UINT32:("uint32","uint32",0),
    LIST_INT64:("int64","int64",0),
    LIST_UNIXTIME:("int64","int64",0),
    LIST_FLOAT16:("float16","float16",0),
    LIST_FLOAT32:("float32","float32",0),
    LIST_FLOAT64:("float64","float64",0),
    LIST_FLOAT96:("float96","float96",0),
    LIST_FLOAT128:("float128","float128",0)
}

def get_default_value(t):
    return _DATA_TYPES[t][2]

def get_type_shape(t,params=None):
    if t < 1000:
        return (datasize,)
    dimension = params.get("dimension") if params else None
    if not dimension:
        raise Exception("Please configure the dimension of the list type({})".format(t))
    if isinstance(dimension,int):
        return (datasize,dimension)
    else:
        return (datasize,*dimension)

def get_np_type(t,params=None):
    if callable(_DATA_TYPES[t][0]):
        return _DATA_TYPES[t][0](params)
    else:
        return _DATA_TYPES[t][0]


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

def is_list(t):
    return t > 1000

def ceiling_type(t1,t2):
    if is_number_type(t1[0]) and is_number_type(t2[0]):
        if t1 == t2:
            return t1
        t1_np = get_np_type(*t1)
        t2_np = get_np_type(*t2)

        if t1_np.endswith(t2_np):
            #use the next bigger integer as the ceiling type
            return (t1[0] + 1,None)
        elif t2_np.endswith(t1_np):
            #use the next bigger integer as the ceiling type
            return (t2[0] + 1,None)
        else:
            return t2 if t2[0] > t1[0] else t1
    elif is_string_type(t1[0]) and is_string_type(t2[0]):
        t1_size = int(get_np_type(*t1)[1:])
        t2_size = int(get_np_type(*t2)[1:])
        return t1 if t1_size >= t2_size else t2
    elif is_bool_type(t1[0]) and is_bool_type(t2[0]):
        return t1
    else:
        raise Exception("Not support,t1={},t2={}".format(get_np_type(t1),get_np_type(t2)))
    
def bigger_type(t1,t2):
    if is_number_type(t1[0]) and is_number_type(t2[0]):
        return t1 if t1[0] >= t2[0] else t2
    elif is_string_type(t1[0]) and is_string_type(t2[0]):
        t1_size = int(get_np_type(*t1)[1:])
        t2_size = int(get_np_type(*t2)[1:])
        return t1 if t1_size >= t2_size else t2
    if is_bool_type(t1[0]) and is_bool_type(t2[0]):
        return t1 
    else:
        raise Exception("Not support,t1={},t2={}".format(get_np_type(t1),get_np_type(t2)))
    
