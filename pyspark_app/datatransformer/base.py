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

DATETIME = 401
DATE = 402
TIME = 403
TIMEDELTA = 404

_DATA_TYPES = {
    BOOL:("bool","bool"),
    STRING:(lambda size:"S{}".format(size),h5py.string_dtype(encoding='utf-8')),
    EMAIL:("S64",h5py.string_dtype(encoding='ascii')),
    DATETIME:("S32",h5py.string_dtype(encoding='ascii')),
    INT8:("int8","int8"),
    UINT8:("uint8","uint8"),
    INT16:("int16","int16"),
    UINT16:("uint16","uint16"),
    INT32:("int32","int32"),
    UINT32:("uint32","uint32"),
    INT64:("int64","int64"),
    UNIXTIME:("int64","int64"),
    FLOAT16:("float16","float16"),
    FLOAT32:("float32","float32"),
    FLOAT64:("float64","float64"),
    FLOAT96:("float96","float96"),
    FLOAT128:("float128""float128")
}

def get_np_type(t,size=None):
    if callable(_DATA_TYPES[t][0]):
        return _DATA_TYPES[t][0](size)
    else:
        return _DATA_TYPES[t][0]


def get_hdf5_type(t,size=None):
    if callable(_DATA_TYPES[t][1]):
        return _DATA_TYPES[t][1](size or 64)
    else:
        return _DATA_TYPES[t][1]

def is_int_type(t):
    return t >= 100 and t < 200

def is_float_type(t):
    return t >= 200 and t < 300

def is_number_type(t):
    return t >= 100 and t < 300

def is_string_type(t):
    return t >= 300 and t < 400

def ceiling_type(t1,t2):
    if is_number_type(t1[0]) and is_number_type(t2[0]):
        if t1 == t2:
            return t1
        t1_np = get_np_type(*t1)
        t2_np = get_np_type(*t2)

        if t1_np.endswith(t2_np):
            return t1 + 1
        elif t2_np.endswith(t1_np):
            return t2 + 1
        else:
            return t2 if t2 > t1 else t1
    elif is_string_type(t1[0]) and is_string_type(t2[0]):
        t1_size = int(get_np_type(*t1)[1:])
        t2_size = int(get_np_type(*t2)[1:])
        return t1 if t1_size >= t2_size else t2
    else:
        raise Exception("Not support,t1={},t2={}".format(get_np_type(t1),get_np_type(t2)))
    
def bigger_type(t1,t2):
    if is_number_type(t1[0]) and is_number_type(t2[0]):
        return t1 if t1[0] >= t2[0] else t2
    elif is_string_type(t1[0]) and is_string_type(t2[0]):
        t1_size = int(get_np_type(*t1)[1:])
        t2_size = int(get_np_type(*t2)[1:])
        return t1 if t1_size >= t2_size else t2
    else:
        raise Exception("Not support,t1={},t2={}".format(get_np_type(t1),get_np_type(t2)))
    
