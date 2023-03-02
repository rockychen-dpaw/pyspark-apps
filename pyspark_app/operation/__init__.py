import numpy  as np

from ..datatransformer import is_number_type,get_np_type,is_string_type

def _number_in(l,vals):
    result = None
    for val in vals:
        if result is None:
            result = np.equal(l,val)
        else:
            result |= np.equal(l,val)

    return result

def _string_in(l,vals):
    result = None
    for val in vals:
        if result is None:
            result = np.equal(l,(val or "").encode())
        else:
            result |= np.equal(l,(val or "").encode())

    return result

number_operator_map = {
    "==":lambda l,val:np.equal(l,val),
    "=":lambda l,val:np.equal(l,val),
    "!=":lambda l,val:np.equal(l,val) == False,
    "<>":lambda l,val:np.equal(l,val) == False,
    ">":lambda l,val:np.greater(l,val),
    ">=":lambda l,val:np.greater_equal(l,val),
    "<":lambda l,val:np.less(l,val),
    "<=":lambda l,val:np.less_equal(l,val),
    "between":lambda l,val:np.greater_equal(l,val[0]) & np.less(l,val[1]),
    "in":_number_in,
    "avg":lambda l:[np.sum(l),l.shape[0]],
    "sum":lambda l:np.sum(l),
    "min":lambda l:np.min(l),
    "max":lambda l:np.max(l)
}

string_operator_map = {
    "==":lambda l,val:np.equal(l,(val or "").encode()),
    "=":lambda l,val:np.equal(l,(val or "").encode()),
    "!=":lambda l,val:np.equal(l,(val or "").encode()) == False,
    "<>":lambda l,val:np.equal(l,(val or "").encode()) == False,
    "in":_string_in,
    "contain":lambda l,val:np.core.defchararray.find(l,(val or "").encode())!=-1,
    "not contain":lambda l,val:np.core.defchararray.find(l,(val or "").encode())==-1
}

def _merge_avg(d1,d2):
    if d1 is None:
        return d2
    elif d2 is None:
        return d1
    elif isinstance(d1,list):
        d1[0] += d2[0]
        d1[1] += d2[1]
        return d1
    elif isinstance(d2,list):
        d2[0] += d1[0]
        d2[1] += d1[1]
        return d2
    else:
        return [d1[0] + d2[0],d1[1] + d2[1]]

merge_operator_map = {
    "count":lambda d1,d2: d1 + d2,
    "min":lambda d1,d2: d1 if d1 <= d2 else d2,
    "max":lambda d1,d2: d1 if d1 >= d2 else d2,
    "sum":lambda d1,d2: d2 + d2,
    "avg":_merge_avg
}

def get_func(dtype,operator):
    if is_number_type(dtype):
        try:
            return number_operator_map[operator]
        except KeyError as ex:
            raise Exception("Operator({}) is not supported for type({})".format(operator,get_np_type(dtype)))
    elif is_string_type(dtype):
        try:
            return string_operator_map[operator]
        except KeyError as ex:
            raise Exception("Operator({}) is not supported for type({})".format(operator,get_np_type(dtype)))
    else:
        raise Exception("Operator({}) is not supported for type({})".format(operator,get_np_type(dtype)))


def get_merge_func(operator):
    try:
        return merge_operator_map[operator]
    except KeyError as ex:
        raise Exception("Merging the result of the operator({}) is not supported ".format(operator))
    
