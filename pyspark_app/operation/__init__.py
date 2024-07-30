import numpy  as np
import logging

from ..datatransformer import is_number_type,get_np_type,is_string_type

logger = logging.getLogger(__name__)

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
            result = np.char.equal(l,val)
        else:
            result |= np.char.equal(l,val)

    return result

number_npoperator_map = {
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
    "avg_sum":lambda l:np.sum(l),
    "sum":lambda l:np.sum(l),
    "min":lambda l:np.min(l),
    "max":lambda l:np.max(l)
}

string_npoperator_map = {
    "==":lambda l,val:np.char.equal(l,val),
    "=":lambda l,val:np.char.equal(l,val),
    "!=":lambda l,val:np.char.equal(l,val) == False,
    "<>":lambda l,val:np.char.equal(l,val) == False,
    "in":_string_in,
    "contain":lambda l,val:np.char.find(l,val)!=-1,
    "mcontain":lambda l,val:np.logical_or.reduce([np.char.find(l,val)!=-1 for v in val]),
    "not contain":lambda l,val:np.char.find(l,val)==-1,
    "endswith":lambda l,val:np.char.endswith(l,val),
    "mendswith":lambda l,val:np.logical_or.reduce([np.char.endswith(l,v) for v in val]),
    "startswith":lambda l,val:np.char.startswith(l,val),
    "mstartswith":lambda l,val:np.logical_or.reduce([np.char.startswith(l,v) for v in val])
}

agg_operator_map = {
    "min":"min",
    "max":"max",
    "sum":"sum",
    "count":"count"
}

number_operator_map = {
    "==":lambda l,val: l == val,
    "=":lambda l,val:l == val,
    "!=":lambda l,val: l != val,
    "<>":lambda l,val: l != val,
    ">":lambda l,val: l > val,
    ">=":lambda l,val: l >= val,
    "<":lambda l,val:l < val,
    "<=":lambda l,val:l <= val,
    "between":lambda l,val:l >= val[0] & l < val[1],
    "in":lambda l,vals: l in vals
}

string_operator_map = {
    "==":lambda l,val:l == val,
    "=":lambda l,val:l == val,
    "!=":lambda l,val:l != val,
    "<>":lambda l,val:l != val,
    "in":lambda l,vals: l in vals if l else False,
    "contain":lambda l,val:val in l if l else False,
    "mcontain":lambda l,val:any((v in l) for v in val) if l else False,
    "not contain":lambda l,val: val not in l if l else True,
    "endswith":lambda l,val: l.endswith(val) if l else False,
    "mendswith":lambda l,val:any(l.endswith(v) for v in val) if l else False,
    "startswith":lambda l,val:l.startswith(val) if l else False,
    "mstartswith":lambda l,val:any(l.startswith(v) for v in val) if l else False
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

def _merge_count(d1,d2):
    return d1 + d2

merge_operator_map = {
    "count":_merge_count,#lambda d1,d2: int(d1 + d2),
    "min":lambda d1,d2: d1 if d1 <= d2 else d2,
    "max":lambda d1,d2: d1 if d1 >= d2 else d2,
    "sum":lambda d1,d2: d1 + d2,
    "distinct":lambda d1,d2: d1 + d2
}

def get_npfunc(dtype,operator):
    if is_number_type(dtype):
        try:
            return number_npoperator_map[operator]
        except KeyError as ex:
            raise Exception("Operator({}) is not supported for type({})".format(operator,get_np_type(dtype)))
    elif is_string_type(dtype):
        try:
            return string_npoperator_map[operator]
        except KeyError as ex:
            raise Exception("Operator({}) is not supported for type({})".format(operator,get_np_type(dtype)))
    else:
        raise Exception("Operator({}) is not supported for type({})".format(operator,get_np_type(dtype)))

def get_func(dtype,operator):
    if is_number_type(dtype):
        try:
            return number_operator_map[operator]
        except KeyError as ex:
            raise Exception("Operator({}) is not supported for type({})".format(operator,dtype))
    elif is_string_type(dtype):
        try:
            return string_operator_map[operator]
        except KeyError as ex:
            raise Exception("Operator({}) is not supported for type({})".format(operator,dtype))
    else:
        raise Exception("Operator({}) is not supported for type({})".format(operator,dtype))


def get_merge_func(operator):
    try:
        return merge_operator_map[operator]
    except KeyError as ex:
        raise Exception("Merging the result of the operator({}) is not supported ".format(operator))

def get_agg_func(operator):
    try:
        return agg_operator_map[operator]
    except KeyError as ex:
        raise Exception("Aggregation operator({}) is not supported ".format(operator))

    
