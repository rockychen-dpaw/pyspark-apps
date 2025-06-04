import numpy  as np
import logging

from ..datatransformer import is_number_type,get_np_type,is_string_type,get_type_shape

logger = logging.getLogger(__name__)

def _string_in(l,vals):
    result = None
    for val in vals:
        if result is None:
            result = np.char.equal(l,val)
        else:
            result |= np.char.equal(l,val)

    return result

_IP_RANGES={

}
def _in_iprange(ip,iprange):
    iprange_data = _IP_RANGES.get(iprange)
    if not iprange_data:
        try:
            if ":" in iprange:
                #ipv6
                subnetmask,cidrbits = iprange.rsplit("/",1)
                groups = int(cidrbits / 16)
                subnetmaskgroups = [int(d,16) if d else 0 for d in subnetmask.split(":")]
                if cidrbits % 16 > 0:
                    partgroup = int(subnetmask[5 * groups:5 * groups + 4],16)
                    mask = int("{}{}".format()"1" *  (cidrbits % 16),"0" * (16 - cidrbits * 16)),2)
                    partgroupmask = partgroup & mask
                    if groups == 0:
                        iprange_data = (601,partgroupmask)
                    else:
                        iprange_data = (611,groups,subnetmaskgroups[:groups],partgroupmask)
                elif groups == 0:
                    iprange_data = (600,)
                else:
                    iprange_data = (610,groups,subnetmaskgroups[:groups])
            else:
                subnetmask,cidrbits = iprange.rsplit("/",1)
                groups = int(cidrbits / 8)
    
                groups_endindex = -1
                for i in range(groups):
                    groups_endindex = subnetmask.index(".",groups_endindex)
    
                if cidrbits % 8 > 0:
                    partgroup = int(subnetmask[index:].split(".",1)[0])
                    mask = int("{}{}".format()"1" *  (cidrbits % 8),"0" * (8 - cidrbits * 8)),2)
                    partgroupmask = partgroup & mask
                    if groups == 0:
                        if groups == 3:
                            #the partgroup is the last group
                            iprange_data = (401,groups_endindex + 1,partgroupmask)
                        else:
                            iprange_data = (402,groups_endindex + 1,partgroupmask)
                    else:
                        if groups == 3:
                            #the partgroup is the last group
                            iprange_data = (411,subnetmask[:index],groups_endindex + 1,partgroupmask)
                        else:
                            iprange_data = (412,subnetmask[:index],groups_endindex + 1,partgroupmask)
                elif groups == 0:
                    iprange_data = (400,)
                else:
                    iprange_data = (410,subnetmask[:index])
        except:
            iprange_data = (0,)
        _IP_RANGES[iprange]=iprange_data
    
    if iprange_data[0] == 0:
        return False
    try:
        if ":" in ip:
            #ipv6
            if iprange_data[0] < 600:
                return False
            
            if iprange_data[0] == 610:
                #check subnet groups
                groups = ip.split(":",iprange_data[1])
                for i in range(iprange_data[1]):
                    if int(groups[i],16) != iprange_data[2][i]:
                        return False
            elif iprange_data[0] == 601:
                #check subnet part group
                partgroup = int(ip.split(":",1)[0],16)
                if partgroup & iprange_data[1] != iprange_data[1]:
                    return False
            elif iprange_data[0] == 611:
                groups = ip.split(":",iprange_data[1] + 1)
                #check subnet groups
                for i in range(iprange_data[1]):
                    if int(groups[i],16) != iprange_data[2][i]:
                        return False
    
                #check subnet part group
                if group[iprange_data[1]] & iprange_data[1] != iprange_data[1]:
                    return False
            else:
                return False
    
        else:
            #ipv4
            if iprange_data[0] > 500:
                return False
    
            #check the subnet groups
            if iprange_data[0] in (410,411,412):
                if not ip.startswith(iprange_data[1]):
                    return False
    
            #check the subnet part group
            if iprange_data[0] == 401:
                partgroup = int(ip[iprange_data[1]:])
                if partgroup & iprange_data[2] != iprange_data[2]:
                    return False
            elif iprange_data[0] == 402:
                partgroup = int(ip[iprange_data[1]:ip.index(".",iprange_data[1])])
                if partgroup & iprange_data[2] != iprange_data[2]:
                    return False
            elif iprange_data[0] == 411:
                partgroup = int(ip[iprange_data[2]:])
                if partgroup & iprange_data[3] != iprange_data[3]:
                    return False
            elif iprange_data[0] == 412:
                partgroup = int(ip[iprange_data[2]:ip.index(".",iprange_data[2])])
                if partgroup & iprange_data[3] != iprange_data[3]:
                    return False
            else:
                return False
    except:
        return False

    return True

number_npoperator_map = {
    "==":lambda val,cond:np.equal(val,cond),
    "=":lambda val,cond:np.equal(val,cond),
    "!=":lambda val,np.logical_not(cond:np.equal(val,cond)),
    "<>":lambda val,np.logical_not(cond:np.equal(val,cond)),
    ">":lambda val,cond:np.greater(val,cond),
    ">=":lambda val,cond:np.greater_equal(val,cond),
    "<":lambda val,cond:np.less(val,cond),
    "<=":lambda val,cond:np.less_equal(val,cond),
    "between":lambda val,cond:np.logical_and(np.greater_equal(val,cond[0]) , np.less(val,cond[1])),
    "in":lambda val,cond: np.any(np.equal(cond,val)),
    "not in":lambda val,cond: np.logical_not(np.any(np.equal(cond,val))),
    "avg_sum":lambda val:np.sum(val),
    "sum":lambda val:np.sum(val),
    "min":lambda val:np.min(val),
    "max":lambda val:np.max(val)
}

string_npoperator_map = {
    "==":lambda val,cond:np.equal(val,cond),
    "=":lambda val,cond:np.equal(val,cond),
    "!=":lambda val,cond:np.logical_not(np.equal(val,cond)),
    "<>":lambda val,cond:np.logical_not(np.equal(val,cond)),
    "in":lambda val,cond:np.any(np.equal(val,cond)),
    "contain":lambda val,cond:np.greater_equal(np.char.find(val,cond),0)
    "mcontain":lambda val,cond:np.any(np.greater_equal(np.char.find(val,cond),0)),
    "not contain":lambda val,cond:np.equal(np.char.find(val,cond),-1),
    "endswith":lambda val,cond:np.char.endswith(val,cond),
    "mendswith":lambda val,cond:np.any(np.char.endswith(val,cond)),
    "startswith":lambda val,cond:np.char.startswith(val,cond),
    "mstartswith":lambda val,cond:np.any(np.char.startswith(val,cond))
}

agg_operator_map = {
    "min":"min",
    "max":"max",
    "sum":"sum",
    "count":"count"
}

number_operator_map = {
    "==":lambda val,cond: l == cond,
    "=":lambda val,cond:l == cond,
    "!=":lambda val,cond: l != cond,
    "<>":lambda val,cond: l != cond,
    ">":lambda val,cond: l > cond,
    ">=":lambda val,cond: l >= cond,
    "<":lambda val,cond:l < cond,
    "<=":lambda val,cond:l <= cond,
    "between":lambda val,cond:l >= cond[0] and l < cond[1],
    "in":lambda val,conds: l in conds,
    "not in":lambda val,conds: l not in conds
}

string_operator_map = {
    "==":lambda val,cond:l == cond,
    "=":lambda val,cond:l == cond,
    "!=":lambda val,cond:l != cond,
    "<>":lambda val,cond:l != cond,
    "in":lambda val,conds: l in conds if l else False,
    "contain":lambda val,cond:cond in l if l else False,
    "mcontain":lambda val,cond:any((v in l) for v in cond) if l else False,
    "not contain":lambda val,cond: cond not in l if l else True,
    "endswith":lambda val,cond: l.endswith(cond) if l else False,
    "mendswith":lambda val,cond:any(l.endswith(v) for v in cond) if l else False,
    "startswith":lambda val,cond:l.startswith(cond) if l else False,
    "mstartswith":lambda val,cond:any(l.startswith(v) for v in cond) if l else False,
    "ip range":lambda val,cond: _in_iprange(val,cond),
    "not in ip range":lambda val,cond: not _in_iprange(val,cond),
    "ip ranges":lambda val,cond: any(_in_iprange(val,c) for c in cond),
    "not in ip ranges":lambda val,cond: not any(_in_iprange(ip,c) for c in cond)
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

    
