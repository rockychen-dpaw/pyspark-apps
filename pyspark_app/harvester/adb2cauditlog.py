import os
import shutil
import json
import csv
import requests
import logging
import traceback
from datetime import datetime,timedelta

from msal import ConfidentialClientApplication

from pyspark_app.utils import timezone

from .base import ResourceHarvester
from ..utils import utils
from .exceptions import ResourceNotFound

logger = logging.getLogger(__name__)

initial_columns=[
    'activityDateTime',
    'category',
    'operationType',
    'activityDisplayName',
    'result',
    'resultReason',
    'loggedByService',
    'id',
    'correlationId',
    'initiatedBy.app.appId',
    'initiatedBy.app.displayName',
    'initiatedBy.app.servicePrincipalId',
    'initiatedBy.user.id',
    'initiatedBy.user.displayName',
    'initiatedBy.user.userPrincipalName',
    'initiatedBy.user.ipAddress',
    'initiatedBy.user.userType',
    'initiatedBy.user.homeTenantId',
    'initiatedBy.user.homeTenantName',
    'initiatedBy.app.servicePrincipalName',
    'additionalDetails.TenantId',
    'additionalDetails.PolicyId',
    'additionalDetails.ApplicationId',
    'additionalDetails.Client',
    'additionalDetails.GrantType',
    'additionalDetails.ClientIpAddress',
    'additionalDetails.DomainName',
    'additionalDetails.IdentityProviderName',
    'additionalDetails.IdentityProviderApplicationId',
    'additionalDetails.IdentityProviderUserId',
    'additionalDetails.LocalAccountUsername',
    'additionalDetails.Scopes',
    'additionalDetails.targetTenant',
    'additionalDetails.targetEntityType',
    'additionalDetails.actorIdentityType',
    'targetResource.User.id',
    'targetResource.User.displayName',
    'targetResource.User.userPrincipalName',
    'targetResource.User.groupType',
    'targetResource.User.modifiedProperties',
    'targetResource.Other.id',
    'targetResource.Other.displayName',
    'targetResource.Other.userPrincipalName',
    'targetResource.Other.groupType',
    'targetResource.Other.modifiedProperties',
]
class ADB2CAuditLogHarvester(ResourceHarvester):
    def __init__(self,tenant_id=None,client_id=None,client_secret=None,activitytime_column=None,activitytime_parse=None,data_ascending=False):
        self.tenant_id = tenant_id 
        self.client_id = client_id 
        self.client_secret = client_secret 
        self.activitytime_column = activitytime_column
        self.activitytime_parse = eval(activitytime_parse) if activitytime_parse else None
        self.data_ascending = data_ascending

    def ms_graph_client_token(self):
        context = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority="https://login.microsoftonline.com/{}".format(self.tenant_id),
        )
        scope = ["https://graph.microsoft.com/.default"]
        return context.acquire_token_for_client(scope)

    def parse_modifiedpropertyvalue(self):
        if not v:
            return ""
        v = json.loads(v)
        if not v:
            return ""
        elif len(v) == 1:
            return v[0]
        else:
            return ",".join(v)
        
    def itercolumns(self,row):
        try:
            for key,data in row.items():
                if  key in ("initiatedBy","targetResources","additionalDetails"):
                    continue
                if isinstance(data,dict):
                    raise Exception("Property '{} = {}' Not Support".format(key,data))
                elif self.activitytime_column and self.activitytime_column == key and self.activitytime_parse:
                    yield (key,self.activitytime_parse(data) if data else None)
                else:
                    yield (key,data)
        
            initiatedBy = row["initiatedBy"]
            for key,data in initiatedBy.items():
                if not data:
                    continue
                for prop,val in data.items():
                    yield ("initiatedBy.{}.{}".format(key,prop),val)
                                        
            additionalDetails = row["additionalDetails"]
            for detail in additionalDetails:
                yield ("additionalDetails.{}".format(detail["key"]),detail["value"])
        
            targetResources = row["targetResources"]
            for resource in targetResources:
                for  prop,val in resource.items():
                    if prop in ("type","modifiedProperties"):
                        continue
                    yield ("targetResource.{}.{}".format(resource["type"],prop),val)
                
                yield ("targetResource.{}.modifiedProperties".format(resource["type"]),"".join("({0}.old={1},{0}.new={2})".format(
                    modifiedProperty["displayName"],
                    parse_modifiedpropertyvalue(modifiedProperty["oldValue"]),
                    parse_modifiedpropertyvalue(modifiedProperty["newValue"])
                ) for modifiedProperty in resource["modifiedProperties"]))
        except:
            logger.error("Failed to parse the audit log,\r\n{}".format(json.dumps(row,indent=4)))
            raise

    def saveas(self,path,destfilename,columns=None,starttime=None,endtime=None):
        """
        Download the blob resource to a file
        """
        token = self.ms_graph_client_token()
        headers = {
            "Authorization": "Bearer {}".format(token["access_token"]),
            "ConsistencyLevel": "eventual",
        }
    
        url = "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$filter=loggedByService eq 'B2C' and activityDateTime ge {} and activityDateTime lt {}".format(
            timezone.format(starttime,pattern="%Y-%m-%dT%H:%M:%SZ",timezone=timezone.UTC),
            timezone.format(endtime,pattern="%Y-%m-%dT%H:%M:%SZ",timezone=timezone.UTC)
        )
        if columns:
            columns_changed = False
            has_original_columns = True
        else:
            columns = list(initial_columns)
            has_original_columns = True if columns else False
            columns_changed = True if columns else False
        column_map = dict([(columns[i],i) for i in range(len(columns))])

        tmpfilename = "{}.tmp".format(destfilename)
        try:
            counter = 0
            rowdata = [None] * len(columns)
            columns_changed_during_processing = False
            #save the file to a tmp file
            with open(tmpfilename,'w') as f:
                csvwriter = csv.writer(f)
                if has_original_columns:
                    csvwriter.writerow(columns)
                while True:
                    resp = requests.get(url, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()
                    rows = data['value']
                    counter += len(rows)
                    logger.debug("Download the adb2c audit log with url({}), Get {} records, totla {} records".format(url,len(rows),counter))
                    for row  in (rows if self.data_ascending else reversed(rows)):
                        #clear the rowdata value
                        for i in range(len(rowdata)):
                            rowdata[i] = None
                        #fill the rowdata 
                        for column in self.itercolumns(row):
                            if column[0] in column_map:
                                rowdata[column_map[column[0]]] = column[1]
                            else:
                                columns.append(column[0])
                                column_map[column[0]] = len(columns) - 1
                                rowdata.append(column[1])
                                columns_changed = True
                                columns_changed_during_processing = True
                                logger.debug("Found new column '{}'".format(column[0]))
    
                        csvwriter.writerow(rowdata)
    
                    if data.get("@odata.nextLink"):
                        url = data["@odata.nextLink"]
                    else:
                        break
    
            #clumns changed, write again
            if columns_changed_during_processing:
                header = has_original_columns
                with open(tmpfilename,'r') as f_reader:
                    with open(destfilename,'w') as f_writer:
                        csvreader = csv.reader(f_reader)
                        csvwriter = csv.writer(f_writer)
                        csvwriter.writerow(columns)
                        for row  in csvreader:
                            if header:
                                header = False
                            else:
                                #clear the rowdata value
                                for i in range(len(rowdata)):
                                    rowdata[i] = None

                                #fill the rowdata 
                                for i in range(len(row)):
                                    rowdata[i] = row[i]

                                csvwriter.writerow(rowdata)
            else:
                shutil.move(tmpfilename, destfilename)
                tmpfilename = None

            return (columns_changed,columns)
        except :
            logger.error("Failed to process the request({}).{}".format(url,traceback.format_exc()))
            raise
        finally:
            if tmpfilename:
                utils.remove_file(tmpfilename)

