import os
import requests
import logging
from datetime import datetime,timedelta

from msal import ConfidentialClientApplication

from pyspark_app.utils import timezone

logger = logging.getLogger(__name__)

def ms_graph_client_token():
    azure_tenant_id = "697167b2-6317-42af-a14c-c970869a5007"
    client_id = "dcf64e07-c43e-4e27-8e41-d8bdcb10032d"
    client_secret = "EJR8Q~eSPaVZiP6IhKbZp~rAWia0ZdgKKml_qaW~"
    context = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority="https://login.microsoftonline.com/{}".format(azure_tenant_id),
    )
    scope = "https://graph.microsoft.com/.default"
    #scope = "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits"
    return context.acquire_token_for_client(scope)

def download_auditlog():
    """Query the Microsoft Graph REST API for on-premise user accounts in our tenancy.
    Passing ``licensed=True`` will return only those users having >0 licenses assigned.
    """
    token = ms_graph_client_token()
    headers = {
        "Authorization": "Bearer {}".format(token["access_token"]),
        "ConsistencyLevel": "eventual",
    }

    endtime = timezone.localtime().replace(hour=0,minute=0,second=0,microsecond=0)
    starttime = endtime - timedelta(days=1)

    
    url = "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$filter=loggedByService eq 'B2C' and activityDateTime gt {}".format(
        timezone.format(starttime,pattern="%Y-%m-%dT%H:%M:%SZ",timezone=timezone.UTC),
        timezone.format(endtime,pattern="%Y-%m-%dT%H:%M:%SZ",timezone=timezone.UTC)
    )
    logger.debug("url = {}".format(url))   
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        with open("./test.csv",'w') as f:
            f.write(resp.text)
    except Exception as ex:
        logger.error("Failed to process the request({}).{}".format(url,str(ex)))


