# iothub_util.py
import time
import json
import hmac
import base64
import hashlib
import urllib.parse
import logging
from typing import Dict
from azure.iot.device import IoTHubDeviceClient, Message
from azure.iot.hub import IoTHubRegistryManager
from db_util import *

# Configure logging
logging.basicConfig(level=logging.INFO)

# Constants
IOT_HUB_HOST = "iothubdevuae.azure-devices.net"
CONNECTION_STRING = "HostName=iothubdevuae.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=TgNmv49DIduLOsnHU7ccaESSOcXnpKu9UAIoTOMlm0s="

# Initialize IoT Hub Registry Manager
registry_manager = IoTHubRegistryManager(CONNECTION_STRING)



def generate_sas_token(uri: str, key: str, policy_name: str, expiry: int) -> str:
    encoded_uri = urllib.parse.quote(uri, safe='')
    signing_string = (encoded_uri + '\n' + str(expiry)).encode('utf-8')
    key = base64.b64decode(key.encode('utf-8'))
    signature = base64.b64encode(hmac.new(key, signing_string, digestmod=hashlib.sha256).digest())
    token = f'SharedAccessSignature sr={encoded_uri}&sig={urllib.parse.quote(signature.decode())}&se={expiry}'
    if policy_name:
        token += f'&skn={policy_name}'
    logging.info("SAS Token generated successfully.")
    return token

def initialize_iothub_client(connection_string: str) -> IoTHubDeviceClient:
    iot_hub_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    iot_hub_client.connect()
    return iot_hub_client

