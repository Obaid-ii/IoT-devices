import time
import urllib.parse
import hmac
import hashlib
import base64
import random
import requests
import json
from azure.iot.hub import IoTHubRegistryManager

# IoT Hub settings
connection_string = "HostName=iothubdevuae.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=TgNmv49DIduLOsnHU7ccaESSOcXnpKu9UAIoTOMlm0s="
iot_hub_name = "iothubdevuae"
device_id = "obaid_device2"
shared_access_key = "TgNmv49DIduLOsnHU7ccaESSOcXnpKu9UAIoTOMlm0s="

def generate_sas_token(iot_hub_name, device_id, shared_access_key, expiry=3600):
    """
    Generate a SAS token for the given IoT Hub and device ID.
    """
    uri = urllib.parse.quote(f"{iot_hub_name}.azure-devices.net/devices/{device_id}")
    ttl = int(time.time()) + expiry
    string_to_sign = f"{uri}\n{ttl}"
    
    # Create the HMAC SHA256 hash of the string to sign
    signed_hmac = hmac.HMAC(
        base64.b64decode(shared_access_key),
        string_to_sign.encode("utf-8"),
        hashlib.sha256
    ).digest()
    
    # Create the SAS token
    token = f"SharedAccessSignature sr={uri}&sig={base64.b64encode(signed_hmac).decode('utf-8')}&se={ttl}&skn=iothubowner"
    return token

def create_device():
    # Create an instance of the IoTHubRegistryManager
    registry_manager = IoTHubRegistryManager(connection_string)

    # Create a device in IoT Hub
    device = registry_manager.create_device_with_sas(
        device_id=device_id,
        primary_key="",
        secondary_key="",
        status="enabled"
    )

    print(f"Device '{device.device_id}' created with status: {device.status}")

def send_random_data(sas_token):
    # Generate random data
    random_data = random.randint(0, 100)

    # Message to send
    message = {"data": random_data}
    
    # Endpoint for sending messages
    endpoint = f"https://{iot_hub_name}.azure-devices.net/devices/{device_id}/messages/events?api-version=2021-04-12"

    # Send the message to the device
    headers = {
        "Authorization": sas_token,
        "Content-Type": "application/json",
        "Content-Length": str(len(json.dumps(message)))
    }

    response = requests.post(endpoint, headers=headers, data=json.dumps(message))

    if response.status_code == 204:
        print(f"Message sent successfully: {message}")
    else:
        print(f"Failed to send message: {response.status_code} {response.text}")

if __name__ == "__main__":
    # Create the device
    create_device()

    # Generate SAS token
    sas_token = generate_sas_token(iot_hub_name, device_id, shared_access_key)
    
    # Send random data using the generated SAS token
    send_random_data(sas_token)
