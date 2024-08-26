import time
import hmac
import base64
import hashlib
import urllib.parse
import json
import requests  # For making HTTP requests
import logging
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.device import IoTHubDeviceClient, Message
from const import *

# Initialize IoT Hub Registry Manager
registry_manager = IoTHubRegistryManager(CONNECTION_STRING)

last_sent_time = 0

def create_or_get_device(device_id):
    try:
        device = registry_manager.get_device(device_id)
        logging.info("Device already exists. Fetching existing device information.")
    except Exception as e:
        logging.error(f"Device does not exist. Error: {str(e)}. Creating a new device.")
        device = registry_manager.create_device_with_sas(
            device_id=device_id, primary_key=None, secondary_key=None, status="enabled")
        logging.info(f"Device ID: {device.device_id}")
        logging.info(f"Primary Key: {device.authentication.symmetric_key.primary_key}")
        logging.info(f"Secondary Key: {device.authentication.symmetric_key.secondary_key}")
    return device

def generate_sas_token(uri, key, policy_name, expiry):
    encoded_uri = urllib.parse.quote(uri, safe='')
    signing_string = (encoded_uri + '\n' + str(expiry)).encode('utf-8')
    key = base64.b64decode(key.encode('utf-8'))
    signature = base64.b64encode(hmac.new(key, signing_string, digestmod=hashlib.sha256).digest())
    token = f'SharedAccessSignature sr={encoded_uri}&sig={urllib.parse.quote(signature)}&se={expiry}'
    if policy_name:
        token += f'&skn={policy_name}'
    logging.info("SAS Token generated successfully.")
    return token

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to IoT Hub successfully.")
    else:
        logging.error(f"Failed to connect, return code {rc}")

def on_publish(client, userdata, mid):
    logging.info("Message published successfully.")

def on_disconnect(client, userdata, rc):
    logging.info(f"Disconnected from IoT Hub with result code: {str(rc)}")

def initialize_iothub_client(connection_string):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    logging.debug("IoT Hub Device Client initialized.")
    return client

def send_data_to_iot_hub(client: IoTHubDeviceClient, data: dict):
    payload = json.dumps(data)
    message = Message(payload)
    message.content_type = "application/json"
    message.content_encoding = "utf-8"
    try:
        client.send_message(message)
        logging.info(f"Data sent successfully: {payload}")
    except Exception as e:
        logging.error(f"Failed to send data. Error: {e}")

def get_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Will raise an error for 4xx/5xx responses
        data = response.json()
        if data:
            logging.info(f"Data received from API: {data}")
            return data  # Return the full list of data points
        else:
            logging.warning("No data received from API.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from API: {e}")
        return None

def send_messages(client):
    global last_sent_time
    current_time = int(time.time() * 1000)  
    if current_time - last_sent_time >= PAYLOAD_INTERVAL:
        data_list = get_data_from_api("http://127.0.0.1:8000/mqtt-data")
        if data_list:
            for data in data_list:
                # Assuming each item in data_list contains a dictionary with the required fields
                device_id = data.get("device_id")
                timestamp = data.get("T")
                if device_id:
                    device = create_or_get_device(device_id)
                    connection_string = f"HostName={IOT_HUB_HOST};DeviceId={device.device_id};SharedAccessKey={device.authentication.symmetric_key.primary_key}"
                    client = initialize_iothub_client(connection_string)
                    client.connect()
                    send_data_to_iot_hub(client, data)
                    client.disconnect()
        last_sent_time = current_time

def main():
    # Initialize client with default device; you might need to adjust this based on the actual device
    device = create_or_get_device(device_id)  # Adjust as necessary
    connection_string = f"HostName={IOT_HUB_HOST};DeviceId={device.device_id};SharedAccessKey={device.authentication.symmetric_key.primary_key}"
    client = initialize_iothub_client(connection_string)
    client.connect()
    global last_sent_time
    last_sent_time = int(time.time() * 1000) - PAYLOAD_INTERVAL

    while True:
        send_messages(client)
        time.sleep(10)  # Check every 10 seconds; adjust as needed

if __name__ == "__main__":
    main()
