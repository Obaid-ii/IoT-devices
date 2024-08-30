import psycopg2
import time
import hmac
import base64
import hashlib
import urllib.parse
import json
import requests
import logging
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.device import IoTHubDeviceClient, Message
from fastapi import FastAPI, HTTPException
import paho.mqtt.client as mqtt
from typing import List, Dict
from const import *

app = FastAPI()

# MQTT and IoT Hub setup
mqtt_data = []  # List to store received MQTT messages
broker_address = "localhost"
port = 1883
last_sent_time = 0
iot_hub_client = None  # Initialize this globally

# Initialize IoT Hub Registry Manager
registry_manager = IoTHubRegistryManager(CONNECTION_STRING)

# PostgreSQL connection setup
def get_postgres_connection():
    return psycopg2.connect(database="postgres", user="postgres", password="obaid", host="localhost", port="5432")

def create_or_get_device(device_id):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM devices WHERE device_id = %s;", (device_id,))
        device_info = cursor.fetchone()

        if device_info:
            logging.info("Device already exists in PostgreSQL.")
            return {
                'device_id': device_info[0],
                'primary_key': device_info[1],
                'secondary_key': device_info[2]
            }

        try:
            device = registry_manager.get_device(device_id)
            logging.info("Device already exists in IoT Hub.")
            return {
                'device_id': device.device_id,
                'primary_key': device.authentication.symmetric_key.primary_key,
                'secondary_key': device.authentication.symmetric_key.secondary_key
            }
        except Exception as e:
            logging.error(f"Creating new device in IoT Hub. Error: {str(e)}")
            device = registry_manager.create_device_with_sas(
                device_id=device_id, primary_key=None, secondary_key=None, status="enabled")
            logging.info(f"New Device created. ID: {device.device_id}, Primary Key: {device.authentication.symmetric_key.primary_key}")

            cursor.execute("INSERT INTO devices (device_id, primary_key, secondary_key) VALUES (%s, %s, %s);",
                           (device.device_id, device.authentication.symmetric_key.primary_key, device.authentication.symmetric_key.secondary_key))
            conn.commit()
            return {
                'device_id': device.device_id,
                'primary_key': device.authentication.symmetric_key.primary_key,
                'secondary_key': device.authentication.symmetric_key.secondary_key
            }

    finally:
        cursor.close()
        conn.close()

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

def initialize_iothub_client(connection_string):
    global iot_hub_client
    if iot_hub_client is None:
        iot_hub_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
        iot_hub_client.connect()
    else:
        try:
            iot_hub_client.send_message("ping")
        except Exception:
            logging.warning("Reconnecting IoT Hub client...")
            iot_hub_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
            iot_hub_client.connect()
    return iot_hub_client

def send_data_to_iot_hub(device_id, data):
    device = create_or_get_device(device_id)

    primary_key = device.get('primary_key')
    if not primary_key:
        raise HTTPException(status_code=500, detail="Device primary key not found.")

    connection_string = f"HostName={IOT_HUB_HOST};DeviceId={device_id};SharedAccessKey={primary_key}"
    client = initialize_iothub_client(connection_string)

    payload = json.dumps(data)
    message = Message(payload)
    message.content_type = "application/json"
    message.content_encoding = "utf-8"

    try:
        client.send_message(message)
        logging.info(f"Data sent successfully: {payload}")
    except Exception as e:
        logging.error(f"Failed to send data. Error: {e}")
       #iot_hub_client = None  # Force reinitialization
        client = initialize_iothub_client(connection_string)
        try:
            client.send_message(message)
            logging.info(f"Data resent successfully: {payload}")
        except Exception as retry_error:
            logging.error(f"Retry failed. Error: {retry_error}")
            raise HTTPException(status_code=500, detail=f"Failed to send data to IoT Hub: {retry_error}")

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("test/subscribe")
    else:
        print(f"Failed to connect to MQTT broker with error code {rc}")

def on_message(client, userdata, message):
    global mqtt_data, last_sent_time
    
    try:
        decoded_message = json.loads(message.payload.decode())
        mqtt_data.append(decoded_message)
        print(f"Received message: {decoded_message} on topic: {message.topic}")

        # Check if enough time has passed since the last send
        current_time = int(time.time() * 1000)
        if current_time - last_sent_time >= PAYLOAD_INTERVAL:
            # Fetch data from the API and send to IoT Hub
            data_list = get_data_from_api("http://127.0.0.1:8000/mqtt-data")
            if data_list:
                last_data = data_list[-1]
                device_id = last_data.get("device_id")
                if device_id:
                    send_data_to_iot_hub(device_id, last_data)
                    last_sent_time = current_time  # Update the last sent time
                else:
                    logging.warning("No device_id found in the last data.")
            else:
                logging.warning("No data fetched from the API.")
        else:
            logging.info("Waiting for the next interval to send data.")

    except json.JSONDecodeError:
        print("Failed to decode JSON message")


mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

try:
    mqtt_client.connect(broker_address, port)
    mqtt_client.loop_start()
except Exception as e:
    print(f"Failed to connect to MQTT broker: {e}")
    raise

@app.get("/mqtt-data", response_model=List[Dict])
async def get_mqtt_data():
    if not mqtt_data:
        raise HTTPException(status_code=404, detail="No MQTT data available")
    return mqtt_data

@app.post("/send-to-hub/")
async def send_to_hub(data: Dict):
    try:
        device_id = data.get("device_id")
        if device_id:
            send_data_to_iot_hub(device_id, data)
            return {
                "message": "Data sent to IoT Hub successfully.",
                "data": data
            }
        else:
            raise HTTPException(status_code=400, detail="Device ID not found in data.")
    except Exception as e:
        logging.error(f"Error in send_to_hub: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

#def send_messages():
    global last_sent_time
    current_time = int(time.time() * 1000)
    if current_time - last_sent_time >= PAYLOAD_INTERVAL:
        data_list = get_data_from_api("http://127.0.0.1:8000/mqtt-data")
        if data_list:
            last_data = data_list[-1]  # Get the last data item
            logging.info(f"Data to send: {last_data}")
            device_id = last_data.get("device_id")
            if device_id:
                send_data_to_iot_hub(device_id, last_data)  # Ensure this function is correct
            else:
                logging.warning("No device_id found in the last data.")
        else:
            logging.warning("No data fetched from the API.")
        last_sent_time = current_time
    else:
        logging.info("Waiting for the next interval to send data.")

def get_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        received_data = response.json()
        if received_data:
            logging.info(f"Data received from API: {received_data}")
            return received_data
        else:
            logging.warning("No data received from API.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
