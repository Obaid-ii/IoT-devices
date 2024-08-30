from fastapi import FastAPI, HTTPException
import paho.mqtt.client as mqtt
import json
from typing import List, Dict

app = FastAPI()

mqtt_data = []  # List to store received MQTT messages

broker_address = "localhost"
port = 1883

# MQTT Setup
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe("test/subscribe")  # Subscribing to the topic
    else:
        print(f"Failed to connect to MQTT broker with error code {rc}")

def on_message(client, userdata, message):
    global mqtt_data
    try:
        decoded_message = json.loads(message.payload.decode())  # Decode the JSON message
        mqtt_data.append(decoded_message)  # Append the decoded JSON data to the list
        print(f"Received message: {decoded_message} on topic: {message.topic}")
    except json.JSONDecodeError:
        print("Failed to decode JSON message")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker
try:
    mqtt_client.connect(broker_address, port)
    mqtt_client.loop_start()  # Start MQTT client loop in the background
except Exception as e:
    print(f"Failed to connect to MQTT broker: {e}")
    raise

@app.get("/mqtt-data", response_model=List[Dict])
async def get_mqtt_data():
    """
    API endpoint to fetch all received MQTT data.
    """
    if not mqtt_data:
        raise HTTPException(status_code=404, detail="No MQTT data available")
    return mqtt_data

# To run the server, use the command:
# uvicorn fastapi_server:app --reload
