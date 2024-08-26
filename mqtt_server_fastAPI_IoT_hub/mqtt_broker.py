import paho.mqtt.client as mqtt
import json
import time
from const import *
from datetime import datetime

 # Generate a unique device_id (Once)
device_id = str(uuid.uuid4())

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("Connected to MQTT broker")
        # Subscribe to a topic after connecting
        client.subscribe("test/subscribe")
        # Publish JSON data once connected
        publish_json_data(client)
    else:
        print("Failed to connect to MQTT broker with error code %d" % rc)

def on_publish(client, userdata, mid, reason_code, properties):
    print("Data published successfully!")

def on_disconnect(client, userdata, rc, properties, reason):
    print(f"Disconnected from MQTT broker with error code {rc}")
    print(f"Disconnect reason: {reason}")

def on_message(client, userdata, message):
    # Handle incoming messages
    print(f"Received message: {message.payload.decode()} on topic: {message.topic}")

def publish_json_data(client):
    topic = "test/topic"

    # Get the current timestamp in ISO 8601 format
    timestamp = datetime.now().isoformat()
    data = {
        "device_id": device_id,
        "T": timestamp,
        "temperature": 30,  # Example value
        "humidity": 75,     # Example value
        "status": "ok"
    }
    payload = json.dumps(data)  # Convert the dictionary to a JSON string
    result = client.publish(topic, payload)
    
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print("JSON data sent successfully.")
    else:
        print("Failed to send JSON data.")

broker_address = "localhost"
port = 1883

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect
client.on_message = on_message  # Set the callback for message handling

try:
    client.connect(broker_address, port)
except ConnectionRefusedError as e:
    print("Connection refused:", e)
    exit(1)

# Start the loop to process network traffic
client.loop_start()

# Keep the script running to allow the loop to process messages
try:
    last_sent_time = time.time()
    interval = 10  # Interval in seconds

    while True:
        current_time = time.time()
        if current_time - last_sent_time >= interval:
            publish_json_data(client)
            last_sent_time = current_time
        time.sleep(1)  # Sleep for a short period to avoid busy-waiting

except KeyboardInterrupt:
    print("Disconnected by user")
    client.disconnect()
    client.loop_stop()
