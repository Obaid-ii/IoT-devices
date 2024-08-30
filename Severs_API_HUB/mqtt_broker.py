import paho.mqtt.client as mqtt
import json
import time
from const import *

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT broker")
        # Subscribe to a topic after connecting
        client.subscribe("test/subscribe")
    else:
        print("Failed to connect to MQTT broker with error code %d" % rc)

def on_message(client, userdata, message):
    try:
        # Parse the incoming message as JSON
        data = json.loads(message.payload.decode())

        # Print the field and related data
        print(f"Received data: {data}")
    except json.JSONDecodeError:
        print("Received message is not valid JSON")

def on_disconnect(client, userdata, rc, properties=None, reason=None):
    print(f"Disconnected from MQTT broker with error code {rc}")
    if reason:
        print(f"Disconnect reason: {reason}")

broker_address = "localhost"
port = 1883

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message  # Set the callback for message handling
client.on_disconnect = on_disconnect

try:
    client.connect(broker_address, port)
except ConnectionRefusedError as e:
    print("Connection refused:", e)
    exit(1)

# Start the loop to process network traffic
client.loop_start()

# Keep the script running to allow the loop to process messages
try:
    while True:
        time.sleep(1)  # Sleep for a short period to avoid busy-waiting

except KeyboardInterrupt:
    print("Disconnected by user")
    client.disconnect()
    client.loop_stop()
