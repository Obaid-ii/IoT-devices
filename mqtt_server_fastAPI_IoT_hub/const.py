import logging
import time
import uuid
CONNECTION_STRING =             "HostName=iothubdevuae.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=TgNmv49DIduLOsnHU7ccaESSOcXnpKu9UAIoTOMlm0s="
# Generate a unique device_id (you can adjust this as needed)
device_id = str(uuid.uuid4())
#DEVICE_REGISTRATION_ID =        "op.Obaid"
IOT_HUB_HOST =                  "iothubdevuae.azure-devices.net"
PAYLOAD_INTERVAL =              60 * 1000 # seconds
MQTT_PORT =                     8883
TEMP_MAX =                      28
TEMP_MIN =                      25
TEMP_DP =                       2
TokenValidity_Time =            3600 # seconds 
last_sent_time =                int(time.time() * 1000)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
