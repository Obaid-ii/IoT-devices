from azure.iot.hub import IoTHubRegistryManager

# Connection string for IoT Hub
connection_string = "HostName=iothubdevuae.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=TgNmv49DIduLOsnHU7ccaESSOcXnpKu9UAIoTOMlm0s="

# Device ID you want to register
device_id = "obaid_device"  # You can use the ESP8266 MAC address here

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

if __name__ == "__main__":
    create_device()
