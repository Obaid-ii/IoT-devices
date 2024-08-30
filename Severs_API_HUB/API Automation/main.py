# main.py
import logging
from fastapi import FastAPI, HTTPException
import requests
from db_util import *
from iothub_util import *
import csv

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)

PAYLOAD = {
    "Temperature": 35,
    "Humidity": 80,
    "other_key": "value"
}

CSV_FILE_PATH = "device_data.csv"  # CSV file setup

def create_or_get_device(device_id: str) -> Dict[str, str]:
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM devices WHERE device_id = %s;", (device_id,))
        device_info = cursor.fetchone()

        if device_info:
            logging.info(f"Device {device_id} already exists in PostgreSQL.")
            return {
                'device_id': device_info[0],
                'primary_key': device_info[1],
                'secondary_key': device_info[2]
            }

        try:
            device = registry_manager.get_device(device_id)
            logging.info(f"Device {device_id} already exists in IoT Hub.")
            return {
                'device_id': device.device_id,
                'primary_key': device.authentication.symmetric_key.primary_key,
                'secondary_key': device.authentication.symmetric_key.secondary_key
            }
        except Exception as e:
            logging.error(f"Creating new device in IoT Hub. Error: {str(e)}")
            device = registry_manager.create_device_with_sas(
                device_id=device_id, primary_key=None, secondary_key=None, status="enabled")
            logging.info(f"New Device created in IoT Hub. ID: {device.device_id}, Primary Key: {device.authentication.symmetric_key.primary_key}")

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

def log_device_data_to_csv(device_id: str, latency: float):
    """Logs device_id and latency to a CSV file."""
    try:
        with open(CSV_FILE_PATH, mode='a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([device_id, latency])
        logging.info(f"Logged data to CSV: Device ID = {device_id}, Latency = {latency} ms")
    except Exception as e:
        logging.error(f"Failed to log data to CSV: {e}")

@app.on_event("startup")
async def startup_event():
    logging.info("Application startup: Sending data to IoT Hub.")
    try:
        await send_data_to_hub_endpoint()  # Directly call the function without HTTP request
    except Exception as e:
        logging.error(f"Error during startup data send: {e}")

@app.on_event("shutdown")
def shutdown_event():
    logging.info("Application shutdown: Closing database connection.")
    conn = get_postgres_connection()
    close_postgres_connection(conn)

def send_data_to_iot_hub(device_id: str, data: Dict[str, any]) -> None:
    try:
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

        start_time = time.time()
        client.send_message(message)
        end_time = time.time()
        
        latency = (end_time - start_time) * 1000  # Convert to milliseconds
        logging.info(f"Data sent successfully: {payload}, Latency: {latency} ms")
        # Log the device data to CSV
        log_device_data_to_csv(device_id, latency)

    except Exception as e:
        logging.error(f"Failed to send data. Error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send data to IoT Hub: {e}")

@app.get("/send-data-to-hub/")
async def send_data_to_hub_endpoint():
    try:
        # Example payload to send
        logging.info(f"Payload being sent: {PAYLOAD}")
        for i in range(1, 51):
            device_id = f"obaid_{i}"
            send_data_to_iot_hub(device_id, PAYLOAD)
        return {"message": "Data sent to IoT Hub for all devices successfully."}
    except HTTPException as e:
        logging.error(f"HTTP Exception: {e.detail}")
        raise e
    except Exception as e:
        logging.error(f"Error sending data to IoT Hub: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
