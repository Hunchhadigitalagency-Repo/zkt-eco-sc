from zk import ZK, const
import time
import requests
import logging
import json
from collections import defaultdict
from datetime import datetime

# Logging configuration
LOG_FILE = "attendance_logs.log"
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Device connection details
BASE_URL = "https://hunchha.hajirkhata.com"
GET_DEVICE_DATA = f"{BASE_URL}/api/device/get-devices/all/"
SERVER_ENDPOINT_TO_SEND_ATTENDANCE = f"{BASE_URL}/api/device/post-device-data"

def get_device_data():
    """
    Fetches device data such as IP, port, username, and password for multiple devices.
    Returns a list of dictionaries, each containing device details.
    """
    try:
        logging.info("Fetching device data...")
        response = requests.get(BASE_URL)
        response.raise_for_status()
        
        # Assuming the API returns a JSON response with an array of device details
        devices = response.json()
        if not devices:
            raise ValueError("No device data found.")
        
        # Parse and format the device data
        device_list = []
        for device in devices:
            device_info = {
                "ip": device.get("device_ip"),
                "port": device.get("port", 4370),  # Default port if not specified
                "username": device.get("device_user_name", "admin"),  # Default username
                "password": device.get("device_password", 0),  # Default password
            }
            device_list.append(device_info)
        
        logging.info(f"Retrieved data for {len(device_list)} device(s).")
        return device_list

    except Exception as e:
        logging.error(f"Failed to fetch device data: {e}")
        return []

def clear_and_store_attendance_data(attendance_data, filename="attendance_records.json"):
    """
    Clears old data and stores the new attendance data in a JSON file.
    """
    try:
        # Clear old data by opening the file in write mode
        with open(filename, "w") as json_file:
            # Store the new attendance data as a list of records
            json.dump(attendance_data, json_file, indent=4)
        logging.info(f"Attendance data successfully stored in {filename}.")
    except Exception as e:
        logging.error(f"Failed to store attendance data: {e}")


def formatted_attendance_data(attendance_data):
    """
    Formats the attendance data into the structure:
    {
        date: {
            user_id: [
                first attendance data of the day,
                last attendance data of the day (if different from the first)
            ]
        }
    }
    """
    # Initialize an empty dictionary to store the formatted data
    formatted_data = defaultdict(lambda: defaultdict(list))

    # Loop through each record in the attendance data
    for record in attendance_data:
        # Extract the date part from the timestamp
        date_str = record["time"].split(" ")[0]  # YYYY-MM-DD

        # Add the record to the date and user_id group
        formatted_data[date_str][record["user_id"]].append(record)

    # Now filter to keep only the first and last record for each user
    final_data = defaultdict(lambda: defaultdict(list))
    for date, users in formatted_data.items():
        for user_id, records in users.items():
            # Sort records by timestamp
            sorted_records = sorted(records, key=lambda x: x["time"])

            # Keep only the first and last record of the day, but avoid duplicates
            if sorted_records:
                # If only one record, add it only once
                final_data[date][user_id].append(sorted_records[0])  # First record
                if len(sorted_records) > 1 and sorted_records[0] != sorted_records[-1]:
                    final_data[date][user_id].append(sorted_records[-1])  # Last record

    # Prepare the final formatted data structure
    formatted_result = {}

    # Convert final_data to the desired format: a dictionary where date is the key
    for date, users in final_data.items():
        date_data = {}
        for user_id, records in users.items():
            # Ensure the user ID's records are formatted correctly
            date_data[user_id] = [
                {
                    "user_id": user_id,
                    "user_name": records[0]["user_name"],  # Assuming all records for the user have the same name
                    "time": record["time"],
                    "status": record["status"]
                } for record in records
            ]
        formatted_result[date] = date_data

    return formatted_result


def fetch_attendance_data(ip, port, password):
    """
    Connects to the device and fetches attendance data.
    """
    zk = ZK(ip, port=port, timeout=5, password=password, force_udp=False, ommit_ping=False)
    conn = None
    try:
        logging.info(f"Connecting to the device at {ip}:{port}...")
        conn = zk.connect()
        conn.disable_device()  # Prevents new logins during data retrieval

        logging.info(f"Fetching attendance data from device at {ip}:{port}...")
        attendance = conn.get_attendance()

        users = conn.get_users()
        user_map = {user.user_id: user.name for user in users}
        
        if attendance:
            logging.info(f"Attendance records fetched successfully from {ip}:{port}.")
            
            # Prepare the attendance data as a list of dictionaries
            attendance_data = []
            for record in attendance:
                # Get the user name from the user_map using the user_id
                user_name = user_map.get(record.user_id, "Unknown User")
                
                # Convert datetime to string format
                timestamp_str = record.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                # Append the record along with the user name and formatted timestamp
                attendance_data.append({
                    "user_id": record.user_id,
                    "user_name": user_name,  # Include the user's name
                    "time": timestamp_str,  # Convert datetime to string
                    "status": record.status
                })

            # format the attendance_data into defined structure
            formatted_data  = formatted_attendance_data(attendance_data)

            # Store the attendance data in a JSON file
            clear_and_store_attendance_data(formatted_data)

            return formatted_data
            
        else:
            logging.info(f"No attendance data found for device at {ip}:{port}.")

    except Exception as e:
        logging.error(f"Unable to fetch data from device at {ip}:{port}: {e}")
    finally:
        if conn:
            conn.enable_device()  # Re-enables the device after data retrieval
            conn.disconnect()
        logging.info(f"Disconnected from the device at {ip}:{port}.")

def sendDataToServer(organization_id,data):
    try:
        payload = {
            "organization_id": organization_id,
            "data": data
        }
        response = requests.post("https://hunchha.hajirkhata.com/api/device/post-device-data", json=payload)
        response.raise_for_status()
        logging.info("Attendance data sent to the server successfully.")
        try:
            response_data = response.json()
            logging.info(f"Response from the server: {response_data}")
        except json.JSONDecodeError:
            logging.error("Failed to parse the response from the server.")
    except Exception as e:
        logging.error(f"Failed to send data to the server: {e}")

def sendLogFileDataToserver(ip_address):
    server_endpoint = "https://hunchha.hajirkhata.com/api/log/log-entries/"
    log_file_path = 'script.log'
    # Read the log file
    try:
        with open(log_file_path, 'r') as file:
            log_data = file.read()
    except FileNotFoundError:
        logging.error("Log file not found")
        return
    except IOError as e:
        logging.error(f"Error reading log file: {str(e)}")
        return
    
    # Prepare data to send
    payload = {
        'log_text': log_data,
        'device_ip': ip_address
    }
    
    # Send the log file data to the server
    try:
        response = requests.post(server_endpoint, json=payload)
        response.raise_for_status()
        logging.info("Log file data sent to server successfully")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending log file data to server: {str(e)}")
    
    # Clear the log file
    try:
        open(log_file_path, 'w').close()
        logging.info("Log file cleared successfully")
    except IOError as e:
        logging.error(f"Error clearing log file: {str(e)}")

if __name__ == "__main__":
    # Fetch data for all devices
    logging.info("Starting attendance fetch script.")
    device_list = get_device_data()
    if device_list:
        while True:
            for device in device_list:
                try:
                    logging.info(f"Processing device at {device['ip']}...")
                    fetched_data = fetch_attendance_data(
                        ip=device["ip"],
                        port=device["port"],
                        password=device["password"],
                    )

                    if fetched_data:
                        logging.info(f"Attendance records fetched successfully from {device['ip']}.")
                        sendDataToServer(device["organization"],fetched_data)
                        sendLogFileDataToserver(device["ip"])

                except Exception as e:
                    logging.error(f"Failed to fetch attendance data for device at {device['ip']}: {e}")
            logging.info("Waiting for 60 seconds before the next fetch for all devices...")
            time.sleep(60)  # Wait for 60 seconds before fetching data again
    else:
        logging.error("No devices found. Terminating script.")
