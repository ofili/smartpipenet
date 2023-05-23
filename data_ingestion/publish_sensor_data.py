import json
import logging
from pathlib import Path
import random
import sys
import threading
import time

from google.cloud import pubsub_v1

path_root = Path(__file__).resolve().parent.parent
sys.path.append(str(path_root))

from config.config import PROJECT_ID, TOPIC_NAME


def generate_sensor_data(sensor_id):
    ambient_temperature = round(random.uniform(19, 35), 2)  # degree Celsius
    exhaust_temperature = round(random.uniform(140, 320), 2)  # degree Celsius
    inlet_pressure = round(random.uniform(0.4, 15), 2)  # bar
    outlet_pressure = round(random.uniform(0.4, 1.5), 2)  # bar
    coolant_flow_rate = round(random.uniform(0.4, 15), 2)  # m3/h
    exhaust_flow_rate = round(random.uniform(1, 10), 2)  # m3/h
    fuel_flow_rate = round(random.uniform(0.1, 0.7), 2)  # m3/h
    energy_output = round(random.uniform(1000, 5005), 2)  # kW
    carbon_monoxide = round(random.uniform(0, 55), 2)  # ppm
    nitrogen_oxide = round(random.uniform(0, 110), 2)  # ppm
    oxygen = round(random.uniform(3, 15), 2)  # ppm
    carbon_dioxide = round(random.uniform(0, 55), 2)  # ppm
    water_vapor = round(random.uniform(0, 110), 2)  # percentage
    timestamp = int(time.time())

    data = {
        "sensor_id": f"{sensor_id}",
        "timestamp": timestamp,
        "ambient_temperature": ambient_temperature,
        "exhaust_temperature": exhaust_temperature,
        "inlet_pressure": inlet_pressure,
        "outlet_pressure": outlet_pressure,
        "coolant_flow_rate": coolant_flow_rate,
        "exhaust_flow_rate": exhaust_flow_rate,
        "fuel_flow_rate": fuel_flow_rate,
        "energy_output": energy_output,
        "carbon_monoxide": carbon_monoxide,
        "nitrogen_oxide": nitrogen_oxide,
        "oxygen": oxygen,
        "carbon_dioxide": carbon_dioxide,
        "water_vapor": water_vapor,
    }

    return json.dumps(data)


def generate_sensor_ids(num_plants, sensors_per_plant):
    sensor_ids = []
    for i in range(num_plants):
        for j in range(sensors_per_plant):
            sensor_id = f"plant{i + 1}_sensor{j + 1}"
            sensor_ids.append(sensor_id)
    return sensor_ids


def publish_data(data):
    # Define Pub/Sub topic and create a Pub/Sub publisher client
    project_id = PROJECT_ID
    topic_name = TOPIC_NAME
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    publisher.publish(topic_path, data=data.encode('utf-8'))


def publish_sensor_data(sensor_id):
    """Publish data for a given sensor ID"""
    while True:
        sensor_data = generate_sensor_data(sensor_id)
        publish_data(sensor_data)
        logging.info(f"Published {sensor_data}")
        time.sleep(0.2)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    sensor_ids = generate_sensor_ids(10, 150)

    # Create threads for emitting data from each sensor concurrently
    threads = []
    for sensor_id in sensor_ids:
        thread = threading.Thread(target=publish_sensor_data, args=(sensor_id,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()
