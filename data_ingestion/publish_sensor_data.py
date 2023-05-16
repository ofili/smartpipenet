import json
import logging
import random
import time

from google.cloud import pubsub_v1


def generate_sensor_data(sensor_id):
    ambient_temperature = round(random.uniform(19, 35), 2)  # degree Celsius
    exhaust_temperature = round(random.uniform(140, 320), 2)  # degree Celsius
    inlet_pressure = round(random.uniform(0.4, 15), 2)  # bar
    outlet_pressure = round(random.uniform(0.4, 1.5), 2)  # bar
    coolant_flow_rate = round(random.uniform(0.4, 15), 2)  # m3/h
    exhaust_flow_rate = round(random.uniform(1, 10), 2)  # m3/h
    fuel_flow_rate = round(random.uniform(0.1, 0.7), 2)  # m3/h
    energy_output = round(random.uniform(1000, 5005), 2)  # kW
    efficiency = round(energy_output / fuel_flow_rate, 2)  # percentage
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
        "efficiency": efficiency,
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
    project_id = "my-project"
    topic_name = "smartpipenet-sensor-data"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    publisher.publish(topic_path, data=data.encode('utf-8'))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    while True:
        for id in generate_sensor_ids(2, 10):
            sensor_data = generate_sensor_data(id)
            print(sensor_data)

            # Publish the sensor data to Pub/Sub topic
            publish_data(sensor_data)

        time.sleep(0.2)  # Wait for 0.2 seconds
