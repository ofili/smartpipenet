import argparse
import logging
from abc import ABC
from pathlib import Path
import sys

import apache_beam as beam

from google.cloud import firestore
from google.cloud.firestore_v1 import Client

path_root = Path(__file__).resolve().parent.parent
sys.path.append(str(path_root))

from config.config import PROJECT_ID, FIRESTORE_PROJECT_ID, FIRESTORE_COLLECTION_NAME, JOB_NAME, SUBSCRIPTION_ID
from pipelines.options import pipeline_options

project_id = PROJECT_ID
subscription_id = SUBSCRIPTION_ID
firestore_project_id = FIRESTORE_PROJECT_ID
collection_name = FIRESTORE_COLLECTION_NAME
batch_size = 500


class DropOutliers(beam.DoFn):
    def process(self, element):
        if element['ambient_temperature'] < 20 or element['ambient_temperature'] > 30:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: ambient temperature = {element['ambient_temperature']}")
            return None
        if element['exhaust_temperature'] < 150 or element['exhaust_temperature'] > 300:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['exhaust_temperature']}")
            return None
        if element['inlet_pressure'] < 0.5 or element['inlet_pressure'] > 15:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['inlet_pressure']}")
            return None
        if element['outlet_pressure'] < 0.5 or element['outlet_pressure'] > 1.5:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['outlet_pressure']}")
            return None
        if element['coolant_flow_rate'] < 0.5 or element['coolant_flow_rate'] > 15:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['coolant_flow_rate']}")
            return None
        if element['exhaust_flow_rate'] < 5 or element['exhaust_flow_rate'] > 10:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['exhaust_flow_rate']}")
            return None
        if element['fuel_flow_rate'] < 0.1 or element['fuel_flow_rate'] > 0.5:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['fuel_flow_rate']}")
            return None
        if element['energy_output'] < 1000 or element['energy_output'] > 5000:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['energy_output']}")
            return None
        if element['carbon_monoxide'] < 0 or element['carbon_monoxide'] > 50:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['carbon_monoxide']}")
            return None
        if element['nitrogen_oxide'] < 0 or element['nitrogen_oxide'] > 100:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['nitrogen_oxide']}")
            return None
        if element['oxygen'] < 5 or element['oxygen'] > 15:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['oxygen']}")
            return None
        if element['carbon_dioxide'] < 5 or element['carbon_dioxide'] > 50:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['carbon_dioxide']}")
            return None
        if element['water_vapor'] < 0 or element['water_vapor'] > 100:
            logging.info(
                f"Removing outlier from sensor {element['sensor_id']} at {element['timestamp']}: exhaust temperature = {element['water_vapor']}")
            return None
        return element['sensor_id'], element['timestamp'], element['ambient_temperature'], element[
            'exhaust_temperature'], element['inlet_pressure'], element['outlet_pressure'], \
            element['coolant_flow_rate'], element['exhaust_flow_rate'], element['fuel_flow_rate'], element[
            'energy_output'], element['efficiency'], element['carbon_dioxide'], \
            element['carbon_monoxide'], element['nitrogen_oxide'], element['oxygen'], element['carbon_dioxide'], \
            element['water_vapor']


def process_sensor_data(data):
    # Parse sensor data
    sensor_id, timestamp, ambient_temperature, exhaust_temperature, inlet_pressure, \
        outlet_pressure, coolant_flow_rate, exhaust_flow_rate, fuel_flow_rate, energy_output, \
        efficiency, carbon_monoxide, nitrogen_oxide, oxygen, carbon_dioxide, water_vapor = data.split(',')

    # Create a document reference in Firestore
    db: Client = firestore.Client(project=firestore_project_id)
    doc_ref = db.collection(collection_name).document()

    # Create a document with the sensor data
    doc_ref.set({
        "sensor_id": sensor_id,
        "timestamp": int(timestamp),
        "ambient_temperature": float(ambient_temperature),
        "exhaust_temperature": float(exhaust_temperature),
        "inlet_pressure": float(inlet_pressure),
        "outlet_pressure": float(outlet_pressure),
        "coolant_flow_rate": float(coolant_flow_rate),
        "exhaust_flow_rate": float(exhaust_flow_rate),
        "fuel_flow_rate": float(fuel_flow_rate),
        "energy_output": float(energy_output),
        "efficiency": float(efficiency),
        "carbon_monoxide": float(carbon_monoxide),
        "nitrogen_oxide": float(nitrogen_oxide),
        "oxygen": float(oxygen),
        "carbon_dioxide": float(carbon_dioxide),
        "water_vapor": float(water_vapor),
    })

    return doc_ref.path


def write_to_firestore(data):
    db = firestore.Client(project=firestore_project_id)
    doc_ref = db.collection(collection_name).document()
    doc_ref.set(data)


def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--mode', dest='mode', choices=["local", "cloud"], default='local',
                        help='Mode to run pipelines in.')
    parser.add_argument('--job_name', dest='job_name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(options=pipeline_options(
            project=PROJECT_ID,
            job_name=known_args.job_name,
            mode=known_args.mode,
    )) as p:
        # Read from Pub/Sub
        # sensor_data = p | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
        #     subscription=f"projects/{project_id}/subscriptions/{subscription_id}")
        sensor_data = p | 'Read data' >> beam.io.ReadFromText('data_ingestion/sensor_data.json')
        #logging.info(sensor_data)
        # Process sensor data
        # processed_data = sensor_data | 'Process Sensor Data' >> beam.Map(process_sensor_data)
        # Drop outliers
        filtered_data = sensor_data | 'Drop Outliers' >> beam.ParDo(DropOutliers())
        # Write to Firestore in batches
        # filtered_data | 'Write to Firestore' >> beam.ParDo(write_to_firestore)
        filtered_data | 'Write to Firestore' >> beam.Map(process_sensor_data)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
