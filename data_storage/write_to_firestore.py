import argparse
import logging

import apache_beam as beam

from google.cloud import firestore

from config.config import PROJECT_ID, FIRESTORE_PROJECT_ID, FIRESTORE_COLLECTION_NAME, JOB_NAME, SUBSCRIPTION_ID
from pipelines.options import pipeline_options

project_id = PROJECT_ID
subscription_id = SUBSCRIPTION_ID
firestore_project_id = FIRESTORE_PROJECT_ID
collection_name = FIRESTORE_COLLECTION_NAME
batch_size = 500


def process_sensor_data(data):
    # Parse sensor data
    sensor_id, timestamp, ambient_temperature, exhaust_temperature, inlet_pressure, \
        outlet_pressure, coolant_flow_rate, exhaust_flow_rate, fuel_flow_rate, energy_output, \
        efficiency, carbon_monoxide, nitrogen_oxide, oxygen, carbon_dioxide, water_vapor = data.split(',')

    # Create a document reference in Firestore
    db = firestore.Client(project=firestore_project_id)
    doc_ref = db.collection(collection_name).document()

    # Create a document with the sensor data
    doc_ref.set({
        "sensor_id": sensor_id,
        "timestamp": timestamp,
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

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(options=pipeline_options(
            project=PROJECT_ID,
            job_name=JOB_NAME,
            mode=known_args.mode,
    )) as p:
        # Read from Pub/Sub
        sensor_data = p | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
            subscription=f'/projects/{project_id}/subscriptions/{subscription_id}')
        # Process sensor data
        processed_data = sensor_data | 'Process Sensor Data' >> beam.Map(process_sensor_data)
        # Write to Firestore in batches
        processed_data | 'Write to Firestore' >> beam.ParDo(write_to_firestore)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
