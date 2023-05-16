import argparse
import logging
import sys
from pathlib import Path

import apache_beam as beam
from google.cloud import firestore
from google.cloud import bigquery

path_root = Path(__file__).resolve().parent.parent
sys.path.append(str(path_root))

from config.config import DATASET_ID, PROJECT_ID, SUBSCRIPTION_ID, FIRESTORE_PROJECT_ID, FIRESTORE_COLLECTION_NAME, \
    TABLE_ID
from pipelines.options import pipeline_options

project_id = PROJECT_ID
subscription_id = SUBSCRIPTION_ID
firestore_project_id = FIRESTORE_PROJECT_ID
collection_name = FIRESTORE_COLLECTION_NAME
dataset_id = DATASET_ID
table_id = TABLE_ID

bq_schema = 'sensor_id:STRING, timestamp:TIMESTAMP, ambient_temperature:FLOAT,exhaust_temperature:FLOAT,inlet_pressure:FLOAT,outlet_pressure:FLOAT,coolant_flow_rate:FLOAT,exhaust_flow_rate:FLOAT,fuel_flow_rate:FLOAT,energy_output:FLOAT,efficiency:FLOAT,carbon_monoxide:FLOAT,nitrogen_oxide:FLOAT,oxygen:FLOAT,carbon_dioxide:FLOAT,water_vapor:FLOAT'


def process_data(doc):
    sensor_id: doc['sensor_id']
    timestamp: doc['timestamp']
    ambient_temperature = doc['ambient_temperature']
    exhaust_temperature: doc['exhaust_temperature']
    inlet_pressure: doc['inlet_pressure']
    outlet_pressure: doc['outlet_pressure']
    coolant_flow_rate: doc['coolant_flow_rate']
    exhaust_flow_rate: doc['exhaust_flow_rate']
    fuel_flow_rate: doc['fuel_flow_rate']
    energy_output: doc['energy_output']
    efficiency: doc['efficiency']
    carbon_monoxide: doc['carbon_monoxide']
    nitrogen_oxide: doc['nitrogen_oxide']
    oxygen: doc['oxygen']
    carbon_dioxide: doc['carbon_dioxide']
    water_vapor: doc['water_vapor']

    # Remove outliers
    if ambient_temperature < 20 or ambient_temperature > 30:
        logging.info(
            f"Removing outlier from sensor {sensor_id} at {timestamp}: ambient temperature = {ambient_temperature}")
        return None
    if exhaust_temperature < 150 or exhaust_flow_rate > 300:
        logging.info(
            f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {exhaust_temperature}")
        return None
    if inlet_pressure < 0.5 or inlet_pressure > 15:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {inlet_pressure}")
        return None
    if outlet_pressure < 0.5 or outlet_pressure > 1.5:
        logging.info(
            f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {outlet_pressure}")
        return None
    if coolant_flow_rate < 0.5 or coolant_flow_rate > 15:
        logging.info(
            f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {coolant_flow_rate}")
        return None
    if exhaust_flow_rate < 5 or exhaust_flow_rate > 10:
        logging.info(
            f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {exhaust_flow_rate}")
        return None
    if fuel_flow_rate < 0.1 or fuel_flow_rate > 0.5:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {fuel_flow_rate}")
        return None
    if energy_output < 1000 or energy_output > 5000:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {energy_output}")
        return None
    if carbon_monoxide < 0 or carbon_monoxide > 50:
        logging.info(
            f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {carbon_monoxide}")
        return None
    if nitrogen_oxide < 0 or nitrogen_oxide > 100:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {nitrogen_oxide}")
        return None
    if oxygen < 5 or oxygen > 15:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {oxygen}")
        return None
    if carbon_dioxide < 5 or carbon_dioxide > 50:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {carbon_dioxide}")
        return None
    if water_vapor < 0 or water_vapor > 100:
        logging.info(f"Removing outlier from sensor {sensor_id} at {timestamp}: exhaust temperature = {water_vapor}")
        return None

    # Return a tuple representing the row to be written to BigQuery
    return sensor_id, timestamp, ambient_temperature, exhaust_temperature, inlet_pressure, outlet_pressure, \
        coolant_flow_rate, exhaust_flow_rate, fuel_flow_rate, energy_output, efficiency, carbon_dioxide, \
        carbon_monoxide, nitrogen_oxide, oxygen, carbon_dioxide, water_vapor


def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--mode', dest='mode', choices=["local", "cloud"], help='Mode to run the pipelines in.')
    parser.add_argument('--job_name', dest='job_name', help='Name of the job.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(
            options=pipeline_options(project=PROJECT_ID, job_name=known_args.job_name, mode=known_args.mode)) as p:
        db = firestore.Client(project=firestore_project_id)
        collection_ref = db.collection(collection_name)

        # Read data
        sensor_data = (
                p
                | 'Read sensor data from Firestore' >> beam.Create(collection_ref.get())
                | 'Extract data' >> beam.Map(lambda doc: doc.to_dict())
        )

        # Process data
        processed_data = sensor_data | 'Process sensor data' >> beam.Map(process_data)

        # Write to BigQuery
        bq_data = processed_data | 'Convert to BigQuery Row' >> beam.Map(lambda data: bigquery.Row(**data))
        bq_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            project=project_id,
            dataset=dataset_id,
            table=table_id,
            schema=bq_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
