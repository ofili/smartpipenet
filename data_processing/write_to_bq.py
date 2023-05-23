import argparse
import logging
import sys
from abc import ABC
from pathlib import Path
from typing import Dict, Any

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud.bigquery import SchemaField

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


# Define BigQuery schema
schema: list[SchemaField] = [
    bigquery.SchemaField("sensor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("ambient_temperature", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("exhaust_temperature", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("inlet_pressure", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("outlet_pressure", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("coolant_flow_rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("exhaust_flow_rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("fuel_flow_rate", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("energy_output", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("efficiency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("combustion_efficiency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("carbon_monoxide", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("nitrogen_oxide", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("oxygen", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("carbon_dioxide", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("water_vapor", "FLOAT", mode="NULLABLE"),
    bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="timestamp"
    ),
]


class RemoveOutliersFn(beam.DoFn, ABC):
    def process(self, element, *args, **kwargs):
        min_values = {
            'ambient_temperature': 20,
            'exhaust_temperature': 150,
            'inlet_pressure': 0.5,
            'outlet_pressure': 0.5,
            'coolant_flow_rate': 0.5,
            'exhaust_flow_rate': 5,
            'fuel_flow_rate': 0.1,
            'energy_output': 1000,
            'carbon_monoxide': 0,
            'nitrogen_oxide': 0,
            'oxygen': 5,
            'carbon_dioxide': 5,
            'water_vapor': 0,
        }

        max_values = {
            'ambient_temperature': 30,
            'exhaust_temperature': 300,
            'inlet_pressure': 15,
            'outlet_pressure': 1.5,
            'coolant_flow_rate': 15,
            'exhaust_flow_rate': 10,
            'fuel_flow_rate': 0.5,
            'energy_output': 5000,
            'carbon_monoxide': 50,
            'nitrogen_oxide': 100,
            'oxygen': 15,
            'carbon_dioxide': 50,
            'water_vapor': 100,
        }

        processed_data: dict[Any, Any] = {}
        for field, value in element.items():
            if min_values.get(field, float('-inf')) <= value <= max_values.get(field, float('-inf')):
                processed_data[field] = value
        
        yield processed_data


def process_sensor_data(doc):
    sensor_data = doc.to_dict()
    return [sensor_data]  # Wrap the  sensor data as a list to emit a single element to the DoFn


def write_to_bigquery(data):
    row = bigquery.Row(**data)
    return row


def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--mode', dest='mode', choices=["local", "cloud"], help='Mode to run the pipelines in.')
    parser.add_argument('--job_name', dest='job_name', help='Name of the job.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(
            options=pipeline_options(project=PROJECT_ID, job_name=known_args.job_name, mode=known_args.mode)) as p:
        
        # Read data from Firestore
        db = firestore.Client(project=firestore_project_id)
        collection_ref = db.collection(collection_name)

        # Read data
        sensor_data = (
            p
            | 'Read sensor data from Firestore' >> beam.Create(collection_ref.get())
            | 'Extract data' >> beam.Map(process_sensor_data)
            | 'Remove outliers' >> beam.ParDo(RemoveOutliersFn())
            | 'Wrap data for write' >> beam.Map(lambda data: (data,))
        )
        # Write data to BigQuery
        sensor_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            project=project_id,
            dataset=dataset_id,
            table=table_id,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method='STREAMING_INSERTS',
            additional_bq_parameters={
                'timePartitioning': {
                    'type': 'MONTH',
                    'field': 'timestamp',
                },
                'clustering': {
                    'fields': ['sensor_id'],
                }
            }
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
