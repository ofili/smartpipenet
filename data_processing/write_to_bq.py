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
