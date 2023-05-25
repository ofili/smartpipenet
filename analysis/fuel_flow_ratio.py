from __future__ import absolute_import

import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
import pandas as pd
import pyarrow as pa
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue, FixedWindows


def calculate_fuel_flow_ratio(element):
    sensor_id, data = element
    oxygen = data['oxygen']
    nitrogen_oxide = data['nitrogen_oxide']
    fuel_flow_rate = data['fuel_flow_rate']

    return {
        'sensor_id': sensor_id,
        'fuel_flow_ratio': (oxygen + nitrogen_oxide) / fuel_flow_rate
    }


def is_valid_date(date: str):
    parsed = datetime.strptime(date, '%Y-%m-%d')
    return datetime(2023, 1, 1) <= parsed <= datetime(2023, 12, 31)


def run_power_plant_efficiency_pipeline(
        pipeline, start_date=None, end_date=None, output=None):
    query = f""""
    SELECT
        sensor_id,
        timestamp AS date,
        oxygen,
        nitrogen_oxide,
        fuel_flow_rate
    FROM `project.dataset.table`
    WHERE
        timestamp >= '{start_date}' AND timestamp <= '{end_date}'
    """

    # The pipeline will be run on exiting the `with` block
    with pipeline as p:
        table = (
                p
                # | 'Read table' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
                | 'Read' >> beam.io.WriteToText('sensor_data.json')
                | 'Parse' >> beam.Map(lambda data: json.loads(data))
                | 'Print' >> beam.Map(print)
                | 'Assign timestamp' >> beam.Map(lambda x: TimestampedValue(x, x['date'].timestamp()))
                # Use beam.Select to make sure data has a schema
                | 'Set schema' >> beam.Select(
                    date=lambda x: str(x['date']),
                    oxygen=lambda x: float(x['oxygen']),
                    nitrogen_oxide=lambda x: float(x['nitrogen_oxide']),
                    fuel_flow_rate=lambda x: float(x['fuel_flow_rate'])
                )
        )

        daily_reading = table | 'Daily windows' >> beam.WindowInto(
            FixedWindows(60 * 60 * 24)
        )

        # Group data by sensor ID
        grouped_data = daily_reading | 'Group by sensor ID' >> beam.GroupByKey()

        # Calculate fuel flow ratio for each sensor ID
        fuel_flow_ratios = grouped_data | 'Calculate fuel flow ratio' >> beam.Map(calculate_fuel_flow_ratio)

        # Convert the result to a PCollection of dictionaries
        results = fuel_flow_ratios | 'Convert to PCollection' >> beam.Map(lambda x: pa.RecordBatch.from_pandas(pd.DataFrame([x])))
        # df = DeferredFrame(df)

        # Write the result to Parquet file
        results | 'Write out' >> beam.io.WriteToParquet(
            output, schema=pa.schema([
                pa.field('sensor_id', pa.string()),
                pa.field('some_string', pa.float64())
            ])
        )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', dest='start_date', type=is_valid_date,
                        help='YYYY-MM_DD lower bound (inclusive) for input dataset.')
    parser.add_argument('--end_date', dest='end_date', type=is_valid_date,
                        help='YYYY-MM-DD upper bound (inclusive) for input dataset.')
    parser.add_argument('--output', dest='output', required=True, help='Location to write the output.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    if not is_valid_date(known_args.start_date):
        logging.error("Invalid start date. Valid range is 2023-01-01 to 2023-12-31.")
        return
    if not is_valid_date(known_args.end_date):
        logging.error("Invalid end date. Valid range is 2023-01-01 to 2023-12-31.")
        return

    run_power_plant_efficiency_pipeline(
        beam.Pipeline(options=PipelineOptions(pipeline_args)),
        start_date=known_args.start_date,
        end_date=known_args.end_date,
        output=known_args.output
    )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
