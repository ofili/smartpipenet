import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from data_processing.write_to_bq import RemoveOutliersFn


class PipelineUnitTest(unittest.TestCase):
    def test_remove_outliers(self):
        # Input data
        input_data = [
            {'sensor_id': 'sensor1', 'ambient_temperature': 25, 'exhaust_temperature': 200},
            {'sensor_id': 'sensor2', 'ambient_temperature': 35, 'exhaust_temperature': 250},
            {'sensor_id': 'sensor3', 'ambient_temperature': 15, 'exhaust_temperature': 180},
            {'sensor_id': 'sensor4', 'ambient_temperature': 40, 'exhaust_temperature': 300},
        ]

        # Expected output after removing outliers
        expected_output = [
            {'sensor_id': 'sensor1', 'ambient_temperature': 25, 'exhaust_temperature': 200},
            {'sensor_id': 'sensor2', 'ambient_temperature': 15, 'exhaust_temperature': 180},
        ]

        # Create a TestPipeline
        with TestPipeline() as p:
            output_data = (
                p
                | beam.Create(input_data)
                | 'Remove outliers' >> beam.ParDo(RemoveOutliersFn())
            )

            # Assert that the output data matches the expected output
            assert_that(output_data, equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
