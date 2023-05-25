import logging
import json

from google.cloud import firestore
from google.cloud.firestore_v1 import DocumentSnapshot, WriteBatch, DocumentReference


def compute_fields(sensor_data):
    # Calculate efficiency
    efficiency = sensor_data['energy_output'] / sensor_data['fuel_flow_rate']
    sensor_data['efficiency'] = efficiency

    # Calculate combustion efficiency
    combustion_efficiency = (
        sensor_data['carbon_dioxide'] + sensor_data['water_vapor']
        ) / (
            sensor_data['carbon_dioxide'] 
            + sensor_data['water_vapor'] 
            + sensor_data['carbon_monoxide']
        )
    sensor_data['combustion_efficiency'] = combustion_efficiency

    return sensor_data


def write_sensor_data(event, context):
    # Check if event and context exist
    if not event or not isinstance(event, dict):
        logging.error("Invalid event data. Event must be a non-empty dictionary.")
        return 'Failure'
    if not context or not isinstance(context, dict):
        logging.error("Invalid context data. Context must be a non-empty dictionary.")
        return 'Failure'
    
    # Extract Pub/Sub message data
    message: bytes = event.get('data', '')
    if not message:
        logging.error("Empty message data. Unable to decode message.")
        return 'Failure'
    sensor_data = message.decode('utf-8')

    # Parse data as JSON
    try:
        sensor_data_json = json.loads(sensor_data)
    except json.JSONDecodeError as e:
        logging.error(f"Unable to parse sensor data JSON: {e}")
        return 'Failure'

    # Check if message has already been processed
    message_id = context.get('event_id')
    if not message_id:
        logging.error("Missing event ID. Unable to determine message processing status.")
        return 'Failure'
    if is_message_processed(message_id):
        logging.info(f"Message with ID '{message_id}' has already been processed. Skipping.")
        return
    
    # Validate fields
    required_fields = ['energy_output', 'fuel_flow_rate', 'carbon_dioxide', 'water_vapor', 'carbon_monoxide']
    if not all(field in sensor_data_json for field in required_fields):
        logging.error("Missing required fields in sensor data.")
        return 'Failure'

    # Get computed fields
    sensor_data_computed_fields = compute_fields(sensor_data_json)

    # Write sensor data to Firestore
    try:
        db = firestore.Client()
        batch: WriteBatch = db.batch()

        # Set sensor data document
        sensor_data_ref: DocumentReference = db.collection('smartpipenet-pp-sensor').document()
        batch.set(sensor_data_ref, sensor_data_computed_fields)

        # Mark the message as processed
        processed_message_ref = db.collection('processed_messages').document(message_id)
        batch.set(processed_message_ref, {'processed': True})

        # Commit the batch write operation as a transaction
        batch.commit()
        
        logging.info(f"Sensor data with ID '{message_id}' written to Firestore.")
        return 'Success'
    except Exception as e:
        logging.error(f"An error occurred: str{e}")
        return 'Failure'


def is_message_processed(message_id):
    db = firestore.Client()
    dec_ref = db.collection('processed_messages').document(message_id)
    doc_snapshot: DocumentSnapshot = dec_ref.get()
    return doc_snapshot.exists
