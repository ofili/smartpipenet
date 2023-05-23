gcloud functions deploy iot-to-firestore \
    --region us-central1 \
    --project smartpipenet \
    --runtime python310 \
    --trigger-topic smartpipenet-data