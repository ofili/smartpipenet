import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage

class ConvertToPubSubMessage(beam.DoFn):
    """A DoFn for converting into PubSub message format."""
    def process(self, element, *args, **kwargs):
        yield PubsubMessage(
            data=element["text"].encode("utf-8"), attributes={"id": element["id"]}
        )
