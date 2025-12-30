import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient

load_dotenv(override=True)


def test_connection():
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
        'sasl.username':     os.getenv('CONFLUENT_API_KEY'),
        'sasl.password':     os.getenv('CONFLEUNT_API_SECRET'),

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    admin = AdminClient(config)
    metadata = admin.list_topics(timeout=10)

    print("\n\n----- Connected to Confluent Cloud -----\n\n")
    print(f"Cluster ID: {metadata.cluster_id}")
    print(f"Topics: {', '.join(metadata.topics.keys())}")


if __name__ == "__main__":
    test_connection()
