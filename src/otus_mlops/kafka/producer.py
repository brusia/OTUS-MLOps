import json
from typing import Dict, Final, NamedTuple, Union
import logging
import argparse

from kafka import KafkaProducer, errors as kafka_errors
from otus_mlops.kafka.parquet_data_loader import ParquetDataLoader


BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    offset: int


def main():
    data_loader = ParquetDataLoader()
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        "-b",
        "--bootstrap_server",
        default="localhost:9092",
        help="Kafka server address:port",
    )
    argparser.add_argument(
        "-t", "--topic", default="transactions", help="Kafka topic to produce to"
    )
    argparser.add_argument(
        "-n",
        default=10,
        type=int,
        help="Number of messages to send",
    )

    args = argparser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=serialize,
        security_protocol='PLAINTEXT',
        request_timeout_ms=30000,
        retries=3,
        api_version=(2, 0, 2),
    )

    try:
        for _ in range(args.n):
            transaction = next(data_loader.get_next_transaction())
            record_md = send_message(transaction, producer, args.topic)
            print(
                f"Msg sent. Topic: {record_md.topic}, partition: {record_md.partition}, offset: {record_md.offset}"
            )
    except kafka_errors.KafkaError as err:
        logging.exception(err)

    producer.flush()
    producer.close()


def send_message(transaction, producer: KafkaProducer, topic: str) -> RecordMetadata:
    future = producer.send(
        topic=topic,
        # key=str(transaction["transaction_id"]),
        value=transaction,
    )

    record_metadata = future.get(timeout=1)
    return RecordMetadata(
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")


if __name__ == "__main__":
    main()
