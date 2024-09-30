from abc import ABC, abstractmethod
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import threading
import logging
import time
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class KafkaService(ABC):
    """
    Abstract base class for building Kafka-based microservices.
    """

    def __init__(
        self,
        bootstrap_servers="localhost:9092",
        group_id=None,
        input_topic=None,
        output_topic=None,
        value_serializer=None,
        value_deserializer=None,
        consumer_config=None,
        producer_config=None,
    ):
        """
        Initialize the Kafka microservice with producer and consumer configurations.

        :param bootstrap_servers: List of Kafka bootstrap servers.
        :param group_id: Consumer group ID.
        :param input_topic: Topic to consume messages from.
        :param output_topic: Topic to produce messages to.
        :param value_serializer: Function to serialize message values.
        :param value_deserializer: Function to deserialize message values.
        :param consumer_config: Additional consumer configurations.
        :param producer_config: Additional producer configurations.
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.value_serializer = value_serializer or (
            lambda x: json.dumps(x).encode("utf-8")
        )
        self.value_deserializer = value_deserializer or (
            lambda x: json.loads(x.decode("utf-8"))
        )
        self.consumer_config = consumer_config or {}
        self.producer_config = producer_config or {}
        self._stop_event = threading.Event()

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.value_serializer,
            retries=5,
            **self.producer_config,
        )

        # Initialize Kafka consumer if input_topic is provided
        if self.input_topic:
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=self.value_deserializer,
                **self.consumer_config,
            )
        else:
            self.consumer = None

    def start(self):
        """
        Start the microservice. If a consumer is configured, start the message processing loop.
        """
        logger.info(f"Starting microservice: {self.__class__.__name__}")
        self.setup()

        if self.consumer:
            self.consumer_thread = threading.Thread(
                target=self._consume_messages, daemon=True
            )
            self.consumer_thread.start()

    def stop(self):
        """
        Stop the microservice gracefully.
        """
        logger.info(f"Stopping microservice: {self.__class__.__name__}")
        self._stop_event.set()
        if self.consumer:
            self.consumer.close()
            self.consumer_thread.join()
        self.teardown()
        self.producer.flush()
        self.producer.close()

    def _consume_messages(self):
        """
        Internal method to consume messages from Kafka and process them.
        """
        logger.info("Consumer thread started.")
        try:
            for message in self.consumer:
                if self._stop_event.is_set():
                    break
                logger.info(f"Received message: {message.value}")
                try:
                    self.handle_message(message.value)
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        logger.info("Consumer thread stopped.")

    def send_message(self, message):
        """
        Send a message to the output topic.

        :param message: The message to send.
        """
        if not self.output_topic:
            raise ValueError("Output topic is not set.")
        future = self.producer.send(self.output_topic, message)
        try:
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}"
            )
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")

    @abstractmethod
    def handle_message(self, message):
        """
        Abstract method to process incoming messages. Must be implemented by subclasses.

        :param message: The message received from Kafka.
        """
        pass

    def setup(self):
        """
        Optional method to set up resources before the microservice starts processing messages.
        """
        pass

    def teardown(self):
        """
        Optional method to clean up resources after the microservice stops processing messages.
        """
        pass

    def run(self):
        """
        Run the microservice and keep it alive until interrupted.
        """
        self.start()
        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Shutting down...")
            self.stop()
