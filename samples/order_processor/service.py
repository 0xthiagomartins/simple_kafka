from src.kafka_service import KafkaService
import logging
import time

logger = logging.getLogger(__name__)


class OrderProcessorService(KafkaService):
    def __init__(self, **kwargs):
        """
        Initialize the OrderProcessorService with necessary configurations.
        """
        super().__init__(**kwargs)

    def setup(self):
        """
        Optional setup before the service starts processing messages.
        """
        logger.info("OrderProcessorService setup complete.")

    def teardown(self):
        """
        Optional teardown after the service stops processing messages.
        """
        logger.info("OrderProcessorService teardown complete.")

    def handle_message(self, message):
        """
        Process an incoming order and send a confirmation.

        :param message: The order details as a dictionary.
        """
        logger.info(f"Processing order: {message}")
        # Simulate order processing logic
        order_id = message.get("order_id")
        customer = message.get("customer")
        items = message.get("items", [])
        total = sum(item.get("price", 0) for item in items)

        # Create a confirmation message
        confirmation = {
            "order_id": order_id,
            "customer": customer,
            "status": "processed",
            "total": total,
            "processed_at": time.time(),
        }

        # Send the confirmation message
        self.send_message(confirmation)
        logger.info(f"Order {order_id} processed and confirmation sent.")
