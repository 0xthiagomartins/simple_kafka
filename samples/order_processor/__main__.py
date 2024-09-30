from .service import OrderProcessorService
import signal
import sys
import logging

logger = logging.getLogger(__name__)


def main():
    # Configuration
    config = {
        "bootstrap_servers": ["localhost:9092"],
        "group_id": "order-processor-group",
        "input_topic": "orders",
        "output_topic": "order-confirmations",
        # Optional: Add custom consumer or producer configurations
        # 'consumer_config': {...},
        # 'producer_config': {...},
    }

    # Initialize the service
    service = OrderProcessorService(**config)

    # Define signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Signal received, shutting down...")
        service.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the service
    service.run()


if __name__ == "__main__":
    main()
