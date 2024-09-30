from kafka_service import KafkaService
import logging
import time

logger = logging.getLogger(__name__)


class NotificationService(KafkaService):
    def handle_message(self, message):
        logger.info(f"Sending notification: {message}")
        # Simulate notification sending logic
        notification_id = message.get("notification_id")
        recipient = message.get("recipient")
        content = message.get("content")

        # Here you could integrate with an email/SMS service
        # For demonstration, we just log the notification
        logger.info(f"Notification {notification_id} sent to {recipient}: {content}")
