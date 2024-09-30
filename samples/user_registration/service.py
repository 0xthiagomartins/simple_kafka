from kafka_service import KafkaService
import logging
import time

logger = logging.getLogger(__name__)


class UserRegistrationService(KafkaService):
    def handle_message(self, message):
        logger.info(f"Registering user: {message}")
        # Simulate user registration logic
        user_id = message.get("user_id")
        email = message.get("email")

        # Create a registration confirmation
        confirmation = {
            "user_id": user_id,
            "email": email,
            "status": "registered",
            "registered_at": time.time(),
        }

        # Send the confirmation
        self.send_message(confirmation)
        logger.info(f"User {user_id} registered and confirmation sent.")
