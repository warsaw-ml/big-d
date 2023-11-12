from google.cloud import pubsub_v1

# Replace with your values
PROJECT_ID = 'big-d-project-404815'
SUBSCRIPTION_NAME = 'telegram-sub'

subscriber = pubsub_v1.SubscriberClient()
subscription_path = f'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}'

def callback(message):
    try:
        # Process the message payload (replace with your processing logic)
        print(f"Received message: {message.data}")
        
        # Acknowledge the message to remove it from the subscription
        message.ack()
    except Exception as e:
        # Handle any errors that occur during message processing
        print(f"Error processing message: {e}")
        # You can consider logging the error, retrying, or taking other appropriate actions

# Create a subscriber and subscribe to the specified subscription
streaming_pull_feature = subscriber.subscribe(subscription_path, callback=callback)

# Keep the script running to continuously listen for messages
print("Listening for messages. Press Ctrl+C to exit.")
try:
    streaming_pull_feature.result()
except KeyboardInterrupt:
    print("Script terminated.")
