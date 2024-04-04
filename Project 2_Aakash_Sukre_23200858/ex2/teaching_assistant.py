import pika
import json

class TeachingAssistant:
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='assignment_exchange', exchange_type='fanout')
        self.channel.queue_declare(queue='validation_queue')
        self.channel.queue_bind(exchange='assignment_exchange', queue='validation_queue')
        self.channel.basic_consume(queue='validation_queue', on_message_callback=self.ta_callback, auto_ack=True)

    def ta_callback(self, ch, method, properties, body):
        assignment = json.loads(body.decode('utf-8'))
        print(f"Received message for validation: {assignment}")

        if assignment['status'] == 'corrected':
            assignment['status'] = 'validated'
            self.channel.basic_publish(
                exchange='assignment_exchange',
                routing_key='validation',
                body=json.dumps(assignment)
            )
            print("Assignment validated")
        else:
            print("Ignoring the assignment as it's not in the 'corrected' state.")

    def start_listening(self):
        print('Teaching Assistant is waiting for messages......')
        self.channel.start_consuming()

if __name__ == "__main__":
    demonstrator = TeachingAssistant()
    demonstrator.start_listening()