import pika
import json

class Demonstrator:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='assignment_exchange', exchange_type='fanout')
        self.channel.queue_declare(queue='correction_queue')
        self.channel.queue_bind(exchange='assignment_exchange', queue='correction_queue')
        self.channel.basic_consume(queue='correction_queue', on_message_callback=self.demonstrator_callback, auto_ack=True)

    def demonstrator_callback(self, ch, method, properties, body):
        assignment = json.loads(body.decode('utf-8'))
        print(f"Received message for correction: {assignment}")
        assignment['status'] = 'corrected'
        self.channel.basic_publish(
            exchange='assignment_exchange',
            routing_key='correction',
            body=json.dumps(assignment)
        )
        print("Assignment corrected")

    def listen_and_correct(self):
        print('Demonstrator is waiting for messages.......')
        self.channel.start_consuming()

if __name__ == "__main__":
    demonstrator = Demonstrator()
    demonstrator.listen_and_correct()
