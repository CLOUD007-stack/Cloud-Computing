import pika
import json

class ModuleCoordinator:
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='assignment_exchange', exchange_type='fanout')
        self.channel.queue_declare(queue='results_queue')
        self.channel.queue_bind(exchange='assignment_exchange', queue='results_queue')
        self.channel.basic_consume(queue='results_queue', on_message_callback=self.coordinator_callback, auto_ack=True)

    def coordinator_callback(self, ch, method, properties, body):
        assignment = json.loads(body.decode('utf-8'))
        print(f"Received message for confirmation: {assignment}")

        if assignment['status'] == 'validated':
            assignment['status'] = 'confirmed'
            print(f"Assignment confirmed: {assignment}")
        else:
            print("Ignoring the assignment as it's not in the 'validated' state.")

    def start_listening(self):
        print('Module Coordinator is waiting for messages........')
        self.channel.start_consuming()

if __name__ == "__main__":
    demonstrator = ModuleCoordinator()
    demonstrator.start_listening()
