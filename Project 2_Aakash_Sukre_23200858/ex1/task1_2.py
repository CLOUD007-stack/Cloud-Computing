import pika
import json

# Connect to RabbitMQ Software through this command
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Task 1
# Using Pika's syntax, declare the necessary queues/exchanges and their connections as shown in Figure 1

channel.exchange_declare(exchange='assignment_exchange', exchange_type='direct')

channel.queue_declare(queue='correction_queue')
channel.queue_declare(queue='validation_queue')
channel.queue_declare(queue='results_queue')

channel.queue_bind(exchange='assignment_exchange', queue='correction_queue', routing_key='correction')
channel.queue_bind(exchange='assignment_exchange', queue='validation_queue', routing_key='validation')
channel.queue_bind(exchange='assignment_exchange', queue='results_queue', routing_key='results')



# Task 2
# Using Pika's syntax, declare where a demonstrator should listen/subscribe

def demonstrator_callback(ch, method, properties, body):
    assignment = json.loads(body.decode('utf-8'))
    print(f"Received message for correction: {assignment}")

    assignment['status'] = 'corrected'

    channel.basic_publish(
        exchange='assignment_exchange',
        routing_key='correction',
        body=json.dumps(assignment)
    )
    print("Assignment corrected")

channel.basic_consume(queue='correction_queue', on_message_callback=demonstrator_callback, auto_ack=True)



# Task 3
# Using Pika's syntax, declare where a TA should listen/subscribe

def ta_callback(ch, method, properties, body):
    assignment = json.loads(body.decode('utf-8'))
    print(f"Received message for validation: {assignment}")

    if assignment['status'] == 'corrected':
        assignment['status'] = 'validated'

        channel.basic_publish(
            exchange='assignment_exchange',
            routing_key='validation',
            body=json.dumps(assignment)
        )
        print("Assignment validated")
    else:
        print("Ignoring the assignment as it's not in the corrected state.")

channel.basic_consume(queue='validation_queue', on_message_callback=ta_callback, auto_ack=True)



# Task 4
# Using Pika's syntax, declare where the module coordinator should listen/subscribe

def coordinator_callback(ch, method, properties, body):
    assignment = json.loads(body.decode('utf-8'))
    print(f"Received message for confirmation: {assignment}")

    if assignment['status'] == 'validated':
        assignment['status'] = 'confirmed'

        print(f"Assignment confirmed: {assignment}")

channel.basic_consume(queue='results_queue', on_message_callback=coordinator_callback, auto_ack=True)

print('Waiting for messages')
channel.start_consuming()
