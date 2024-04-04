import pika
import json
import time

def establish_rabbitmq_connection():
    attempts = 0
    max_attempts = 5

    while attempts < max_attempts:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', 5672, '/', pika.PlainCredentials("guest", "guest")))
            return conn
        except pika.exceptions.AMQPConnectionError:
            print(f"Failed to connect to RabbitMQ. Retrying... ({attempts}/{max_attempts})")
            attempts += 1
            time.sleep(2)
    print("Exceeded maximum connection attempts. Exiting.")
    exit(1)

def main():
    conn = establish_rabbitmq_connection()
    ch = conn.channel()
    ch.exchange_declare(exchange='assignment_exchange', exchange_type='direct')

    students = [
        {'StudentID': 23200858, 'Solution': 'Task 3 - Cloud Computing', 'Module': 'cloud_computing', 'status': 'submitted'},
        {'StudentID': 12345678, 'Solution': 'Task 3 - Data Mining', 'Module': 'data_mining', 'status': 'submitted'}
    ]
    for student in students:
        message = json.dumps(student)
        print(f"{message}")
        routing_key = 'demonstrator_dm' if student['Module'] == 'data_mining' else 'demonstrator_cc'
        ch.basic_publish(exchange='assignment_exchange', routing_key=routing_key, body=message)
        print(f"{routing_key}")
        print(f"Student {student['StudentID']} sent {message}")
    conn.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        exit(0)
