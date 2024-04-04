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
    result = ch.queue_declare(queue='validation_queue')
    q_name = result.method.queue
    ch.queue_bind(exchange='assignment_exchange', queue=q_name, routing_key='validation')

    def callback(_, __, ___, body):
        message = json.loads(body)
        print(f"Teaching Assistant received {message}")
        message['status'] = 'validated'
        ch.basic_publish(exchange='assignment_exchange', routing_key='confirmation', body=json.dumps(message))
        print(f"Teaching Assistant Validated {json.dumps(message)}")

    ch.basic_consume(queue="validation_queue", on_message_callback=callback, auto_ack=True)
    print('Teaching Assistant is waiting for assignments')
    ch.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        exit(0)
