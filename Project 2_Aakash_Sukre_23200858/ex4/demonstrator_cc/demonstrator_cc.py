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
    q_name = ch.queue_declare(queue='correction_queue_cc').method.queue
    ch.queue_bind(exchange='assignment_exchange', queue=q_name, routing_key='demonstrator_cc')

    def callback(ch, method, properties, body):
        msg = json.loads(body)
        print(f"Cloud Computing Demonstrator received {msg}")
        msg['status'] = 'corrected'
        ch.basic_publish(exchange='assignment_exchange', routing_key='validation', body=json.dumps(msg))
        print(f"Cloud Computing Demonstrator corrected {json.dumps(msg)}")

    ch.basic_consume(queue='correction_queue_cc', on_message_callback=callback, auto_ack=True)
    print('Cloud Computing Demonstrator is waiting for assignments')
    ch.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        exit(0)
