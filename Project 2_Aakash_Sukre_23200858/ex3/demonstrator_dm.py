import pika, json, sys, os
from time import sleep

def main():
    creds, conn = pika.PlainCredentials("guest", "guest"), pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange='assignment_exchange', exchange_type='direct')
    q_name = ch.queue_declare(queue='correction_queue_dm').method.queue
    ch.queue_bind(exchange='assignment_exchange', queue=q_name, routing_key='demonstrator_dm')

    def callback(ch, method, properties, body):
        message = json.loads(body)
        print(f"Data Mining Demonstrator received {message}")
        message['status'] = 'corrected'
        ch.basic_publish(exchange='assignment_exchange', routing_key='validation', body=json.dumps(message))
        print(f"Data Mining Demonstrator corrected {json.dumps(message)}")

    ch.basic_consume(queue='correction_queue_dm', on_message_callback=callback, auto_ack=True)
    print('Data Mining Demonstrator is waiting for assignments')
    ch.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
