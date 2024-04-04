import pika, json, sys, os
from time import sleep

def main():
    creds, conn = pika.PlainCredentials("guest", "guest"), pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
