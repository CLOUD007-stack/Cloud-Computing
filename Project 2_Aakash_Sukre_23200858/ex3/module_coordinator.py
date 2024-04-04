import pika, json, sys, os

def main():
    creds, conn = pika.PlainCredentials("guest", "guest"), pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.exchange_declare(exchange='assignment_exchange', exchange_type='direct')

    q_name = ch.queue_declare(queue='confirmation_queue').method.queue
    ch.queue_bind(exchange='assignment_exchange', queue=q_name, routing_key='confirmation')

    def callback(ch, method, properties, body):
        message = json.loads(body)
        print(f"Module Coordinator received {message}")
        message['status'] = 'confirmed'
        ch.basic_publish(exchange='assignment_exchange', routing_key='test', body=json.dumps(message))
        print(f"Module Coordinator corrected {json.dumps(message)}")

    ch.basic_consume(queue="confirmation_queue", on_message_callback=callback, auto_ack=True)
    print('Module Coordinator is waiting for assignments')
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
