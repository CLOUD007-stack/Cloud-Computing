import pika, json, sys, os

def main():
    creds, conn = pika.PlainCredentials("guest", "guest"), pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
