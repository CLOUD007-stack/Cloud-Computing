import pika, json

creds, conn = pika.PlainCredentials("guest", "guest"), pika.BlockingConnection(pika.ConnectionParameters('localhost'))
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
