import pika
import json

class Student:
    def __init__(self, student_id, answer):
        self.student_id = student_id
        self.answer = answer

    def submit_assignment(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.exchange_declare(exchange='assignment_exchange', exchange_type='fanout')
            assignment_message = {
                'StudentID': self.student_id,
                'Answer': self.answer,
                'status': 'submitted'
            }

            channel.basic_publish(
                exchange='assignment_exchange',
                routing_key='correction',
                body=json.dumps(assignment_message)
            )
            print("Assignment submitted for correction")
            connection.close()
        except Exception as e:
            print(f"Error submitting assignment: {e}")

if __name__ == "__main__":
    student_id = 23200858
    answer = "Solution"
    student = Student(student_id, answer)
    student.submit_assignment()
