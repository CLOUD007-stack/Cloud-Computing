FROM python:3.8
FROM python:slim-buster
WORKDIR /app
RUN pip install pika
COPY student.py .
CMD ["python", "student.py"]
ENV PYTHONUNBUFFERED=1

