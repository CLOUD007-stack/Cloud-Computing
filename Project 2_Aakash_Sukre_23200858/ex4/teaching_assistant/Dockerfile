FROM python:3.8
FROM python:slim-buster
WORKDIR /app
RUN pip install pika
COPY teaching_assistant.py .
CMD ["python", "teaching_assistant.py"]
ENV PYTHONUNBUFFERED=1