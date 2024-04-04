FROM python:3.8
FROM python:slim-buster
WORKDIR /app
RUN pip install pika
COPY module_coordinator.py .
CMD ["python", "module_coordinator.py"]
ENV PYTHONUNBUFFERED=1