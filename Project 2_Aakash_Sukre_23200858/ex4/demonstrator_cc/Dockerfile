FROM python:3.8
FROM python:slim-buster
WORKDIR /app
RUN pip install pika
COPY demonstrator_cc.py .
CMD ["python", "demonstrator_cc.py"]
ENV PYTHONUNBUFFERED=1
