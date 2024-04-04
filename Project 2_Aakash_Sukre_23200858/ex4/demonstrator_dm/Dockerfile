FROM python:3.8
FROM python:slim-buster
WORKDIR /app
RUN pip install pika
COPY demonstrator_dm.py .
CMD ["python", "demonstrator_dm.py"]
ENV PYTHONUNBUFFERED=1