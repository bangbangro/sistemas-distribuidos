FROM python:3.9-slim
WORKDIR /app

COPY . .

ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
RUN pip install redis pandas numpy kafka-python

CMD ["python", "main.py"]

