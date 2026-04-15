FROM python:3.9-slim
WORKDIR /app

RUN pip install redis
COPY . .

ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]



