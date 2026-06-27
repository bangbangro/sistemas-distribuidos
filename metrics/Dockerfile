FROM python:3.10

WORKDIR /app

COPY . .

# Install required Python packages
RUN pip install --no-cache-dir redis numpy kafka-python

CMD ["python", "main.py"]

