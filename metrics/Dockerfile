FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install redis requests numpy

RUN pip install redis numpy

CMD ["python", "main.py"]

