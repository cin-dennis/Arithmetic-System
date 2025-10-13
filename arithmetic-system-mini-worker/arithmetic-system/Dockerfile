FROM python:3.13-slim

WORKDIR /app

COPY package ./package
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONPATH="${PYTHONPATH}:/app/package"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

