# Imagem do processador (consumidor Kafka)
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processador.py .
CMD ["python", "-u", "processador.py"]
