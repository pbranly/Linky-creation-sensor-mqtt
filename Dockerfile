FROM python:3.11-slim

RUN apt-get update && apt-get install -y 

WORKDIR /app

# Copier requirements.txt depuis le dossier app
COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le code de app/
COPY app/ .

CMD ["python", "main.py"]
