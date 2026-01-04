FROM python:3.9-slim

# Install system dependencies (SASL is often required for Hive connections)
RUN apt-get update && apt-get install -y \
    libsasl2-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy your script
COPY main.py /app/main.py

# Command to run
CMD ["python3", "main.py"]
