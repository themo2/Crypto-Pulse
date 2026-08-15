FROM python:3.11-slim

# Install Java JRE for PySpark model inference
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
