FROM python:3.11-slim

WORKDIR /service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# DATA_DIR and NODE_ID are provided via docker-compose environment
ENV DATA_DIR=/data/chunks

# Healthcheck uses Python's stdlib urllib (no curl in slim image)
HEALTHCHECK --interval=5s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
