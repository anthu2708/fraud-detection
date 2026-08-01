FROM python:3.13-slim
WORKDIR /app
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/main.py .
CMD ["python", "main.py"]
