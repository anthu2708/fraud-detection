FROM python:3.13-slim
WORKDIR /app
COPY scoring/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scoring/main.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
