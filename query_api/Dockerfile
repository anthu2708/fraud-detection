FROM python:3.13-slim
WORKDIR /app
COPY query_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY query_api/ query_api/
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "query_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
