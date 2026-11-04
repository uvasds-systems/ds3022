FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir "websockets>=15,<17"
COPY main.py .
ENV PYTHONUNBUFFERED=1 PORT=8765 RATE=1000 BUFFER_SECONDS=60
EXPOSE 8765
CMD ["python", "main.py"]
