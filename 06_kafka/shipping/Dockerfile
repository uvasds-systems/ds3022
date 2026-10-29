# One image for the shipping pipeline; each k8s Deployment picks its script via `args`.
# Build (cluster nodes are x86_64):
#   docker buildx build --platform linux/amd64 -t ghcr.io/nmagee/shipping:1.1 --push .
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Non-root user owns /app so log files and the quixstreams state dir can be written here
RUN useradd --uid 1000 --create-home app && chown app:app /app
COPY --chown=app:app shipping-producer.py shipping-consumer.py shipping-map.py ./
COPY --chown=app:app .streamlit/ .streamlit/
USER 1000

EXPOSE 8501
ENTRYPOINT ["python"]
CMD ["shipping-producer.py"]
