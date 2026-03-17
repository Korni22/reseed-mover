FROM python:3-alpine
RUN pip install --no-cache-dir requests
COPY reseed-mover.py /app/reseed-mover.py
ENTRYPOINT ["python3", "/app/reseed-mover.py"]
