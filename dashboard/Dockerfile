FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN python -m venv /venv && /venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /venv /venv

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV PATH="/venv/bin:$PATH"

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health', timeout=5)" || exit 1

RUN useradd -m -u 1000 app && chown -R app:app /app
USER app

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 300 app:app
