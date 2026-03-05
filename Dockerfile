FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -e .

RUN mkdir -p /app/output

CMD ["python", "-m", "alert_project.cli", "--input", "/data/data.csv", "--output", "/app/output/alerts.jsonl", "--chunksize", "200000"]
