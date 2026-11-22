FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STOCKPACT_DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir --no-deps -e .

RUN mkdir -p /data && useradd -m -u 10001 app && chown -R app /data /app
USER app

EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).status==200 else 1)"

# 单进程 + 内置后台线程（中继/恢复）。多实例部署时把 STOCKPACT_BACKGROUND 设为 0，
# 由独立的 worker 进程承担轮询，见 README「部署形态」。
CMD ["uvicorn", "stockpact.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
