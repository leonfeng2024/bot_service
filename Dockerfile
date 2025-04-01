FROM python:3.12 AS builder

WORKDIR /app
COPY requirements.txt .

# Create pip.conf with multiple index servers for redundancy
RUN mkdir -p /root/.config/pip && \
    echo "[global]" > /root/.config/pip/pip.conf && \
    echo "timeout = 300" >> /root/.config/pip/pip.conf && \
    echo "retries = 5" >> /root/.config/pip/pip.conf && \
    echo "index-url = https://pypi.org/simple" >> /root/.config/pip/pip.conf && \
    echo "extra-index-url = https://pypi.tuna.tsinghua.edu.cn/simple" >> /root/.config/pip/pip.conf

# Install dependencies with retry for large packages
RUN pip install --no-cache-dir --default-timeout=300 --retries=5 pip setuptools wheel && \
    # Try installing large/problematic packages separately with retries
    for i in 1 2 3; do pip install --no-cache-dir faiss-cpu && break || sleep 15; done && \
    # Install the rest of the requirements
    pip install --no-cache-dir --default-timeout=300 --retries=5 -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY . .

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]