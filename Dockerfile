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

# Update pip and install basic tools
RUN pip install --upgrade pip setuptools wheel

# Install packages individually with retries and without dependency resolution
RUN pip --no-cache-dir install --default-timeout=300 numpy || pip --no-cache-dir install --default-timeout=300 numpy && \
    pip --no-cache-dir install --default-timeout=300 pandas || pip --no-cache-dir install --default-timeout=300 pandas && \
    pip --no-cache-dir install --default-timeout=300 scipy || pip --no-cache-dir install --default-timeout=300 scipy && \
    pip --no-cache-dir install --default-timeout=300 requests || pip --no-cache-dir install --default-timeout=300 requests && \
    pip --no-cache-dir install --default-timeout=300 pyyaml || pip --no-cache-dir install --default-timeout=300 pyyaml && \
    pip --no-cache-dir install --default-timeout=300 uvicorn || pip --no-cache-dir install --default-timeout=300 uvicorn && \
    pip --no-cache-dir install --default-timeout=300 fastapi || pip --no-cache-dir install --default-timeout=300 fastapi && \
    # Install the rest with retries and ignoring errors
    pip --no-cache-dir install --default-timeout=300 -r requirements.txt || true && \
    # Try again for any missing packages
    pip --no-cache-dir install --default-timeout=300 -r requirements.txt || true && \
    rm -rf /root/.cache/pip

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY . .

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]