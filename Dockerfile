FROM python:3.12 AS builder

WORKDIR /app
COPY requirements.txt .

# Create pip.conf with multiple index servers for redundancy
RUN mkdir -p /root/.config/pip && \
    echo "[global]" > /root/.config/pip/pip.conf && \
    echo "timeout = 300" >> /root/.config/pip/pip.conf && \
    echo "retries = 5" >> /root/.config/pip/pip.conf && \
    echo "index-url = https://pypi.org/simple" >> /root/.config/pip/pip.conf && \
    echo "extra-index-url = https://pypi.tuna.tsinghua.edu.cn/simple https://download.pytorch.org/whl/cpu" >> /root/.config/pip/pip.conf

# Update pip and install dependencies from requirements.txt
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Clean up pip cache
RUN rm -rf /root/.cache/pip

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/
COPY . .

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
