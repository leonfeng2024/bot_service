#!/bin/bash
# 调试脚本，用于检查和修复mermaid-cli环境问题

echo "=== 系统信息 ==="
uname -a
cat /etc/os-release

echo -e "\n=== Node.js & npm版本 ==="
node -v
npm -v

echo -e "\n=== mmdc版本 ==="
mmdc -V || echo "mmdc命令不可用"

echo -e "\n=== 检查libnss3.so ==="
ldconfig -p | grep libnss3
ls -la /usr/lib/*/libnss3.so || echo "libnss3.so未找到"

echo -e "\n=== 检查Chromium ==="
ls -la /usr/bin/chromium || echo "Chromium未找到"
ldd /usr/bin/chromium | grep "not found" || echo "Chromium依赖齐全"

echo -e "\n=== 尝试修复问题 ==="
echo "1. 更新软件包列表..."
apt-get update

echo "2. 确保安装了所有Puppeteer依赖..."
apt-get install -y --no-install-recommends \
    libnspr4 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2

echo "3. 创建一个测试mermaid文件..."
cat > test.mmd << 'EOL'
graph TD
    A[开始] --> B[结束]
EOL

echo "4. 尝试生成图片..."
mmdc -i test.mmd -o test.png -b transparent

if [ -f "test.png" ]; then
    echo "成功! 生成了test.png"
    ls -la test.png
else
    echo "失败! 未生成test.png"
fi

echo -e "\n=== 调试完成 ===" 