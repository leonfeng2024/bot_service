#!/bin/bash
# 在Docker容器中测试mermaid-cli

# 创建一个简单的mermaid图
echo "Creating test mermaid diagram..."
cat > test.mmd << 'EOL'
graph TD
    A[开始] --> B[处理]
    B --> C[结束]
EOL

# 测试默认方式
echo -e "\n=== 测试1: 基本命令 ==="
mmdc -i test.mmd -o test1.png -b transparent

# 测试无沙盒模式
echo -e "\n=== 测试2: 无沙盒模式 ==="
PUPPETEER_NO_SANDBOX=true mmdc -i test.mmd -o test2.png -b transparent

# 测试使用配置文件
echo -e "\n=== 测试3: 使用配置文件 ==="
mmdc -i test.mmd -o test3.png -b transparent -p /app/puppeteer-config.json

# 检查生成结果
echo -e "\n=== 测试结果 ==="
ls -la test*.png 2>/dev/null || echo "未生成任何图片"

echo -e "\n测试完成!" 