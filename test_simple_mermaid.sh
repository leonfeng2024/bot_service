#!/bin/bash
# 简单的mermaid-cli测试脚本

echo "Creating simple mermaid diagram..."
cat > simple.mmd << 'EOL'
graph TD
    A[Start] --> B[End]
EOL

echo -e "\n=== 使用puppeteer配置测试 ==="
mmdc -i simple.mmd -o simple.png -b transparent -p /app/puppeteer-config.json

if [ -f "simple.png" ]; then
  echo -e "\n✅ 成功! 图片已生成:"
  ls -la simple.png
  echo -e "\n图片将在以下位置可用: $(pwd)/simple.png"
else
  echo -e "\n❌ 失败! 未能生成图片"
fi 