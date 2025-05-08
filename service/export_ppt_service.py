import pandas as pd
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
import os
from datetime import datetime
import requests
import base64
from io import BytesIO
from pptx.dml.color import RGBColor
from typing import Dict, List, Any
import mermaid as md
from mermaid.graph import Graph
import matplotlib.pyplot as plt
import networkx as nx
from PIL import Image, ImageDraw, ImageFont
import traceback
import subprocess

class ExportPPTService:
    def __init__(self):
        self.temp_dir = "temp"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

    async def create_mermaid_diagram(self, relationships_df: pd.DataFrame, sandbox: bool = True) -> str:
        """
        Create a diagram using Mermaid from relationship data
        
        Args:
            relationships_df: DataFrame containing relationship data
            sandbox: Whether to use the sandbox mode
            
        Returns:
            Path to the generated diagram
        """
        try:
            # Import os module to ensure accessibility everywhere
            import os
            import subprocess
            
            # Generate Mermaid diagram definition with compact layout settings
            mermaid_definition = """graph LR
%%{
  init: {
    'flowchart': {
      'nodeSpacing': 40,
      'rankSpacing': 60,
      'curve': 'basis',
      'nodeWidth': 200,
      'nodeHeight': 40,
      'edgeLengthFactor': '1',
      'arrowMarkerAbsolute': false,
      'htmlLabels': true
    },
    'themeVariables': {
      'fontSize': '14px',
      'fontFamily': 'Arial',
      'primaryColor': '#e6f2ff',
      'primaryTextColor': '#333',
      'primaryBorderColor': '#5d9fe4',
      'lineColor': '#4d77a5',
      'secondaryColor': '#eee',
      'tertiaryColor': '#fff',
      'arrowheadSize': '1.2'
    }
  }
}%%

classDef tableNode fill:#d7e9f7,stroke:#3c78d8,stroke-width:2px,font-size:14px,text-align:center;
classDef fieldNode fill:#fff2cc,stroke:#f1c232,stroke-width:1px,font-size:12px,text-align:left;
classDef relationshipEdge stroke:#4d77a5,stroke-width:2px;"""

            # 提取所有唯一的表名
            all_tables = set()
            for content in relationships_df['content']:
                if isinstance(content, str) and "表 " in content and " 通过字段 " in content and " 关联到表 " in content:
                    parts = content.split(" 通过字段 ")
                    if len(parts) >= 2:
                        source_part = parts[0].replace("表 ", "").strip()
                        target_part = parts[1].split(" 关联到表 ")[1].split(" 的字段 ")[0].strip()
                        all_tables.add(source_part)
                        all_tables.add(target_part)
                    else:
                        print(f"Warning: Unexpected content format: {content}")

            # 为每个表生成节点
            for table in all_tables:
                table_id = table.replace('.', '_').replace('-', '_')
                mermaid_definition += f"\n    {table_id}[<div style='padding:5px;'>{table}</div>]"

            # 添加关系
            for content in relationships_df['content']:
                if isinstance(content, str) and "表 " in content and " 通过字段 " in content and " 关联到表 " in content:
                    try:
                        # 解析内容
                        parts = content.split(" 通过字段 ")
                        source_table = parts[0].replace("表 ", "").strip()
                        remaining = parts[1].split(" 关联到表 ")
                        source_field = remaining[0].strip()
                        target_remaining = remaining[1].split(" 的字段 ")
                        target_table = target_remaining[0].strip()
                        target_field = target_remaining[1].strip() if len(target_remaining) > 1 else "unknown"
                        
                        # 创建关系ID
                        table1_id = source_table.replace('.', '_').replace('-', '_')
                        table2_id = target_table.replace('.', '_').replace('-', '_')
                        
                        # 添加关系
                        label = f"{source_field} -> {target_field}"
                        mermaid_definition += f"\n    {table1_id} -->|{label}| {table2_id}"
                    except Exception as e:
                        print(f"Error processing relationship: {content}, Error: {str(e)}")

            # 设置样式类
            for table in all_tables:
                table_id = table.replace('.', '_').replace('-', '_')
                mermaid_definition += f"\n    class {table_id} tableNode;"

            # 处理无法识别的数据
            if not all_tables:
                mermaid_definition += "\n    error[无法识别的数据格式]\n"
                
            mermaid_definition += """
"""
            print(f"Generated Mermaid definition:\n{mermaid_definition}")
            
            # Ensure output directory exists
            output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # First save mermaid definition to temporary file
            mermaid_file = os.path.join(output_dir, f"mermaid_{timestamp}.mmd")
            image_path = os.path.join(output_dir, f"relationships_{timestamp}.png")
            
            # Save mermaid definition file
            with open(mermaid_file, 'w', encoding='utf-8') as f:
                f.write(mermaid_definition)
            
            # Use directly installed mmdc command line tool to render image - using no sandbox mode
            print("Attempting to generate diagram using mmdc command line tool...")
            try:
                # Use environment variables to ensure Puppeteer runs correctly
                env = os.environ.copy()
                env['PUPPETEER_NO_SANDBOX'] = 'true'
                env['PUPPETEER_EXECUTABLE_PATH'] = '/usr/bin/chromium'
                
                # Build mmdc command and execute - add no sandbox option
                cmd = ["mmdc", "-i", mermaid_file, "-o", image_path, "-b", "transparent", "-p", "/app/puppeteer-config.json"]
                result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
                print(f"mmdc output: {result.stdout}")
                
                if os.path.exists(image_path) and os.path.getsize(image_path) > 1000:
                    print(f"Successfully generated diagram using mermaid-cli")
                    return image_path
                else:
                    print("Generated image file is too small or doesn't exist, trying other methods")
                    # Try checking dependencies
                    try:
                        subprocess.run(["ldd", "/usr/bin/chromium"], check=True, capture_output=True, text=True)
                        print("Chromium dependency check completed")
                    except Exception as dep_error:
                        print(f"Chromium dependency check error: {str(dep_error)}")
            except Exception as mmdc_error:
                print(f"mmdc command line error: {str(mmdc_error)}")
                if hasattr(mmdc_error, 'stderr'):
                    print(f"mmdc stderr: {mmdc_error.stderr}")
            
            # 备选方法：尝试使用NPX运行mermaid-cli，使用无沙盒模式和puppeteer配置
            print("尝试使用npx运行mermaid-cli...")
            try:
                # 使用npx安装并运行mmdc，添加无沙盒选项
                cmd = f"PUPPETEER_NO_SANDBOX=true npx --yes @mermaid-js/mermaid-cli mmdc -i {mermaid_file} -o {image_path} -b transparent -p /app/puppeteer-config.json"
                result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True, env=env)
                print(f"npx mmdc输出: {result.stdout}")
                
                if os.path.exists(image_path) and os.path.getsize(image_path) > 1000:
                    print(f"Successfully generated diagram using npx mermaid-cli")
                    return image_path
                else:
                    print("npx mmdc生成的图像文件过小或不存在，尝试下一种方法")
            except Exception as npx_error:
                print(f"npx命令错误: {str(npx_error)}")
                if hasattr(npx_error, 'stderr'):
                    print(f"npx stderr: {npx_error.stderr}")
            
            # 使用matplotlib作为最后的备用方法
            print("使用matplotlib作为备选方案绘制关系图...")
            import matplotlib.pyplot as plt
            import networkx as nx
            
            # 创建图对象
            G = nx.DiGraph()
            
            # 添加节点
            for table in all_tables:
                G.add_node(table)
            
            # 添加边
            for content in relationships_df['content']:
                if isinstance(content, str) and "表 " in content and " 通过字段 " in content and " 关联到表 " in content:
                    try:
                        parts = content.split(" 通过字段 ")
                        source_table = parts[0].replace("表 ", "").strip()
                        remaining = parts[1].split(" 关联到表 ")
                        source_field = remaining[0].strip()
                        target_remaining = remaining[1].split(" 的字段 ")
                        target_table = target_remaining[0].strip()
                        target_field = target_remaining[1].strip() if len(target_remaining) > 1 else "unknown"
                        
                        # 添加边
                        G.add_edge(source_table, target_table, label=f"{source_field} -> {target_field}")
                    except Exception as edge_err:
                        print(f"Error adding edge from content: {content}, Error: {str(edge_err)}")
            
            # 如果没有边，尝试从纯过程/表名关系中提取
            if not list(G.edges()):
                print("没有找到完整关系描述，尝试从内容中提取基本关系...")
                for content in relationships_df['content']:
                    try:
                        # 尝试匹配 "procedure x uses table y" 模式
                        if isinstance(content, str):
                            words = content.split()
                            # 查找表/过程/视图等名称
                            for i, word in enumerate(words):
                                if i > 0 and len(word) > 3 and not word.startswith(('the', 'and', 'with', 'from', 'into')):
                                    # 简单启发式：添加看起来像表/过程名的词作为节点
                                    if word.lower() not in ('table', 'view', 'procedure', 'function'):
                                        G.add_node(word)
                            
                            # 尝试连接明显相关的节点
                            nodes = list(G.nodes())
                            for i in range(len(nodes)):
                                for j in range(i+1, len(nodes)):
                                    if nodes[i] in content and nodes[j] in content:
                                        # 检查两个节点名称是否在同一个句子中出现
                                        if abs(content.find(nodes[i]) - content.find(nodes[j])) < 100:
                                            G.add_edge(nodes[i], nodes[j], label="related")
                    except Exception as text_err:
                        print(f"Error processing text relations: {str(text_err)}")
            
            # 图为空的情况处理
            if not G.nodes():
                print("无法提取有效的关系，创建简单图表")
                G.add_node("No relationships found")
            
            # 设置图形大小
            plt.figure(figsize=(12, 9), dpi=100)
            
            # 使用spring布局，增加节点间距和迭代次数提高布局质量
            pos = nx.spring_layout(G, k=0.9, iterations=100, seed=42)
            
            # 绘制节点 - 使用更大的节点和更鲜明的颜色
            nx.draw_networkx_nodes(G, pos, 
                                   node_size=3000, 
                                   node_color='#d7e9f7', 
                                   edgecolors='#3c78d8', 
                                   linewidths=2)
            
            # 绘制边 - 增加箭头大小和弯曲度
            nx.draw_networkx_edges(G, pos, 
                                   edge_color='#4d77a5', 
                                   width=2.0, 
                                   arrowsize=20, 
                                   arrowstyle='->', 
                                   connectionstyle='arc3,rad=0.1')
            
            # 添加节点标签 - 增加字体大小和权重
            nx.draw_networkx_labels(G, pos, 
                                    font_size=14, 
                                    font_weight='bold',
                                    font_family='sans-serif')
            
            # 添加边标签 - 确保边标签清晰可见
            edge_labels = nx.get_edge_attributes(G, 'label')
            nx.draw_networkx_edge_labels(G, pos, 
                                         edge_labels=edge_labels, 
                                         font_size=10,
                                         font_color='#000066',
                                         bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=2))
            
            # 保存高质量图像
            plt.axis('off')
            plt.tight_layout()
            plt.savefig(image_path, format='png', dpi=150, bbox_inches='tight', pad_inches=0.5, transparent=True)
            plt.close()
            
            print(f"Successfully generated diagram using matplotlib")
            print(f"Diagram saved at: {image_path}")
            
            return image_path
                
        except Exception as e:
            print(f"Error creating diagram: {str(e)}")
            traceback.print_exc()
            return None

    async def create_ppt(self, excel_file: str = "relationship.xlsx", output_file: str = "database_relationships.pptx"):
        """
        Create a PPT presentation from relationship data in Excel
        
        Args:
            excel_file: Path to the Excel file containing relationship data
            output_file: Path where the PPT file should be saved
        """
        try:
            # Read Excel file
            df = pd.read_excel(excel_file)
            print(f"Excel columns: {df.columns.tolist()}")
            
            # Create presentation
            prs = Presentation()
            
            # Add title slide
            title_slide_layout = prs.slide_layouts[0]
            slide = prs.slides.add_slide(title_slide_layout)
            title = slide.shapes.title
            subtitle = slide.placeholders[1]
            
            title.text = "Database Relationships"
            subtitle.text = f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Add relationship diagram slide - use a landscape slide for better fit
            blank_slide_layout = prs.slide_layouts[6]
            slide = prs.slides.add_slide(blank_slide_layout)
            
            # Create and add relationship diagram
            diagram_path = None
            try:
                diagram_path = await self.create_mermaid_diagram(df)
                
                # 检查是否是有效的图像文件
                if diagram_path is not None:
                    is_image = diagram_path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))
                    
                    # Add the diagram to the slide with adjusted size and position
                    # Use most of the slide area
                    left = Inches(0.25)  # Reduced margin
                    top = Inches(0.75)   # Reduced top margin
                    width = Inches(9.5)  # Almost full width
                    
                    if is_image and os.path.exists(diagram_path) and os.path.getsize(diagram_path) > 1000:
                        slide.shapes.add_picture(diagram_path, left, top, width=width)
                    else:
                        # 如果图像生成失败，添加一个文本框作为替代
                        txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                        tf = txt_box.text_frame
                        p = tf.add_paragraph()
                        p.text = "无法生成关系图。请查看详细信息表格。"
                        p.font.size = Pt(14)
                        p.font.bold = True
                        
                        # 如果是文本文件，读取内容并显示
                        if diagram_path is not None and diagram_path.lower().endswith('.txt') and os.path.exists(diagram_path):
                            with open(diagram_path, 'r') as f:
                                mermaid_content = f.read()
                                p = tf.add_paragraph()
                                p.text = "Mermaid定义（供参考）："
                                p.font.size = Pt(12)
                                p.font.bold = True
                                
                                p = tf.add_paragraph()
                                p.text = mermaid_content
                                p.font.size = Pt(8)
                else:
                    # 如果图表生成完全失败，添加错误信息
                    left = Inches(0.25)
                    top = Inches(0.75)
                    width = Inches(9.5)
                    txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                    tf = txt_box.text_frame
                    p = tf.add_paragraph()
                    p.text = "关系图生成失败。请查看详细信息表格。"
                    p.font.size = Pt(14)
                    p.font.bold = True
            except Exception as diagram_error:
                print(f"Error adding diagram to PPT: {str(diagram_error)}")
                traceback.print_exc()
                txt_box = slide.shapes.add_textbox(Inches(0.25), Inches(0.75), Inches(9.5), Inches(5))
                tf = txt_box.text_frame
                p = tf.add_paragraph()
                p.text = "生成关系图时出错。请查看详细信息表格。"
                p.font.size = Pt(14)
                p.font.bold = True
            
            # Add title to the diagram slide
            title_box = slide.shapes.add_textbox(Inches(0.25), Inches(0.1), Inches(9.5), Inches(0.5))
            title_frame = title_box.text_frame
            title_para = title_frame.add_paragraph()
            title_para.text = "Table Relationships Diagram"
            title_para.alignment = PP_ALIGN.CENTER
            title_para.font.size = Pt(18)
            title_para.font.bold = True
            
            # Add details slide
            table_slide_layout = prs.slide_layouts[6]
            slide = prs.slides.add_slide(table_slide_layout)
            
            # Add title to the details slide
            title_box = slide.shapes.add_textbox(Inches(0.25), Inches(0.5), Inches(9.5), Inches(0.5))
            title_frame = title_box.text_frame
            title_para = title_frame.add_paragraph()
            title_para.text = "Relationship Details"
            title_para.alignment = PP_ALIGN.CENTER
            title_para.font.size = Pt(18)
            title_para.font.bold = True
            
            # Add table with relationship details
            rows = min(len(df) + 1, 20)  # 限制最多20行，防止表格过大
            cols = min(len(df.columns), 5)  # 限制最多5列
            top = Inches(1)
            table_width = Inches(9.5)  # Wider table
            table_height = Inches(0.3 * rows)  # Shorter rows
            
            table = slide.shapes.add_table(rows, cols, Inches(0.25), top, table_width, table_height).table
            
            # 根据实际数据列确定表头
            if 'content' in df.columns and 'description' in df.columns:
                headers = ["Content", "Description", "Created At", "Score", "Source"]
                col_map = {
                    0: 'content',
                    1: 'description',
                    2: 'created_at' if 'created_at' in df.columns else None,
                    3: 'score' if 'score' in df.columns else None,
                    4: 'source' if 'source' in df.columns else None
                }
            else:
                # 使用默认表头
                headers = ["Table", "First Relationship", "View", "Second Relationship", "Dataset"]
                col_map = None
            
            # 设置表头
            for i, header in enumerate(headers[:cols]):
                cell = table.cell(0, i)
                cell.text = header
                paragraph = cell.text_frame.paragraphs[0]
                paragraph.font.bold = True
                paragraph.font.size = Pt(11)  # Slightly smaller font
            
            # 填充表格数据
            for i, row_data in df.iterrows():
                if i >= rows - 1:  # 如果超过了表格行数限制，跳出循环
                    break
                    
                for j in range(cols):
                    if col_map and col_map[j] in df.columns:
                        cell_value = str(row_data[col_map[j]]) if not pd.isna(row_data[col_map[j]]) else ""
                    else:
                        cell_value = str(row_data[j]) if j < len(df.columns) and not pd.isna(row_data[j]) else ""
                    
                    # 限制单元格文本长度，防止单元格过大
                    if len(cell_value) > 200:
                        cell_value = cell_value[:197] + "..."
                        
                    table.cell(i + 1, j).text = cell_value
                    # 为表格数据设置更小的字体
                    paragraph = table.cell(i + 1, j).text_frame.paragraphs[0]
                    paragraph.font.size = Pt(9)  # Smaller font for data
            
            # 如果数据行数超过表格限制，添加一个说明
            if len(df) > rows - 1:
                note_slide = prs.slides.add_slide(table_slide_layout)
                note_box = note_slide.shapes.add_textbox(Inches(0.25), Inches(1), Inches(9.5), Inches(5))
                note_frame = note_box.text_frame
                note_para = note_frame.add_paragraph()
                note_para.text = f"注意：数据共有{len(df)}行，由于篇幅限制，只显示了前{rows-1}行。"
                note_para.font.size = Pt(14)
                
                # 添加标题
                title_box = note_slide.shapes.add_textbox(Inches(0.25), Inches(0.1), Inches(9.5), Inches(0.5))
                title_frame = title_box.text_frame
                title_para = title_frame.add_paragraph()
                title_para.text = "数据完整性说明"
                title_para.alignment = PP_ALIGN.CENTER
                title_para.font.size = Pt(18)
                title_para.font.bold = True
            
            # Save presentation
            prs.save(output_file)
            
            # 我们不再删除diagram_path，而是将其保留，以便可以在其他地方使用
            if diagram_path is not None and os.path.exists(diagram_path):
                print(f"Diagram saved at: {diagram_path}")
            print(f"PPT created at: {output_file}")
            
            return True
            
        except Exception as e:
            print(f"Error creating PPT: {str(e)}")
            traceback.print_exc()
            return False

    async def export_to_ppt(self, excel_file: str, filename_prefix: str) -> str:
        """
        Export Excel data to PowerPoint
        Returns the path to the created PowerPoint file
        """
        try:
            # Create a new presentation
            prs = Presentation()
            
            # Import pandas here to read the Excel file
            import pandas as pd
            df = pd.read_excel(excel_file)
            
            # Add a title slide
            title_slide_layout = prs.slide_layouts[0]
            slide = prs.slides.add_slide(title_slide_layout)
            title = slide.shapes.title
            subtitle = slide.placeholders[1]
            
            title.text = "Database Query Results"
            subtitle.text = f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Try to create and add relationship diagram
            diagram_path = None
            try:
                # Add diagram slide
                blank_slide_layout = prs.slide_layouts[6]
                slide = prs.slides.add_slide(blank_slide_layout)
                
                # Create and add relationship diagram
                diagram_path = await self.create_mermaid_diagram(df)
                
                # Check if it's a valid image file
                if diagram_path is not None:
                    is_image = diagram_path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))
                    
                    # Add the diagram to the slide with adjusted size and position
                    left = Inches(0.25)
                    top = Inches(0.75)
                    width = Inches(9.5)
                    
                    if is_image and os.path.exists(diagram_path) and os.path.getsize(diagram_path) > 1000:
                        slide.shapes.add_picture(diagram_path, left, top, width=width)
                    else:
                        # If image generation failed, add a text box as fallback
                        txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                        tf = txt_box.text_frame
                        p = tf.add_paragraph()
                        p.text = "Unable to generate relationship diagram. Please check the detailed information section."
                        p.font.size = Pt(14)
                        p.font.bold = True
                        
                        # If it's a text file, read and display content
                        if diagram_path is not None and diagram_path.lower().endswith('.txt') and os.path.exists(diagram_path):
                            with open(diagram_path, 'r') as f:
                                mermaid_content = f.read()
                                p = tf.add_paragraph()
                                p.text = "Mermaid definition (for reference):"
                                p.font.size = Pt(12)
                                p.font.bold = True
                                
                                p = tf.add_paragraph()
                                p.text = mermaid_content
                                p.font.size = Pt(8)
                else:
                    # If diagram generation completely failed, add error message
                    left = Inches(0.25)
                    top = Inches(0.75)
                    width = Inches(9.5)
                    txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                    tf = txt_box.text_frame
                    p = tf.add_paragraph()
                    p.text = "Relationship diagram generation failed. Please check the detailed information section."
                    p.font.size = Pt(14)
                    p.font.bold = True
                
                # Add title to the diagram slide
                title_box = slide.shapes.add_textbox(left, Inches(0.1), width, Inches(0.5))
                title_frame = title_box.text_frame
                title_para = title_frame.add_paragraph()
                title_para.text = "Field Relationships Diagram"
                title_para.alignment = PP_ALIGN.CENTER
                title_para.font.size = Pt(18)
                title_para.font.bold = True
                
                print(f"Added relationship diagram to PPT")
            except Exception as diagram_error:
                print(f"Error creating diagram: {str(diagram_error)}")
                traceback.print_exc()
                print(f"Continuing with PPT creation without diagram")
            
            # Add content slides
            # We'll create a table slide for the data
            slide_layout = prs.slide_layouts[5]  # Title and Content layout
            slide = prs.slides.add_slide(slide_layout)
            title = slide.shapes.title
            title.text = "Database Relationships"
            
            # Add table
            rows, cols = df.shape
            left = Inches(0.5)
            top = Inches(1.5)
            width = Inches(9.0)
            height = Inches(5.0)
            
            # 限制表格的大小以避免溢出
            rows = min(rows, 20) + 1  # +1 是为了表头
            cols = min(cols, 10)
            
            # Add a table with headers
            shapes = slide.shapes
            table = shapes.add_table(rows, cols, left, top, width, height).table
            
            # Set header row
            for i, col_name in enumerate(df.columns[:cols]):
                table.cell(0, i).text = str(col_name)
                # Format the header row
                for paragraph in table.cell(0, i).text_frame.paragraphs:
                    for run in paragraph.runs:
                        run.font.bold = True
                        run.font.size = Pt(12)
            
            # Fill in the data rows
            for i in range(min(rows-1, df.shape[0])):
                for j in range(min(cols, df.shape[1])):
                    cell_value = str(df.iloc[i, j]) if not pd.isna(df.iloc[i, j]) else ""
                    # 限制单元格文本长度
                    if len(cell_value) > 200:
                        cell_value = cell_value[:197] + "..."
                    table.cell(i + 1, j).text = cell_value
            
            # 如果数据行数超过表格限制，添加一个说明
            if df.shape[0] > rows - 1:
                note_slide = prs.slides.add_slide(slide_layout)
                note_title = note_slide.shapes.title
                note_title.text = "数据完整性说明"
                
                txt_box = shapes.add_textbox(left, top, width, Inches(1))
                tf = txt_box.text_frame
                p = tf.add_paragraph()
                p.text = f"注意：数据共有{df.shape[0]}行，由于篇幅限制，只显示了前{rows-1}行。"
                p.font.size = Pt(14)
            
            # Define output file path
            ppt_file = os.path.join(self.output_dir, f"{filename_prefix}.pptx")
            
            # Save the presentation
            prs.save(ppt_file)
            
            return ppt_file
        except Exception as e:
            print(f"Error exporting to PowerPoint: {str(e)}")
            traceback.print_exc()
            
            # 尝试创建一个最基本的PPT，确保至少有一个输出
            try:
                prs = Presentation()
                slide = prs.slides.add_slide(prs.slide_layouts[0])
                title = slide.shapes.title
                subtitle = slide.placeholders[1]
                
                title.text = "Error Creating Presentation"
                subtitle.text = f"Error: {str(e)}"
                
                # 保存输出
                ppt_file = os.path.join(self.output_dir, f"{filename_prefix}_error.pptx")
                prs.save(ppt_file)
                
                return ppt_file
            except:
                # 如果连简单的PPT都创建失败，抛出原始异常
                raise
    
    async def append_to_ppt(self, data: List[Dict[str, Any]], ppt_file: str) -> str:
        """
        Append additional data to an existing PowerPoint file
        Returns the path to the updated PowerPoint file
        """
        try:
            # Open the existing presentation
            prs = Presentation(ppt_file)
            
            # Check if data contains content field and table relationship information
            has_relationship_data = False
            for item in data:
                content = item.get('content', '')
                if content and isinstance(content, str) and "表" in content and "关联到表" in content:
                    has_relationship_data = True
                    break
            
            if has_relationship_data:
                try:
                    # Create a temporary DataFrame for diagram generation
                    import pandas as pd
                    temp_df = pd.DataFrame(data)
                    
                    # Add diagram slide
                    blank_slide_layout = prs.slide_layouts[6]
                    slide = prs.slides.add_slide(blank_slide_layout)
                    
                    # Create and add relationship diagram
                    diagram_path = await self.create_mermaid_diagram(temp_df)
                    
                    # Check if it's a valid image file
                    if diagram_path is not None:
                        is_image = diagram_path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))
                        
                        # Add the diagram to the slide with adjusted size and position
                        left = Inches(0.25)
                        top = Inches(0.75)
                        width = Inches(9.5)
                        
                        if is_image and os.path.exists(diagram_path) and os.path.getsize(diagram_path) > 1000:
                            slide.shapes.add_picture(diagram_path, left, top, width=width)
                        else:
                            # If image generation failed, add a text box as fallback
                            txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                            tf = txt_box.text_frame
                            p = tf.add_paragraph()
                            p.text = "Unable to generate relationship diagram. Please check the detailed information section."
                            p.font.size = Pt(14)
                            p.font.bold = True
                            
                            # If it's a text file, read and display content
                            if diagram_path is not None and diagram_path.lower().endswith('.txt') and os.path.exists(diagram_path):
                                with open(diagram_path, 'r') as f:
                                    mermaid_content = f.read()
                                    p = tf.add_paragraph()
                                    p.text = "Mermaid definition (for reference):"
                                    p.font.size = Pt(12)
                                    p.font.bold = True
                                    
                                    p = tf.add_paragraph()
                                    p.text = mermaid_content
                                    p.font.size = Pt(8)
                    else:
                        # If diagram generation completely failed, add error message
                        left = Inches(0.25)
                        top = Inches(0.75)
                        width = Inches(9.5)
                        txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                        tf = txt_box.text_frame
                        p = tf.add_paragraph()
                        p.text = "Relationship diagram generation failed. Please check the detailed information section."
                        p.font.size = Pt(14)
                        p.font.bold = True
                    
                    # Add title to the diagram slide
                    title_box = slide.shapes.add_textbox(left, Inches(0.1), width, Inches(0.5))
                    title_frame = title_box.text_frame
                    title_para = title_frame.add_paragraph()
                    title_para.text = "Additional Field Relationships"
                    title_para.alignment = PP_ALIGN.CENTER
                    title_para.font.size = Pt(18)
                    title_para.font.bold = True
                    
                    print(f"Added new relationship diagram to PPT")
                except Exception as diagram_error:
                    print(f"Error creating additional diagram: {str(diagram_error)}")
                    traceback.print_exc()
                    print(f"Continuing with presentation without additional diagram")
            
            # Add slides for each data item with proper content handling
            for i, item in enumerate(data):
                # Get raw content without any processing or formatting
                content = item.get('content', '')
                source = item.get('source', 'search_result')
                # Use source as title
                title_text = f"{source.capitalize()} Results"
                
                if not content:
                    continue
                
                # Add a new slide
                slide_layout = prs.slide_layouts[5]  # Title and Content layout
                slide = prs.slides.add_slide(slide_layout)
                
                # Set slide title
                title = slide.shapes.title
                title.text = title_text
                
                # Add content text box
                left = Inches(0.5)
                top = Inches(1.5)
                width = Inches(9.0)
                height = Inches(5.0)
                
                txBox = slide.shapes.add_textbox(left, top, width, height)
                tf = txBox.text_frame
                tf.word_wrap = True
                
                # Add original content, ensuring format preservation
                p = tf.add_paragraph()
                # Ensure content is string
                if not isinstance(content, str):
                    try:
                        import json
                        content = json.dumps(content, ensure_ascii=False, indent=2)
                    except:
                        content = str(content)
                
                p.text = content
                p.font.size = Pt(12)
            
            # Save the updated presentation
            prs.save(ppt_file)
            print(f"Successfully saved updated PPT to {ppt_file}")
            
            return ppt_file
        except Exception as e:
            print(f"Error appending to PowerPoint: {str(e)}")
            traceback.print_exc()
            # Return original file path to ensure processing continues
            return ppt_file 