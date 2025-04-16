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

class ExportPPTService:
    def __init__(self):
        self.temp_dir = "temp"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

    async def create_mermaid_diagram(self, relationships_df: pd.DataFrame) -> str:
        """
        Create a diagram using Mermaid from relationship data
        
        Args:
            relationships_df: DataFrame containing relationship data
            
        Returns:
            str: Path to the generated image file
        """
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
"""
        
        # Track unique nodes to avoid duplicates
        added_nodes = set()
        added_relationships = set()
        
        # Check the column names in the dataframe
        print(f"Available columns: {relationships_df.columns.tolist()}")
        
        # 检测数据框的结构
        has_content_column = 'content' in relationships_df.columns
        has_description_column = 'description' in relationships_df.columns
        
        # 定义节点和关系的样式
        mermaid_definition += """
classDef tableNode fill:#d7e9f7,stroke:#3c78d8,stroke-width:2px,font-size:14px,text-align:center;
classDef fieldNode fill:#fff2cc,stroke:#f1c232,stroke-width:1px,font-size:12px,text-align:left;
classDef relationshipEdge stroke:#4d77a5,stroke-width:2px;
"""

        # 处理类似于图片中展示的数据
        if has_content_column:
            # 假设content列包含关系信息，如"表 p_UpdateEmployeeSalary 通过字段 changed_by 关联到表 employees 的字段 employee_id"
            unique_tables = set()
            all_relationships = []
            
            # 首先提取所有表名
            for _, row in relationships_df.iterrows():
                content = str(row['content'])
                # 检查是否包含"表"和"关联到表"
                if "表" in content and "关联到表" in content:
                    try:
                        # 提取第一个表名
                        table1_parts = content.split("表")[1].split("通过")[0].strip()
                        # 提取第二个表名
                        table2_parts = content.split("关联到表")[1].split("的字段")[0].strip()
                        
                        # 提取字段名
                        field1 = content.split("通过字段")[1].split("关联到表")[0].strip()
                        field2 = content.split("的字段")[1].strip()
                        
                        # 添加表到唯一表集合
                        unique_tables.add(table1_parts)
                        unique_tables.add(table2_parts)
                        
                        # 保存关系
                        all_relationships.append({
                            'table1': table1_parts,
                            'field1': field1,
                            'table2': table2_parts,
                            'field2': field2
                        })
                    except Exception as e:
                        print(f"Error parsing relationship: {e} - Content: {content}")
            
            # 添加所有表作为节点
            for table in unique_tables:
                table_id = "".join(c if c.isalnum() else "_" for c in table)
                mermaid_definition += f"    {table_id}[<div style='padding:5px;'>{table}</div>]\n"
                added_nodes.add(table)
            
            # 添加关系
            for rel in all_relationships:
                table1_id = "".join(c if c.isalnum() else "_" for c in rel['table1'])
                table2_id = "".join(c if c.isalnum() else "_" for c in rel['table2'])
                
                # 创建唯一的关系ID
                rel_id = f"{rel['table1']}_{rel['field1']}_to_{rel['table2']}_{rel['field2']}"
                
                if rel_id not in added_relationships:
                    # 添加有标签的关系
                    label = f"{rel['field1']} -> {rel['field2']}"
                    mermaid_definition += f"    {table1_id} -->|{label}| {table2_id}\n"
                    added_relationships.add(rel_id)
            
            # 对所有表应用样式
            for table in unique_tables:
                table_id = "".join(c if c.isalnum() else "_" for c in table)
                mermaid_definition += f"    class {table_id} tableNode;\n"
                
        else:
            # 回退到原始实现或提供错误信息
            mermaid_definition += "    error[无法识别的数据格式]\n"
        
        print(f"Generated Mermaid definition:\n{mermaid_definition}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_path = os.path.join(self.output_dir, f"relationships_{timestamp}.png")
        
        try:
            # 首先尝试使用在线的Mermaid.ink服务
            try:
                # Use the Mermaid.ink service to render the diagram
                mermaid_encoded = base64.urlsafe_b64encode(mermaid_definition.encode()).decode()
                
                # Add parameters for a more compact rendering and higher quality
                mermaid_url = f"https://mermaid.ink/img/{mermaid_encoded}?width=1200&height=900&dpi=200"
                
                print(f"Requesting diagram from: {mermaid_url}")
                
                # Download the image with timeout
                response = requests.get(mermaid_url, timeout=10)  # 添加10秒超时
                
                if response.status_code == 200 and len(response.content) > 1000:  # 确保返回的内容是有效的图像
                    # Save the image
                    with open(image_path, "wb") as f:
                        f.write(response.content)
                    print(f"Successfully generated diagram using Mermaid.ink")
                    return image_path
                else:
                    print(f"Error from Mermaid.ink API: Status {response.status_code}, Content length: {len(response.content)}")
                    raise Exception(f"Invalid response from Mermaid.ink: Status {response.status_code}")
            except Exception as e:
                print(f"Error using Mermaid.ink service: {str(e)}")
                print("Trying alternative method...")
                
            # 如果在线服务失败，尝试使用本地渲染方法
            try:
                import matplotlib.pyplot as plt
                import networkx as nx
                
                # 创建一个简单的替代图表
                G = nx.DiGraph()
                
                # 添加节点
                for node in added_nodes:
                    G.add_node(node)
                
                # 添加边
                for rel in all_relationships:
                    G.add_edge(rel['table1'], rel['table2'], label=f"{rel['field1']} -> {rel['field2']}")
                
                # 设置图形大小
                plt.figure(figsize=(16, 10))
                
                # 使用spring布局
                pos = nx.spring_layout(G, k=0.5, iterations=50)
                
                # 绘制节点
                nx.draw_networkx_nodes(G, pos, node_size=3000, node_color="#d7e9f7", 
                                    edgecolors="#3c78d8", linewidths=2)
                
                # 绘制边
                nx.draw_networkx_edges(G, pos, width=2, edge_color="#4d77a5", 
                                    arrowsize=20, arrowstyle='->', connectionstyle='arc3,rad=0.1')
                
                # 添加节点标签
                nx.draw_networkx_labels(G, pos, font_size=12, font_weight='bold')
                
                # 添加边标签
                edge_labels = {(u, v): d['label'] for u, v, d in G.edges(data=True)}
                nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=10)
                
                # 保存图像
                plt.axis('off')
                plt.tight_layout()
                plt.savefig(image_path, dpi=200, bbox_inches='tight')
                plt.close()
                
                print(f"Successfully generated diagram using matplotlib")
                return image_path
            except Exception as matplotlib_error:
                print(f"Error using matplotlib for rendering: {str(matplotlib_error)}")
            
            # 如果所有方法都失败，创建一个基本的文本图像
            try:
                from PIL import Image, ImageDraw, ImageFont
                
                # 创建空白图像
                img_width, img_height = 1200, 800
                image = Image.new('RGB', (img_width, img_height), color='white')
                draw = ImageDraw.Draw(image)
                
                # 尝试加载一个字体
                try:
                    font = ImageFont.truetype("Arial", 14)
                except:
                    font = ImageFont.load_default()
                
                # 画一个标题
                draw.text((50, 30), "Database Relationship Diagram", fill='black', font=font)
                
                # 为每个关系添加一行文本
                y_position = 80
                for rel in all_relationships:
                    text = f"{rel['table1']} ({rel['field1']}) -> {rel['table2']} ({rel['field2']})"
                    draw.text((50, y_position), text, fill='black', font=font)
                    y_position += 25
                
                # 保存图像
                image.save(image_path)
                print(f"Created basic text image as fallback")
                return image_path
            except Exception as pil_error:
                print(f"Error creating basic image: {str(pil_error)}")
                
                # 如果所有图像生成方法都失败，返回路径，但会在创建演示文稿时处理
                with open(image_path, "w") as f:
                    f.write("Error generating diagram")
                return image_path
                
        except Exception as final_error:
            print(f"All diagram generation methods failed: {str(final_error)}")
            
            # 将Mermaid定义保存到文本文件，以便至少可以查看
            text_path = os.path.join(self.output_dir, f"relationships_{timestamp}.txt")
            with open(text_path, "w") as f:
                f.write(mermaid_definition)
            
            # 返回文本文件路径
            return text_path

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
            try:
                diagram_path = await self.create_mermaid_diagram(df)
                
                # 检查是否是有效的图像文件
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
                    if diagram_path.lower().endswith('.txt') and os.path.exists(diagram_path):
                        with open(diagram_path, 'r') as f:
                            mermaid_content = f.read()
                            p = tf.add_paragraph()
                            p.text = "Mermaid定义（供参考）："
                            p.font.size = Pt(12)
                            p.font.bold = True
                            
                            p = tf.add_paragraph()
                            p.text = mermaid_content
                            p.font.size = Pt(8)
            except Exception as diagram_error:
                print(f"Error adding diagram to PPT: {str(diagram_error)}")
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
            if locals().get('diagram_path') and os.path.exists(diagram_path):
                print(f"Diagram saved at: {diagram_path}")
            print(f"PPT created at: {output_file}")
            
            return True
            
        except Exception as e:
            print(f"Error creating PPT: {str(e)}")
            import traceback
            print(traceback.format_exc())
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
            
            # 尝试创建和添加关系图表
            try:
                # 添加diagram slide
                blank_slide_layout = prs.slide_layouts[6]
                slide = prs.slides.add_slide(blank_slide_layout)
                
                # Create and add relationship diagram
                diagram_path = await self.create_mermaid_diagram(df)
                
                # 检查是否是有效的图像文件
                is_image = diagram_path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))
                
                # Add the diagram to the slide with adjusted size and position
                left = Inches(0.25)
                top = Inches(0.75)
                width = Inches(9.5)
                
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
                    if diagram_path.lower().endswith('.txt') and os.path.exists(diagram_path):
                        with open(diagram_path, 'r') as f:
                            mermaid_content = f.read()
                            p = tf.add_paragraph()
                            p.text = "Mermaid定义（供参考）："
                            p.font.size = Pt(12)
                            p.font.bold = True
                            
                            p = tf.add_paragraph()
                            p.text = mermaid_content
                            p.font.size = Pt(8)
                
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
            import traceback
            print(f"Error exporting to PowerPoint: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            
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
            
            # 如果数据中有content字段并且有表关系信息，尝试生成关系图并添加
            has_relationship_data = False
            for item in data:
                content = item.get('content', '')
                if content and isinstance(content, str) and "表" in content and "关联到表" in content:
                    has_relationship_data = True
                    break
            
            if has_relationship_data:
                try:
                    # 创建一个临时DataFrame用于图表生成
                    import pandas as pd
                    temp_df = pd.DataFrame(data)
                    
                    # 添加diagram slide
                    blank_slide_layout = prs.slide_layouts[6]
                    slide = prs.slides.add_slide(blank_slide_layout)
                    
                    # Create and add relationship diagram
                    diagram_path = await self.create_mermaid_diagram(temp_df)
                    
                    # 检查是否是有效的图像文件
                    is_image = diagram_path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp'))
                    
                    # Add the diagram to the slide with adjusted size and position
                    left = Inches(0.25)
                    top = Inches(0.75)
                    width = Inches(9.5)
                    
                    if is_image and os.path.exists(diagram_path) and os.path.getsize(diagram_path) > 1000:
                        slide.shapes.add_picture(diagram_path, left, top, width=width)
                    else:
                        # 如果图像生成失败，添加一个文本框作为替代
                        txt_box = slide.shapes.add_textbox(left, top, width, Inches(5))
                        tf = txt_box.text_frame
                        p = tf.add_paragraph()
                        p.text = "无法生成关系图。请查看详细信息部分。"
                        p.font.size = Pt(14)
                        p.font.bold = True
                        
                        # 如果是文本文件，读取内容并显示
                        if diagram_path.lower().endswith('.txt') and os.path.exists(diagram_path):
                            with open(diagram_path, 'r') as f:
                                mermaid_content = f.read()
                                p = tf.add_paragraph()
                                p.text = "Mermaid定义（供参考）："
                                p.font.size = Pt(12)
                                p.font.bold = True
                                
                                p = tf.add_paragraph()
                                p.text = mermaid_content
                                p.font.size = Pt(8)
                    
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
                    print(f"Continuing with presentation without additional diagram")
            
            # Add slides for each data item with proper content handling
            for i, item in enumerate(data):
                content = item.get('content', '')
                source = item.get('source', 'search_result')
                title_text = item.get('title', f"{source.capitalize()} Results")
                
                if not content:
                    continue
                
                # Handle large content by breaking it into chunks
                # Calculate roughly how many characters can fit on a slide
                # This is a rough estimate and may need adjustment
                chars_per_slide = 1000
                
                # Break content into chunks if it's a long string
                if isinstance(content, str) and len(content) > chars_per_slide:
                    # Split content into multiple slides
                    chunks = [content[i:i+chars_per_slide] for i in range(0, len(content), chars_per_slide)]
                    
                    # Add a slide for each chunk
                    for chunk_index, chunk in enumerate(chunks):
                        slide_layout = prs.slide_layouts[5]  # Title and Content layout
                        slide = prs.slides.add_slide(slide_layout)
                        
                        # Set slide title
                        title = slide.shapes.title
                        if len(chunks) > 1:
                            title.text = f"{title_text} (Part {chunk_index+1}/{len(chunks)})"
                        else:
                            title.text = title_text
                        
                        # Add content
                        left = Inches(0.5)
                        top = Inches(1.5)
                        width = Inches(9.0)
                        height = Inches(5.0)
                        
                        txBox = slide.shapes.add_textbox(left, top, width, height)
                        tf = txBox.text_frame
                        tf.word_wrap = True
                        
                        # Add chunk text
                        p = tf.add_paragraph()
                        p.text = chunk
                        p.font.size = Pt(12)
                else:
                    # For shorter content, just add one slide
                    slide_layout = prs.slide_layouts[5]  # Title and Content layout
                    slide = prs.slides.add_slide(slide_layout)
                    
                    # Set slide title
                    title = slide.shapes.title
                    title.text = title_text
                    
                    # Add content
                    left = Inches(0.5)
                    top = Inches(1.5)
                    width = Inches(9.0)
                    height = Inches(5.0)
                    
                    txBox = slide.shapes.add_textbox(left, top, width, height)
                    tf = txBox.text_frame
                    tf.word_wrap = True
                    
                    # Add content text
                    p = tf.add_paragraph()
                    
                    # Convert non-string content to string
                    if not isinstance(content, str):
                        import json
                        try:
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
            import traceback
            print(f"Error appending to PowerPoint: {str(e)}")
            print(f"Detailed error: {traceback.format_exc()}")
            # 返回原始文件路径，确保处理继续
            return ppt_file 