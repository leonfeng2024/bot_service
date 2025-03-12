import pandas as pd
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
import os
from datetime import datetime
import requests
import base64
from io import BytesIO

class ExportPPTService:
    def __init__(self):
        self.temp_dir = "temp"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

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
      'nodeSpacing': 35,
      'rankSpacing': 35,
      'curve': 'linear',
      'nodeWidth': 160,
      'nodeHeight': 30,
      'edgeLengthFactor': '0.8',
      'arrowMarkerAbsolute': false,
      'htmlLabels': true
    },
    'themeVariables': {
      'fontSize': '13px',
      'fontFamily': 'Arial',
      'primaryColor': '#f4f4f4',
      'primaryTextColor': '#333',
      'primaryBorderColor': '#888',
      'lineColor': '#000',
      'secondaryColor': '#eee',
      'tertiaryColor': '#fff',
      'arrowheadSize': '1'
    }
  }
}%%
"""
        
        # Track unique nodes to avoid duplicates
        added_nodes = set()
        added_relationships = set()
        
        # Check the column names in the dataframe
        print(f"Available columns: {relationships_df.columns.tolist()}")
        
        # Identify the column names based on new format and provide fallbacks for backward compatibility
        table_col = 'Table' if 'Table' in relationships_df.columns else 'Source Table'
        rel_type_col = 'Relationship Type' 
        view_col = 'View' if 'View' in relationships_df.columns else 'Target Table'
        second_rel_type_col = 'Relationship Type.1' if 'Relationship Type.1' in relationships_df.columns else 'D'
        dataset_col = 'Dataset' if 'Dataset' in relationships_df.columns else 'Target Dataset'
        
        # If column names are numeric indices, try to map them
        if set(relationships_df.columns) == set(range(relationships_df.shape[1])):
            # Assuming the columns are in the order shown in the image
            table_col = 0  # Column A
            rel_type_col = 1      # Column B
            view_col = 2  # Column C
            second_rel_type_col = 3  # Column D
            dataset_col = 4   # Column E
        
        # Define node classes with explicit styles for fixing text position inside nodes
        mermaid_definition += """
classDef tableNode fill:#d0d0d0,stroke:#666,stroke-width:1px,font-size:13px,text-align:center;
classDef viewNode fill:#ffeb99,stroke:#666,stroke-width:1px,font-size:13px,text-align:center;
classDef datasetNode fill:#e6ccff,stroke:#666,stroke-width:1px,font-size:13px,text-align:center;
linkStyle default stroke:#000,stroke-width:1.5px;
"""
        
        for _, row in relationships_df.iterrows():
            try:
                # First relationship: Table -> View
                table = str(row[table_col])
                rel_type = str(row[rel_type_col])
                view = str(row[view_col])
                
                # Second relationship: View -> Dataset
                second_rel_type = str(row[second_rel_type_col])
                dataset = str(row[dataset_col])
                
                # Skip empty rows
                if pd.isna(table) or table.strip() == "" or pd.isna(view) or view.strip() == "":
                    continue
                
                # Create sanitized node IDs (remove spaces and special characters)
                table_id = "".join(c if c.isalnum() else "_" for c in table)
                view_id = "".join(c if c.isalnum() else "_" for c in view)
                dataset_id = "".join(c if c.isalnum() else "_" for c in dataset)
                
                # Use rounded rectangle shape (stadium) for all nodes - ensure text is inside
                if table not in added_nodes:
                    # Using standard stadium shape syntax
                    mermaid_definition += f"    {table_id}([{table}])\n"
                    added_nodes.add(table)
                
                if view not in added_nodes:
                    mermaid_definition += f"    {view_id}([{view}])\n"
                    added_nodes.add(view)
                
                if dataset not in added_nodes and not pd.isna(dataset) and dataset.strip() != "":
                    mermaid_definition += f"    {dataset_id}([{dataset}])\n"
                    added_nodes.add(dataset)
                
                # Add relationship without text label
                rel_key = f"{table}-{rel_type}-{view}"
                if rel_key not in added_relationships:
                    mermaid_definition += f"    {table_id} --> {view_id}\n"
                    added_relationships.add(rel_key)
                
                if not pd.isna(dataset) and dataset.strip() != "":
                    dataset_rel_key = f"{view}-{second_rel_type}-{dataset}"
                    if dataset_rel_key not in added_relationships:
                        mermaid_definition += f"    {view_id} --> {dataset_id}\n"
                        added_relationships.add(dataset_rel_key)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        
        # Apply classes to nodes based on their type
        for node in added_nodes:
            node_id = "".join(c if c.isalnum() else "_" for c in node)
            
            # Check the node type based on your data patterns
            if any(node.startswith(prefix) for prefix in ['t_', 'table_']):
                # Tables (usually start with t_)
                mermaid_definition += f"    class {node_id} tableNode;\n"
            elif any(node.startswith(prefix) for prefix in ['v_', 'view_']):
                # Views (usually start with v_)
                mermaid_definition += f"    class {node_id} viewNode;\n"
            elif any(node.startswith(prefix) for prefix in ['d_', 'dataset_']):
                # Datasets (usually start with d_)
                mermaid_definition += f"    class {node_id} datasetNode;\n"
        
        print(f"Generated Mermaid definition:\n{mermaid_definition}")
        
        # Use the Mermaid.ink service to render the diagram
        mermaid_encoded = base64.urlsafe_b64encode(mermaid_definition.encode()).decode()
        
        # Add parameters for a more compact rendering and higher quality
        mermaid_url = f"https://mermaid.ink/img/{mermaid_encoded}?width=1200&height=800&dpi=200"
        
        # Download the image
        response = requests.get(mermaid_url)
        
        # Save the image
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_path = os.path.join(self.temp_dir, f"relationships_{timestamp}.png")
        
        with open(image_path, "wb") as f:
            f.write(response.content)
        
        return image_path

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
            diagram_path = await self.create_mermaid_diagram(df)
            
            # Add the diagram to the slide with adjusted size and position
            # Use most of the slide area
            left = Inches(0.25)  # Reduced margin
            top = Inches(0.75)   # Reduced top margin
            width = Inches(9.5)  # Almost full width
            slide.shapes.add_picture(diagram_path, left, top, width=width)
            
            # Add title to the diagram slide
            title_box = slide.shapes.add_textbox(left, Inches(0.1), width, Inches(0.5))
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
            title_box = slide.shapes.add_textbox(left, Inches(0.5), width, Inches(0.5))
            title_frame = title_box.text_frame
            title_para = title_frame.add_paragraph()
            title_para.text = "Relationship Details"
            title_para.alignment = PP_ALIGN.CENTER
            title_para.font.size = Pt(18)
            title_para.font.bold = True
            
            # Add table with relationship details
            rows = len(df) + 1
            cols = 5
            top = Inches(1)
            table_width = Inches(9.5)  # Wider table
            table_height = Inches(0.3 * rows)  # Shorter rows
            
            table = slide.shapes.add_table(rows, cols, left, top, table_width, table_height).table
            
            # Set column headers
            headers = ["Table", "First Relationship", "View", "Second Relationship", "Dataset"]
            for i, header in enumerate(headers):
                cell = table.cell(0, i)
                cell.text = header
                paragraph = cell.text_frame.paragraphs[0]
                paragraph.font.bold = True
                paragraph.font.size = Pt(11)  # Slightly smaller font
            
            # Determine column names
            table_col = 'Table' if 'Table' in df.columns else 'Source Table'
            rel_type_col = 'Relationship Type'
            view_col = 'View' if 'View' in df.columns else 'Target Table'
            second_rel_type_col = 'Relationship Type.1' if 'Relationship Type.1' in df.columns else 'D'
            dataset_col = 'Dataset' if 'Dataset' in df.columns else 'Target Dataset'
            
            # Fill table data
            for i, row in df.iterrows():
                # Check if we need to use indexed columns or named columns
                if isinstance(df.columns[0], int):
                    # Using numeric indices
                    table.cell(i + 1, 0).text = str(row[0])  # Table
                    table.cell(i + 1, 1).text = str(row[1])  # Relationship Type
                    table.cell(i + 1, 2).text = str(row[2])  # View
                    table.cell(i + 1, 3).text = str(row[3])  # Second Relationship Type
                    table.cell(i + 1, 4).text = str(row[4])  # Dataset
                else:
                    # Using named columns
                    table.cell(i + 1, 0).text = str(row[table_col])
                    table.cell(i + 1, 1).text = str(row[rel_type_col])
                    table.cell(i + 1, 2).text = str(row[view_col])
                    table.cell(i + 1, 3).text = str(row[second_rel_type_col])
                    table.cell(i + 1, 4).text = str(row[dataset_col])
                
                # Set smaller font for table data
                for col in range(5):
                    paragraph = table.cell(i + 1, col).text_frame.paragraphs[0]
                    paragraph.font.size = Pt(9)  # Smaller font for data
            
            # Save presentation
            prs.save(output_file)
            
            # Clean up temporary files
            if os.path.exists(diagram_path):
                os.remove(diagram_path)
            
            return True
            
        except Exception as e:
            print(f"Error creating PPT: {str(e)}")
            return False 