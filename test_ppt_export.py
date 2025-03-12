import asyncio
from service.export_ppt_service import ExportPPTService

async def main():
    service = ExportPPTService()
    result = await service.create_ppt(
        excel_file="relationship.xlsx", 
        output_file="test_relationship_diagram.pptx"
    )
    print(f"PPT创建{'成功' if result else '失败'}")

if __name__ == "__main__":
    asyncio.run(main())
