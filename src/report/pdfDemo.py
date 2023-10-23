import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PIL import Image as PilImage


font_path = '/src/data/zk2/MaShanZheng-Regular.ttf'
pdfmetrics.registerFont(TTFont('ChineseFont', font_path))


# 创建PDF文档
doc = SimpleDocTemplate("/src/data/zk2/report_demo.pdf", pagesize=letter)
# 添加文本
styles = getSampleStyleSheet()
styles['Heading1'].fontName = 'ChineseFont'
styles['Normal'].fontName = 'ChineseFont'
text = "数据分析报告"
paragraph = Paragraph(text, styles['Heading1'])

# 读取CSV文件并创建表格
data = pd.read_csv("/src/data/zk2/main1.csv")
table_data = [data.columns.to_list()] + data.values.tolist()
table = Table(table_data)
table.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
    ('GRID', (0, 0), (-1, -1), 1, colors.black)
]))

# 获取图片原始尺寸
pil_image = PilImage.open("/src/data/zk2/sunspots.png")
original_width, original_height = pil_image.size
print(pil_image.size)

# 计算新的高度
page_width, page_height = letter
new_width = page_width
new_height = original_height * (new_width / original_width)

# 创建Image对象
image = Image("/src/data/zk2/sunspots.png", width=new_width, height=new_height)
print(new_width,new_height)

# 将元素添加到PDF文档
doc.build([paragraph, Spacer(1, 0.25 * inch), table, Spacer(1, 0.25 * inch), image])
