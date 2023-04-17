import qrcode

url_prefix = "HTTPS://M.NH73.CN/"

# 五个字符串
strings = [
    "6M1KZJYRZ74T8XO4U9E1",
    "GDT6W8U6K34O6J0U0ME1",
    "Z4X9X1O9V7W2O2K2ZE1",
    "5B5P5WZCJ0Z7X9L9CE1",
    "R8F6K7R2Q2A3D7Z3ME1"
]

# 生成二维码并保存为 PNG 文件
for i, s in enumerate(strings):
    url = url_prefix + s
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    img.save(f"qrcode_{i+1}.png")