import RPi.GPIO as GPIO
import spidev
import time

# 初始化GPIO和SPI
GPIO.setmode(GPIO.BCM)
CSn_PIN = 5  # 改用GPIO5控制CSn
GPIO.setup(CSn_PIN, GPIO.OUT)
GPIO.output(CSn_PIN, GPIO.HIGH)  # 初始化为未选中

spi = spidev.SpiDev()
spi.open(0, 1)  # 使用SPI总线0，设备1（CE1引脚）
spi.mode = 0b00  # SPI模式0
spi.max_speed_hz = 1000000  # 1MHz时钟

def test_csn():
    try:
        # 测试1：拉低CSn后读取PARTNUM寄存器（0x30）
        GPIO.output(CSn_PIN, GPIO.LOW)
        partnum = spi.xfer2([0x80 | 0x30, 0x00])[1]  # 读操作
        GPIO.output(CSn_PIN, GPIO.HIGH)
        print(f"[CSn测试] PARTNUM=0x{partnum:02X} (正常应为0x00或0x14)")

        # 测试2：检查CSn拉低期间的SCLK和MOSI信号
        GPIO.output(CSn_PIN, GPIO.LOW)
        spi.xfer2([0x3F, 0x00])  # 发送无效指令，观察逻辑分析仪波形
        GPIO.output(CSn_PIN, GPIO.HIGH)
        print("[CSn测试] 请用逻辑分析仪检查SCLK/MOSI时序")

    except Exception as e:
        print(f"[CSn测试] 异常: {e}")
    finally:
        spi.close()
        GPIO.cleanup()

test_csn()