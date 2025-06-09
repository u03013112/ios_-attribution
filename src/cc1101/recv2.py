import time
import spidev
import RPi.GPIO as GPIO
import sys

class CC1101Receiver:
    def __init__(self, frequency=433):
        # 初始化GPIO
        GPIO.setwarnings(False)
        GPIO.setmode(GPIO.BCM)
        
        # 关键硬件配置
        self.RESET_PIN = 25  # GPIO25用于硬件复位
        self.GDO0_PIN = 22   # GPIO22用于监测GDO0状态
        GPIO.setup(self.RESET_PIN, GPIO.OUT)
        GPIO.setup(self.GDO0_PIN, GPIO.IN)
        
        # 初始化SPI接口
        self.spi = spidev.SpiDev()
        self.spi.open(0, 0)
        self.spi.mode = 0b00
        self.spi.max_speed_hz = 200000
        
        # 执行深度硬件复位
        self.perform_deep_reset()

        # 初始化芯片
        self.init_chip()

        # 设置频率
        self.set_frequency(frequency)

    def perform_deep_reset(self):
        GPIO.output(self.RESET_PIN, GPIO.HIGH)
        time.sleep(0.01)
        GPIO.output(self.RESET_PIN, GPIO.LOW)
        time.sleep(0.01)
        GPIO.output(self.RESET_PIN, GPIO.HIGH)
        time.sleep(0.01)

    def set_frequency(self, frequency):
        if frequency == 433:
            # 设置为433 MHz
            freq_regs = [(0x0D, 0x5D), (0x0E, 0x90), (0x0F, 0x00)]
        elif frequency == 315:
            # 设置为315 MHz
            freq_regs = [(0x0D, 0x21), (0x0E, 0x62), (0x0F, 0x00)]
        else:
            raise ValueError("Unsupported frequency. Use 433 or 315 MHz.")

        for reg, value in freq_regs:
            self.spi.xfer2([reg, value])
            time.sleep(0.01)

    def init_chip(self):
        # 标准初始化寄存器设置
        init_regs = [
            (0x00, 0x06),   # IOCFG2：配置 GDO2 引脚
            (0x01, 0x0D),   # IOCFG1：配置 GDO1 引脚
            (0x17, 0xD3),   # MCSM1：RX结束后进入IDLE
            (0x18, 0x18),   # MCSM0：启用自动校准
            (0x0B, 0x05),   # FSCTRL1：频率合成器设置
            (0x19, 0x16),   # FOCCFG：频率偏移补偿
            (0x1B, 0x1B),   # AGCCTRL2：自动增益控制
        ]

        for reg, value in init_regs:
            self.spi.xfer2([reg, value])
            time.sleep(0.01)

    def receive_data(self):
        print("[接收] 等待数据...")
        while True:
            # 进入接收模式
            self.spi.xfer2([0x34])  # SRX
            time.sleep(0.1)
            
            # 检查GDO0引脚以确定是否有数据可读
            if GPIO.input(self.GDO0_PIN) == 1:
                # 读取数据长度
                length = self.spi.xfer2([0x80 | 0x3F, 0])[1]
                
                # 读取数据
                data = self.spi.xfer2([0x80 | 0x3F] + [0x00] * length)[1:]
                print(f"[接收] 数据: {data}")
                
                # 回到空闲状态
                self.spi.xfer2([0x36])  # SIDLE
                time.sleep(0.1)

if __name__ == "__main__":
    frequency = 433
    if len(sys.argv) > 1:
        frequency = int(sys.argv[1])

    print(f"\n=== CC1101 接收程序，频率: {frequency} MHz ===")
    receiver = CC1101Receiver(frequency)
    try:
        receiver.receive_data()
    except KeyboardInterrupt:
        print("\n[退出] 清理GPIO")
        GPIO.cleanup()
