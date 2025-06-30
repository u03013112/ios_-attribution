import pigpio
import time

GDO2_PIN = 18  # GPIO18，根据你的连接

pi = pigpio.pi()
if not pi.connected:
    print("pigpio连接失败，请检查pigpiod是否启动")
    exit(0)

pi.set_mode(GDO2_PIN, pigpio.INPUT)

last_tick = None
pulse_times = []

def cbf(gpio, level, tick):
    global last_tick, pulse_times
    if last_tick is not None:
        pulse_length = pigpio.tickDiff(last_tick, tick)
        pulse_times.append((level, pulse_length))
        print(f"电平: {level}, 脉冲长度: {pulse_length} 微秒")
    last_tick = tick

cb = pi.callback(GDO2_PIN, pigpio.EITHER_EDGE, cbf)

print("开始捕获GDO2上的脉冲数据，按遥控器按钮发送信号...")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n停止捕获，清理资源")
    cb.cancel()
    pi.stop()
