#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RTL433 信号监控脚本 - 基于时间的模式切换版
功能：
1. 启动rtl_433进程进行信号录制
2. 定期检查录制目录的新文件
3. 统计文件数量和大小
4. 发送通知到飞书webhook
5. 基于时间的双模式切换（高效30分钟/节能10分钟）
6. 工作时间控制（0:00-6:00休息）
7. 自动偏置电源管理
"""

import os
import time
import subprocess
import threading
import requests
import json
from datetime import datetime, time as dt_time
from pathlib import Path
import signal
import sys
import atexit

class RTL433Monitor:
    def __init__(self, work_dir="/home/u03013112/rtl433_monitor", webhook_url=None):
        self.work_dir = Path(work_dir)
        self.webhook_url = webhook_url
        self.rtl433_process = None
        self.known_files = set()  # 已知的文件集合
        self.total_files = 0
        self.total_size = 0
        self.running = True
        
        # 模式控制
        self.current_mode = "high_performance"  # high_performance 或 energy_saving
        self.mode_start_time = time.time()
        self.high_performance_duration = 30 * 60  # 高效模式30分钟
        self.energy_saving_duration = 10 * 60     # 节能模式10分钟
        self.is_working_hours = False
        
        # 工作时间设置 (24小时制)
        self.rest_start_hour = 1   # 0:00开始休息
        self.rest_end_hour = 6     # 6:00结束休息
        
        # 创建工作目录
        self.work_dir.mkdir(exist_ok=True)
        
        # 初始化已知文件
        self._scan_existing_files()
        
        # 注册退出时的清理函数
        atexit.register(self.cleanup_bias_tee)
        
    def cleanup_bias_tee(self):
        """程序退出时关闭偏置电源"""
        try:
            print(f"[{datetime.now()}] 正在关闭偏置电源...")
            subprocess.run(['rtl_biast', '-b', '0'], 
                         capture_output=True, timeout=5)
            print(f"[{datetime.now()}] 偏置电源已关闭")
        except Exception as e:
            print(f"[{datetime.now()}] 关闭偏置电源失败: {e}")

    def check_working_hours(self):
        """检查是否在工作时间"""
        current_time = datetime.now().time()
        current_hour = current_time.hour
        
        # 判断是否在休息时间 (0:00-6:00)
        if self.rest_start_hour <= current_hour < self.rest_end_hour:
            return False
        return True

    def sendMessageToWebhook2(self, title, text, aText="", aUrl="", webhook=None):
        """发送消息到飞书webhook"""
        if not webhook:
            webhook = self.webhook_url
            
        if not webhook:
            print(f"[{datetime.now()}] Webhook URL not set, skipping notification")
            return
            
        # 在消息中添加当前模式信息
        mode_emoji = "🚀" if self.current_mode == "high_performance" else "🧊"
        mode_text = "高效模式" if self.current_mode == "high_performance" else "节能模式"
        mode_info = f"\n⚙️ 当前模式: {mode_text} {mode_emoji}"
        
        # 添加工作状态信息
        work_status = "工作中" if self.is_working_hours else "休息中"
        work_info = f"\n⏰ 工作状态: {work_status}"
        
        text_with_info = text + mode_info + work_info
            
        url = webhook
        headers = {'Content-Type': 'application/json'}
        
        content = [{
            "tag": "text",
            "text": text_with_info
        }]
        
        if aText and aUrl:
            content.append({
                "tag": "a", 
                "text": aText,
                "href": aUrl
            })
            
        data = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": [content]
                    }
                }
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=data, timeout=10)
            if response.status_code != 200:
                print(f"[{datetime.now()}] Webhook Error: {response.status_code}, {response.text}")
            else:
                print(f"[{datetime.now()}] Notification sent successfully")
        except Exception as e:
            print(f"[{datetime.now()}] Webhook Exception: {e}")
    
    def _scan_existing_files(self):
        """扫描现有文件，初始化统计"""
        cu8_files = list(self.work_dir.glob("g*.cu8"))
        for file_path in cu8_files:
            self.known_files.add(file_path.name)
            self.total_size += file_path.stat().st_size
        
        self.total_files = len(cu8_files)
        print(f"[{datetime.now()}] 初始化完成: {self.total_files} 个文件, "
              f"总大小: {self._format_size(self.total_size)}")
    
    def _format_size(self, size_bytes):
        """格式化文件大小"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.2f} {size_names[i]}"
    
    def get_rtl433_command(self, mode="high_performance"):
        """根据模式获取rtl433命令"""
        base_cmd = [
            "rtl_433",
            "-f", "433920000",
            "-s", "250000",  # 固定250kHz采样率
            "-S", "all",
            "-M", "time",
            "-M", "level"
        ]
        
        if mode == "high_performance":
            # 高效模式：启用偏置电源
            cmd = base_cmd + [
                "-t", "biastee=1,offset_tune=1",
                "-g", "49.6",
                "-Y", "level=-25"
            ]
        else:  # energy_saving mode
            # 节能模式：只关闭偏置电源，其他参数相同
            cmd = base_cmd + [
                "-t", "offset_tune=1",
                "-g", "49.6",
                "-Y", "level=-25"
            ]
        
        return cmd
    
    def start_rtl433(self, mode=None):
        """启动rtl_433进程"""
        if mode is None:
            mode = self.current_mode
            
        cmd = self.get_rtl433_command(mode)
        
        try:
            # 切换到工作目录
            os.chdir(self.work_dir)
            
            mode_text = "高效模式" if mode == "high_performance" else "节能模式"
            print(f"[{datetime.now()}] 启动RTL433监听 ({mode_text})...")
            print(f"[{datetime.now()}] 工作目录: {self.work_dir}")
            print(f"[{datetime.now()}] 命令: {' '.join(cmd)}")
            
            # 启动进程，重定向输出到日志文件
            log_file = self.work_dir / f"rtl433_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{mode}.log"
            with open(log_file, 'w') as f:
                self.rtl433_process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=self.work_dir
                )
            
            self.current_mode = mode
            self.mode_start_time = time.time()
            
            # 发送启动通知
            bias_status = "开启" if mode == "high_performance" else "关闭"
            
            self.sendMessageToWebhook2(
                f"🎯 RTL433监听已启动 ({mode_text})",
                f"监听频率: 433.92MHz\n"
                f"采样率: 250kHz\n"
                f"工作目录: {self.work_dir}\n"
                f"日志文件: {log_file.name}\n"
                f"偏置电源: {bias_status}",
                "查看详情", 
                f"file://{log_file}"
            )
            
            return True
            
        except Exception as e:
            print(f"[{datetime.now()}] 启动RTL433失败: {e}")
            self.sendMessageToWebhook2(
                "❌ RTL433启动失败",
                f"错误信息: {str(e)}"
            )
            return False
    
    def should_switch_mode(self):
        """检查是否应该切换模式"""
        current_time = time.time()
        running_time = current_time - self.mode_start_time
        
        if self.current_mode == "high_performance":
            # 高效模式运行30分钟后切换到节能模式
            return running_time >= self.high_performance_duration
        else:
            # 节能模式运行10分钟后切换到高效模式
            return running_time >= self.energy_saving_duration
    
    def switch_mode(self):
        """切换工作模式"""
        old_mode = self.current_mode
        new_mode = "energy_saving" if old_mode == "high_performance" else "high_performance"
        
        running_time = time.time() - self.mode_start_time
        running_minutes = int(running_time / 60)
        
        print(f"[{datetime.now()}] 切换模式: {old_mode} -> {new_mode}, 运行时间: {running_minutes}分钟")
        
        # 停止当前进程
        if self.rtl433_process:
            self.rtl433_process.terminate()
            try:
                self.rtl433_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.rtl433_process.kill()
        
        # 如果从高效模式切换到节能模式，需要关闭偏置电源
        if old_mode == "high_performance" and new_mode == "energy_saving":
            self.cleanup_bias_tee()
            time.sleep(1)  # 等待1秒
        
        # 启动新模式
        if self.start_rtl433(new_mode):
            old_mode_text = "高效模式" if old_mode == "high_performance" else "节能模式"
            new_mode_text = "高效模式" if new_mode == "high_performance" else "节能模式"
            
            next_switch_minutes = 30 if new_mode == "high_performance" else 10
            
            self.sendMessageToWebhook2(
                f"🔄 模式切换: {old_mode_text} → {new_mode_text}",
                f"上一模式运行时间: {running_minutes}分钟\n"
                f"新模式将运行: {next_switch_minutes}分钟\n"
                f"切换时间: {datetime.now().strftime('%H:%M:%S')}"
            )
    
    def check_working_hours_and_control(self):
        """检查工作时间并控制进程"""
        should_work = self.check_working_hours()
        if should_work and not self.is_working_hours:
            # 从休息转为工作
            self.is_working_hours = True
            
            self.sendMessageToWebhook2(
                "⏰ 工作时间开始",
                f"当前时间: {datetime.now().strftime('%H:%M:%S')}\n"
                f"开始工作，启动信号监听\n"
                f"工作时间: 6:00-24:00"
            )
            
            # 启动rtl433（默认高效模式）
            self.current_mode = "high_performance"
            self.start_rtl433()
            
        elif not should_work and self.is_working_hours:
            # 从工作转为休息
            self.is_working_hours = False
            
            # 停止rtl433进程
            if self.rtl433_process:
                self.rtl433_process.terminate()
                try:
                    self.rtl433_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.rtl433_process.kill()
                self.rtl433_process = None
            
            # 关闭偏置电源
            self.cleanup_bias_tee()
            
            self.sendMessageToWebhook2(
                "😴 进入休息时间",
                f"当前时间: {datetime.now().strftime('%H:%M:%S')}\n"
                f"停止工作，进入休息模式\n"
                f"休息时间: 0:00-6:00\n"
                f"偏置电源已关闭"
            )
    
    def check_new_files(self):
        """检查新文件"""
        try:
            current_files = set()
            current_total_size = 0
            
            # 扫描当前所有cu8文件
            cu8_files = list(self.work_dir.glob("g*.cu8"))
            
            for file_path in cu8_files:
                current_files.add(file_path.name)
                current_total_size += file_path.stat().st_size
            
            # 找出新文件
            new_files = current_files - self.known_files
            
            if new_files:
                new_files_info = []
                new_files_size = 0
                
                for filename in sorted(new_files):
                    file_path = self.work_dir / filename
                    file_size = file_path.stat().st_size
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    new_files_info.append({
                        'name': filename,
                        'size': file_size,
                        'time': file_time
                    })
                    new_files_size += file_size
                
                # 更新统计
                self.known_files.update(new_files)
                old_total_files = self.total_files
                old_total_size = self.total_size
                
                self.total_files = len(current_files)
                self.total_size = current_total_size
                
                # 构建通知消息
                files_text = "\n".join([
                    f"📁 {info['name']} ({self._format_size(info['size'])}) - {info['time'].strftime('%H:%M:%S')}"
                    for info in new_files_info
                ])
                
                message = (f"🆕 发现 {len(new_files)} 个新信号文件:\n\n"
                          f"{files_text}\n\n"
                          f"📊 统计信息:\n"
                          f"• 新增文件: {len(new_files)} 个\n"
                          f"• 新增大小: {self._format_size(new_files_size)}\n"
                          f"• 总文件数: {old_total_files} → {self.total_files}\n"
                          f"• 总大小: {self._format_size(old_total_size)} → {self._format_size(self.total_size)}")
                
                print(f"[{datetime.now()}] {message}")
                
                # 发送通知
                self.sendMessageToWebhook2(
                    "📡 新信号检测",
                    message,
                    "查看目录",
                    f"file://{self.work_dir}"
                )
                
        except Exception as e:
            print(f"[{datetime.now()}] 检查文件时出错: {e}")
    
    def send_status_report(self):
        """发送状态报告"""
        # 检查rtl433进程状态
        if self.rtl433_process:
            if self.rtl433_process.poll() is None:
                process_status = "🟢 运行中"
            else:
                process_status = f"🔴 已停止 (退出码: {self.rtl433_process.returncode})"
        else:
            if self.is_working_hours:
                process_status = "⚪ 未启动"
            else:
                process_status = "😴 休息中"
        
        # 磁盘使用情况
        disk_usage = os.statvfs(self.work_dir)
        free_space = disk_usage.f_frsize * disk_usage.f_available
        
        # 工作状态和模式信息
        work_status = "工作中" if self.is_working_hours else "休息中 (0:00-6:00)"
        mode_text = "高效模式" if self.current_mode == "high_performance" else "节能模式"
        
        # 计算当前模式剩余时间
        if self.is_working_hours and self.rtl433_process:
            running_time = time.time() - self.mode_start_time
            if self.current_mode == "high_performance":
                remaining_time = max(0, self.high_performance_duration - running_time)
            else:
                remaining_time = max(0, self.energy_saving_duration - running_time)
            remaining_minutes = int(remaining_time / 60)
            mode_info = f"{mode_text} (剩余{remaining_minutes}分钟)"
        else:
            mode_info = mode_text
        
        message = (f"📈 RTL433监控状态报告\n\n"
                  f"🔧 进程状态: {process_status}\n"
                  f"⏰ 工作状态: {work_status}\n"
                  f"⚙️ 当前模式: {mode_info}\n"
                  f"📁 工作目录: {self.work_dir}\n"
                  f"📊 已捕获: {self.total_files} 个信号文件\n"
                  f"💾 占用空间: {self._format_size(self.total_size)}\n"
                  f"🗄️ 剩余空间: {self._format_size(free_space)}\n"
                  f"⏰ 报告时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        self.sendMessageToWebhook2(
            "📊 RTL433状态报告",
            message
        )
        
        print(f"[{datetime.now()}] 状态报告已发送")
    
    def monitor_loop(self):
        """主监控循环"""
        last_status_time = time.time()
        status_interval = 3600  # 每小时发送一次状态报告
        
        # 初始检查工作时间
        self.check_working_hours_and_control()
        
        while self.running:
            try:
                # 检查工作时间
                self.check_working_hours_and_control()
                
                # 只在工作时间进行以下检查
                if self.is_working_hours:
                    # 检查是否需要切换模式
                    if self.should_switch_mode():
                        self.switch_mode()
                    
                    # 检查新文件
                    self.check_new_files()
                    
                    # 检查rtl433进程是否还在运行
                    if self.rtl433_process and self.rtl433_process.poll() is not None:
                        print(f"[{datetime.now()}] RTL433进程意外退出，尝试重启...")
                        self.sendMessageToWebhook2(
                            "⚠️ RTL433进程异常",
                            f"进程意外退出，退出码: {self.rtl433_process.returncode}\n正在尝试重启..."
                        )
                        time.sleep(5)
                        self.start_rtl433()
                
                # 检查是否需要发送状态报告
                current_time = time.time()
                if current_time - last_status_time >= status_interval:
                    self.send_status_report()
                    last_status_time = current_time
                
                # 等待30秒
                time.sleep(30)
                
            except KeyboardInterrupt:
                print(f"\n[{datetime.now()}] 收到中断信号，正在停止...")
                break
            except Exception as e:
                print(f"[{datetime.now()}] 监控循环出错: {e}")
                time.sleep(10)
    
    def stop(self):
        """停止监控"""
        self.running = False
        
        if self.rtl433_process:
            print(f"[{datetime.now()}] 正在停止RTL433进程...")
            self.rtl433_process.terminate()
            
            # 等待进程结束
            try:
                self.rtl433_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(f"[{datetime.now()}] 强制结束RTL433进程...")
                self.rtl433_process.kill()
        
        # 关闭偏置电源
        self.cleanup_bias_tee()
        
        # 发送停止通知
        self.sendMessageToWebhook2(
            "🛑 RTL433监控已停止",
            f"监控结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"总共捕获: {self.total_files} 个信号文件\n"
            f"总大小: {self._format_size(self.total_size)}\n"
            f"偏置电源已关闭"
        )
        
        print(f"[{datetime.now()}] 监控已停止")

def signal_handler(signum, frame):
    """处理系统信号"""
    print(f"\n[{datetime.now()}] 收到信号 {signum}，正在停止监控...")
    if 'monitor' in globals():
        monitor.stop()
    
    # 确保偏置电源关闭
    try:
        subprocess.run(['rtl_biast', '-b', '0'], capture_output=True, timeout=5)
        print("偏置电源已关闭")
    except:
        print("关闭偏置电源失败，请手动执行: rtl_biast -b 0")
    
    sys.exit(0)

def main():
    global monitor
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Webhook URL
    testWebhookUrl = 'https://open.feishu.cn/open-apis/bot/v2/hook/acceb43c-5da3-47a2-987f-fc7228449a9c'
    
    # 创建监控实例
    monitor = RTL433Monitor(
        work_dir="/home/u03013112/rtl433_monitor",
        webhook_url=testWebhookUrl
    )
    
    try:
        # 开始监控循环（会自动判断是否启动rtl433）
        monitor.monitor_loop()
            
    except Exception as e:
        print(f"程序异常: {e}")
        monitor.sendMessageToWebhook2(
            "❌ RTL433监控程序异常",
            f"异常信息: {str(e)}"
        )
    finally:
        monitor.stop()

if __name__ == "__main__":
    main()