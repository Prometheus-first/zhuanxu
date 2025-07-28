#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试进程清理功能
"""
import time
import psutil
from services.video_data_collector import VideoDataCollector

def print_chrome_processes():
    """打印当前的Chrome进程"""
    chrome_procs = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and ('chrome' in proc.info['name'].lower() or 'chromium' in proc.info['name'].lower()):
                chrome_procs.append(f"PID: {proc.info['pid']}, Name: {proc.info['name']}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if chrome_procs:
        print("当前Chrome进程:")
        for proc in chrome_procs:
            print(f"  {proc}")
    else:
        print("未找到Chrome进程")

def test_cleanup():
    """测试清理功能"""
    print("=== 测试进程清理功能 ===")
    
    print("\n1. 启动前的Chrome进程:")
    print_chrome_processes()
    
    print("\n2. 启动VideoDataCollector...")
    collector = VideoDataCollector()
    
    print("\n3. 连接浏览器...")
    if collector.connect():
        print("浏览器连接成功")
        
        print("\n4. 启动后的Chrome进程:")
        print_chrome_processes()
        
        print("\n5. 等待5秒...")
        time.sleep(5)
        
        print("\n6. 执行清理...")
        collector.cleanup()
        
        print("\n7. 清理后的Chrome进程:")
        print_chrome_processes()
        
        print("\n8. 强制清理...")
        collector.force_cleanup()
        
        print("\n9. 强制清理后的Chrome进程:")
        print_chrome_processes()
        
    else:
        print("浏览器连接失败")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    test_cleanup()