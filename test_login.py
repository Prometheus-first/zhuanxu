#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试连接和登录过程
"""
import sys
import time
sys.path.append('.')

from services.video_data_collector import VideoDataCollector

def test_connection_and_login():
    """测试连接和登录"""
    print("=== 测试连接和登录过程 ===")
    
    def progress_callback(info):
        print(f"[{info['timestamp']}] {info['level'].upper()}: {info['message']}")
    
    collector = VideoDataCollector(progress_callback=progress_callback)
    
    print("\n1. 测试浏览器连接...")
    if collector.connect():
        print("浏览器连接成功")
        
        print("\n2. 测试系统登录...")
        if collector.login(username='admin', password='admin@liandanxia'):
            print("登录成功")
            print(f"获取到的token: {collector.backserver_token[:50] if collector.backserver_token else 'None'}...")
            
            print("\n3. 等待5秒后清理...")
            time.sleep(5)
            
        else:
            print("登录失败")
        
        print("\n4. 清理资源...")
        collector.cleanup()
        
    else:
        print("浏览器连接失败")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    try:
        test_connection_and_login()
    except KeyboardInterrupt:
        print("\n测试被用户中断")
    except Exception as e:
        print(f"测试出现异常: {e}")
        import traceback
        traceback.print_exc()