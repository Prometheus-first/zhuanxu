#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用户数据查询脚本
从MongoDB数据库中查询指定日期范围内的用户信息
"""

import pymongo
from datetime import datetime, timedelta
import pandas as pd

class UserDataQuery:
    def __init__(self):
        self.init_mongodb()
    
    def init_mongodb(self):
        try:
            self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            self.mydb = self.myclient["留存"]
            self.mycol_raw = self.mydb["原始数据"]
            self.mycol_retention = self.mydb["用户日活跃"]
            print("✅ MongoDB连接成功")
        except Exception as e:
            print(f"❌ MongoDB连接失败: {e}")
            self.myclient = None
    
    def query_users_by_date_range(self, start_date, end_date):
        """
        查询指定日期范围内的用户活跃数据
        :param start_date: 开始日期，格式：'2025-07-22' 或 datetime对象
        :param end_date: 结束日期，格式：'2025-07-22' 或 datetime对象
        :return: 用户数据列表
        """
        if not self.myclient:
            print("❌ 数据库未连接")
            return []
        
        try:
            # 处理日期格式
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                start_date = start_date.strftime('%Y-%m-%d')
                
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                end_date = end_date.strftime('%Y-%m-%d')
            
            # 构建查询条件
            query = {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
            
            # 查询数据
            results = list(self.mycol_retention.find(query).sort("date", 1))
            
            print(f"📊 查询结果: {len(results)} 条用户活跃记录")
            print(f"📅 日期范围: {start_date} 到 {end_date}")
            
            return results
            
        except Exception as e:
            print(f"❌ 查询失败: {e}")
            return []
    
    def query_users_by_single_date(self, date):
        """
        查询指定单日的用户活跃数据
        :param date: 日期，格式：'2025-07-22'
        :return: 用户数据列表
        """
        if not self.myclient:
            print("❌ 数据库未连接")
            return []
        
        try:
            if isinstance(date, str):
                date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                date = date.strftime('%Y-%m-%d')
            
            query = {"date": date}
            results = list(self.mycol_retention.find(query).sort("usage_count", -1))
            
            print(f"📊 {date} 活跃用户: {len(results)} 个")
            
            return results
            
        except Exception as e:
            print(f"❌ 查询失败: {e}")
            return []
    
    def query_user_history(self, nickname):
        """
        查询指定用户的历史活跃记录
        :param nickname: 用户昵称
        :return: 用户历史数据列表4
        """
        if not self.myclient:
            print("❌ 数据库未连接")
            return []
        
        try:
            query = {"nickname": nickname}
            results = list(self.mycol_retention.find(query).sort("date", 1))
            
            print(f"📊 用户 {nickname} 的历史记录: {len(results)} 条")
            
            return results
            
        except Exception as e:
            print(f"❌ 查询失败: {e}")
            return []
    
    def get_active_users_summary(self, start_date, end_date):
        """
        获取指定日期范围内的用户活跃汇总统计
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 统计结果
        """
        users_data = self.query_users_by_date_range(start_date, end_date)
        
        if not users_data:
            return {}
        
        # 统计分析
        total_records = len(users_data)
        unique_users = len(set([user['nickname'] for user in users_data]))
        unique_dates = len(set([user['date'] for user in users_data]))
        
        total_usage = sum([user['usage_count'] for user in users_data])
        total_success = sum([user['success_count'] for user in users_data])
        total_fail = sum([user['fail_count'] for user in users_data])
        
        # 按用户统计
        user_stats = {}
        for user in users_data:
            nickname = user['nickname']
            if nickname not in user_stats:
                user_stats[nickname] = {
                    'active_days': 0,
                    'total_usage': 0,
                    'total_success': 0,
                    'total_fail': 0
                }
            user_stats[nickname]['active_days'] += 1
            user_stats[nickname]['total_usage'] += user['usage_count']
            user_stats[nickname]['total_success'] += user['success_count']
            user_stats[nickname]['total_fail'] += user['fail_count']
        
        # 按日期统计
        date_stats = {}
        for user in users_data:
            date = user['date']
            if date not in date_stats:
                date_stats[date] = {
                    'active_users': 0,
                    'total_usage': 0
                }
            date_stats[date]['active_users'] += 1
            date_stats[date]['total_usage'] += user['usage_count']
        
        summary = {
            'period': f"{start_date} 到 {end_date}",
            'total_records': total_records,
            'unique_users': unique_users,
            'unique_dates': unique_dates,
            'total_usage': total_usage,
            'total_success': total_success,
            'total_fail': total_fail,
            'success_rate': f"{(total_success/total_usage*100):.1f}%" if total_usage > 0 else "0%",
            'user_stats': user_stats,
            'date_stats': date_stats
        }
        
        return summary
    
    def export_to_csv(self, data, filename):
        """
        导出数据到CSV文件
        :param data: 数据列表
        :param filename: 文件名
        """
        try:
            if not data:
                print("❌ 没有数据可导出")
                return
            
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✅ 数据已导出到: {filename}")
            print(f"📄 导出了 {len(data)} 条记录")
            
        except Exception as e:
            print(f"❌ 导出失败: {e}")
    
    def print_user_data(self, users_data, limit=10):
        """
        打印用户数据
        :param users_data: 用户数据列表
        :param limit: 显示条数限制
        """
        if not users_data:
            print("❌ 没有数据可显示")
            return
        
        print(f"\n📋 用户活跃数据 (显示前{min(limit, len(users_data))}条):")
        print("-" * 80)
        
        for i, user in enumerate(users_data[:limit], 1):
            print(f"\n{i}. 用户: {user.get('nickname')}")
            print(f"   日期: {user.get('date')}")
            print(f"   使用次数: {user.get('usage_count')}")
            print(f"   成功次数: {user.get('success_count')}")
            print(f"   失败次数: {user.get('fail_count')}")
            print(f"   首次使用: {user.get('first_usage_time')}")
            print(f"   最后使用: {user.get('last_usage_time')}")
            print(f"   使用模型: {user.get('models_used')}")
            print(f"   平均处理时长: {user.get('avg_processing_minutes')} 分钟")
        
        if len(users_data) > limit:
            print(f"\n... 还有 {len(users_data) - limit} 条记录未显示")
    
    def print_summary(self, summary):
        """
        打印汇总统计信息
        :param summary: 汇总数据
        """
        if not summary:
            print("❌ 没有汇总数据")
            return
        
        print(f"\n📊 用户活跃汇总统计")
        print("=" * 60)
        print(f"📅 统计期间: {summary['period']}")
        print(f"📈 总记录数: {summary['total_records']}")
        print(f"👥 独立用户数: {summary['unique_users']}")
        print(f"📅 活跃天数: {summary['unique_dates']}")
        print(f"🎯 总使用次数: {summary['total_usage']}")
        print(f"✅ 成功次数: {summary['total_success']}")
        print(f"❌ 失败次数: {summary['total_fail']}")
        print(f"📊 成功率: {summary['success_rate']}")
        
        print(f"\n👥 用户排行榜 (Top 10):")
        user_ranking = sorted(summary['user_stats'].items(), 
                            key=lambda x: x[1]['total_usage'], reverse=True)[:10]
        for i, (nickname, stats) in enumerate(user_ranking, 1):
            success_rate = f"{(stats['total_success']/stats['total_usage']*100):.1f}%" if stats['total_usage'] > 0 else "0%"
            print(f"  {i:2d}. {nickname}: {stats['total_usage']}次使用, {stats['active_days']}天活跃, 成功率{success_rate}")
        
        print(f"\n📅 每日活跃统计:")
        for date, stats in sorted(summary['date_stats'].items()):
            print(f"  {date}: {stats['active_users']}个用户, {stats['total_usage']}次使用")

def main():
    """主程序"""
    query_tool = UserDataQuery()
    
    if not query_tool.myclient:
        print("❌ 无法连接数据库，程序退出")
        return
    
    print("\n" + "="*60)
    print("🔍 用户数据查询工具")
    print("="*60)
    
    print("\n请选择查询模式:")
    print("1. 按日期范围查询")
    print("2. 按单日查询")
    print("3. 按用户查询历史")
    print("4. 获取活跃汇总统计")
    
    try:
        choice = input("\n请输入选择 (1/2/3/4): ").strip()
        
        if choice == "1":
            print("\n📅 按日期范围查询")
            start_date = input("请输入开始日期 (格式: 2025-07-22): ").strip()
            end_date = input("请输入结束日期 (格式: 2025-07-22): ").strip()
            
            users_data = query_tool.query_users_by_date_range(start_date, end_date)
            query_tool.print_user_data(users_data)
            
            if users_data:
                export = input("\n是否导出到CSV文件? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"用户数据_{start_date}_到_{end_date}.csv"
                    query_tool.export_to_csv(users_data, filename)
        
        elif choice == "2":
            print("\n📅 按单日查询")
            date = input("请输入日期 (格式: 2025-07-22): ").strip()
            
            users_data = query_tool.query_users_by_single_date(date)
            query_tool.print_user_data(users_data)
            
            if users_data:
                export = input("\n是否导出到CSV文件? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"用户数据_{date}.csv"
                    query_tool.export_to_csv(users_data, filename)
        
        elif choice == "3":
            print("\n👤 按用户查询历史")
            nickname = input("请输入用户昵称: ").strip()
            
            users_data = query_tool.query_user_history(nickname)
            query_tool.print_user_data(users_data, limit=50)
            
            if users_data:
                export = input("\n是否导出到CSV文件? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"用户历史_{nickname}.csv"
                    query_tool.export_to_csv(users_data, filename)
        
        elif choice == "4":
            print("\n📊 获取活跃汇总统计")
            start_date = input("请输入开始日期 (格式: 2025-07-22): ").strip()
            end_date = input("请输入结束日期 (格式: 2025-07-22): ").strip()
            
            summary = query_tool.get_active_users_summary(start_date, end_date)
            query_tool.print_summary(summary)
        
        else:
            print("❌ 无效选择")
    
    except KeyboardInterrupt:
        print("\n\n👋 用户取消操作")
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
    
    print("\n" + "="*60)
    print("✅ 查询完成")
    print("="*60)

if __name__ == "__main__":
    main()
