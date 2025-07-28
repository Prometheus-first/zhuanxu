#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
视频工具活跃数据服务层
整合 yisen 文件夹下的视频数据查询逻辑
"""

import pymongo
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict

class VideoActiveService:
    def __init__(self):
        self.init_mongodb()
    
    def init_mongodb(self):
        try:
            self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
            self.mydb = self.myclient["留存"]
            self.mycol_raw = self.mydb["原始数据"]
            self.mycol_retention = self.mydb["用户日活跃"]
            print("MongoDB连接成功")
        except Exception as e:
            print(f"MongoDB连接失败: {e}")
            self.myclient = None
    
    def query_users_by_date_range(self, start_date, end_date):
        """查询指定日期范围内的用户活跃数据"""
        if not self.myclient:
            print("数据库未连接")
            return []
        
        try:
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                start_date = start_date.strftime('%Y-%m-%d')
                
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                end_date = end_date.strftime('%Y-%m-%d')
            
            query = {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
            
            results = list(self.mycol_retention.find(query, {'_id': 0}).sort("date", 1))
            
            print(f"查询结果: {len(results)} 条用户活跃记录")
            print(f"日期范围: {start_date} 到 {end_date}")
            
            return results
            
        except Exception as e:
            print(f"查询失败: {e}")
            return []
    
    def query_users_by_single_date(self, date):
        """查询指定单日的用户活跃数据"""
        if not self.myclient:
            print("数据库未连接")
            return []
        
        try:
            if isinstance(date, str):
                date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                date = date.strftime('%Y-%m-%d')
            
            query = {"date": date}
            results = list(self.mycol_retention.find(query, {'_id': 0}).sort("usage_count", -1))
            
            print(f"{date} 活跃用户: {len(results)} 个")
            
            return results
            
        except Exception as e:
            print(f"查询失败: {e}")
            return []
    
    def query_user_history(self, nickname):
        """查询指定用户的历史活跃记录"""
        if not self.myclient:
            print("数据库未连接")
            return []
        
        try:
            query = {"nickname": nickname}
            results = list(self.mycol_retention.find(query, {'_id': 0}).sort("date", 1))
            
            print(f"用户 {nickname} 历史记录: {len(results)} 条")
            
            return results
            
        except Exception as e:
            print(f"查询失败: {e}")
            return []
    
    def get_active_users_summary(self, start_date, end_date):
        """获取活跃用户汇总统计"""
        users_data = self.query_users_by_date_range(start_date, end_date)
        
        if not users_data:
            return {}
        
        total_records = len(users_data)
        unique_users = len(set([user['nickname'] for user in users_data]))
        unique_dates = len(set([user['date'] for user in users_data]))
        
        total_usage = sum([user.get('usage_count', 0) for user in users_data])
        total_success = sum([user.get('success_count', 0) for user in users_data])
        total_fail = sum([user.get('fail_count', 0) for user in users_data])
        
        # 计算总提示词长度
        total_prompt_length = sum([user.get('total_prompt_length', 0) for user in users_data])
        
        # 用户统计
        user_stats = {}
        for user in users_data:
            nickname = user['nickname']
            if nickname not in user_stats:
                user_stats[nickname] = {
                    'active_days': 0,
                    'total_usage': 0,
                    'total_success': 0,
                    'total_fail': 0,
                    'total_prompt_length': 0,
                    'avg_processing_minutes': 0
                }
            user_stats[nickname]['active_days'] += 1
            user_stats[nickname]['total_usage'] += user.get('usage_count', 0)
            user_stats[nickname]['total_success'] += user.get('success_count', 0)
            user_stats[nickname]['total_fail'] += user.get('fail_count', 0)
            user_stats[nickname]['total_prompt_length'] += user.get('total_prompt_length', 0)
            user_stats[nickname]['avg_processing_minutes'] += user.get('avg_processing_minutes', 0)
        
        # 日期统计
        date_stats = {}
        for user in users_data:
            date = user['date']
            if date not in date_stats:
                date_stats[date] = {
                    'active_users': 0,
                    'total_usage': 0
                }
            date_stats[date]['active_users'] += 1
            date_stats[date]['total_usage'] += user.get('usage_count', 0)
        
        summary = {
            'period': f"{start_date} 到 {end_date}",
            'total_records': total_records,
            'unique_users': unique_users,
            'unique_dates': unique_dates,
            'total_usage': total_usage,
            'total_success': total_success,
            'total_fail': total_fail,
            'total_prompt_length': total_prompt_length,
            'success_rate': f"{(total_success/total_usage*100):.1f}%" if total_usage > 0 else "0%",
            'user_stats': user_stats,
            'date_stats': date_stats
        }
        
        return summary
    
    def get_data_summary(self, start_date=None, end_date=None):
        """获取数据概览 - 最近7天或指定日期范围"""
        if not self.myclient:
            return {'message': '数据库未连接', 'total_records': 0}
        
        try:
            if not start_date or not end_date:
                # 默认查询最近7天
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
            
            query = {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
            
            total_records = self.mycol_retention.count_documents(query)
            
            if total_records == 0:
                return {'message': '暂无数据', 'total_records': 0}
            
            # 获取统计数据
            pipeline = [
                {"$match": query},
                {"$group": {
                    "_id": None,
                    "total_users": {"$addToSet": "$nickname"},
                    "total_usage": {"$sum": "$usage_count"},
                    "total_success": {"$sum": "$success_count"},
                    "total_fail": {"$sum": "$fail_count"},
                    "total_prompt_length": {"$sum": "$total_prompt_length"},
                    "unique_dates": {"$addToSet": "$date"}
                }}
            ]
            
            result = list(self.mycol_retention.aggregate(pipeline))
            
            if result:
                stats = result[0]
                total_users = len(stats['total_users'])
                unique_dates = len(stats['unique_dates'])
                total_usage = stats['total_usage']
                total_success = stats['total_success']
                total_fail = stats['total_fail']
                total_prompt_length = stats['total_prompt_length']
                success_rate = f"{(total_success/total_usage*100):.1f}%" if total_usage > 0 else "0%"
            else:
                total_users = unique_dates = total_usage = total_success = total_fail = total_prompt_length = 0
                success_rate = "0%"
            
            return {
                'period': f"{start_date} 到 {end_date}",
                'total_records': total_records,
                'unique_users': total_users,
                'unique_dates': unique_dates,
                'total_usage': total_usage,
                'total_success': total_success,
                'total_fail': total_fail,
                'total_prompt_length': total_prompt_length,
                'success_rate': success_rate
            }
            
        except Exception as e:
            print(f"获取数据概览失败: {e}")
            return {'message': f'查询失败: {str(e)}', 'total_records': 0}