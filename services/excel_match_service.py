"""
Excel匹配服务 - 处理Excel文件匹配的业务逻辑
"""
from datetime import datetime
from models.match_record import MatchRecord
from utils.database import db

class ExcelMatchService:
    """Excel匹配服务类"""
    
    @staticmethod
    def save_match_result(date, matched_count):
        """
        保存匹配结果到数据库
        
        Args:
            date: 匹配日期
            matched_count: 匹配数量
            
        Returns:
            dict: 包含成功状态和消息的字典
        """
        try:
            # 创建匹配记录
            record = MatchRecord(date=date, matched_count=matched_count)
            
            # 保存到数据库
            result = db.insert_one(record.to_dict())
            
            print(f"成功保存匹配结果: 日期={date}, 数量={matched_count}")
            
            return {
                'success': True,
                'message': '数据保存成功',
                'id': str(result.inserted_id)
            }
            
        except Exception as e:
            print(f"保存数据时出错: {str(e)}")
            return {
                'success': False,
                'message': f'保存失败: {str(e)}'
            }
    
    @staticmethod
    def get_match_history(limit=30):
        """
        获取匹配历史记录
        
        Args:
            limit: 返回记录数量限制
            
        Returns:
            dict: 包含历史记录的字典
        """
        try:
            # 查询最近的记录
            records = list(db.find(
                {'事件': '注册匹配'},
                {'_id': 0, '日期': 1, '数量': 1, '创建时间': 1}
            ).sort('创建时间', -1).limit(limit))
            
            # 转换日期格式
            for record in records:
                if '创建时间' in record and isinstance(record['创建时间'], datetime):
                    record['创建时间'] = record['创建时间'].strftime('%Y-%m-%d %H:%M:%S')
            
            return {
                'success': True,
                'data': records
            }
            
        except Exception as e:
            print(f"获取历史记录时出错: {str(e)}")
            return {
                'success': False,
                'message': f'获取失败: {str(e)}'
            } 