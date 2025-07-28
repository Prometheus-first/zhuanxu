"""
匹配记录数据模型
"""
from datetime import datetime

class MatchRecord:
    """匹配记录模型"""
    
    def __init__(self, date, matched_count, event="注册匹配"):
        self.event = event
        self.date = date
        self.matched_count = matched_count
        self.created_time = datetime.now()
    
    def to_dict(self):
        """转换为字典格式"""
        return {
            '事件': self.event,
            '日期': self.date,
            '数量': self.matched_count,
            '创建时间': self.created_time
        }
    
    @staticmethod
    def from_dict(data):
        """从字典创建对象"""
        record = MatchRecord(
            date=data.get('日期'),
            matched_count=data.get('数量'),
            event=data.get('事件', '注册匹配')
        )
        if '创建时间' in data:
            record.created_time = data['创建时间']
        return record 