"""
数据库工具类 - MongoDB连接和操作
"""
import pymongo
from config.config import Config

class Database:
    """MongoDB数据库管理类"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self):
        """连接数据库"""
        try:
            self.client = pymongo.MongoClient(Config.MONGO_URI)
            self.db = self.client[Config.MONGO_DB_NAME]
            self.collection = self.db[Config.MONGO_COLLECTION_NAME]
            print(f"成功连接到MongoDB: {Config.MONGO_DB_NAME}/{Config.MONGO_COLLECTION_NAME}")
            return True
        except Exception as e:
            print(f"MongoDB连接失败: {str(e)}")
            return False
    
    def disconnect(self):
        """断开数据库连接"""
        if self.client:
            self.client.close()
            print("MongoDB连接已关闭")
    
    def insert_one(self, document):
        """插入单个文档"""
        return self.collection.insert_one(document)
    
    def find(self, filter_dict, projection=None):
        """查询文档"""
        return self.collection.find(filter_dict, projection)
    
    def find_one(self, filter_dict):
        """查询单个文档"""
        return self.collection.find_one(filter_dict)

# 创建全局数据库实例
db = Database() 