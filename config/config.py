"""
配置文件 - 包含应用的所有配置信息
"""
import os

class Config:
    """基础配置类"""
    # Flask配置
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = True
    
    # MongoDB配置
    MONGO_URI = os.environ.get('MONGO_URI') or "mongodb://localhost:27017/"
    MONGO_DB_NAME = "运营部"
    MONGO_COLLECTION_NAME = "张童义森"
    
    # CORS配置
    CORS_ORIGINS = ["*"]  # 生产环境应该限制具体域名
    
    # 服务器配置
    HOST = '0.0.0.0'
    PORT = 5000

class DevelopmentConfig(Config):
    """开发环境配置"""
    DEBUG = True

class ProductionConfig(Config):
    """生产环境配置"""
    DEBUG = False
    # 生产环境应该设置更安全的配置

# 配置映射
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
} 