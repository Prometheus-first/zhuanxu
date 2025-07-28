"""
Flask应用主入口 - 重构后的模块化结构
"""
from flask import Flask, send_from_directory
from flask_cors import CORS
import os
import signal
import sys
import atexit

# 导入配置
from config.config import config
# 导入数据库
from utils.database import db
# 导入路由
from routes.excel_routes import excel_bp
from routes.admin_routes import admin_bp
from routes.ai_routes import ai_bp
from routes.retention_routes import retention_bp
from routes.video_active_routes import video_active_bp, cleanup_global_resources
from auth.routes import auth_bp

def signal_handler(signum, frame):
    """处理程序终止信号"""
    print(f"\n🚨 接收到信号 {signum}，正在清理资源...")
    cleanup_global_resources()
    print("✅ 程序正常退出")
    sys.exit(0)

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # 程序终止
if hasattr(signal, 'SIGBREAK'):  # Windows
    signal.signal(signal.SIGBREAK, signal_handler)

def create_app(config_name=None):
    """应用工厂函数"""
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    app = Flask(__name__)
    
    # 加载配置
    app.config.from_object(config[config_name])
    
    # 初始化CORS
    CORS(app, origins=app.config['CORS_ORIGINS'])
    
    # 连接数据库
    db.connect()
    
    # 注册蓝图
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(ai_bp)
    app.register_blueprint(excel_bp)
    app.register_blueprint(retention_bp)
    app.register_blueprint(video_active_bp, url_prefix='/api/video-active')
    
    # 配置静态文件目录
    app.static_folder = 'static'
    app.static_url_path = '/static'
    
    # 路由处理
    @app.route('/')
    def home():
        return app.send_static_file('login.html')
    
    @app.route('/login.html')
    def login():
        return app.send_static_file('login.html')
    
    @app.route('/dashboard.html')
    def dashboard():
        return app.send_static_file('dashboard.html')
    
    @app.route('/index.html')
    def excel_tool():
        return app.send_static_file('index.html')
    
    @app.route('/user-management.html')
    def user_management():
        return app.send_static_file('user-management.html')
    
    @app.route('/ai-chat.html')
    def ai_chat():
        return app.send_static_file('ai-chat.html')

    @app.route('/daily-active-analysis.html')
    def daily_active_analysis():
        return app.send_static_file('daily-active-analysis.html')
    
    return app

# 创建应用实例
app = create_app()

if __name__ == '__main__':
    print("Flask服务器启动中...")
    print(f"请确保MongoDB服务已经启动在 {app.config['MONGO_URI']}")
    print(f"数据库: {app.config['MONGO_DB_NAME']}, 集合: {app.config['MONGO_COLLECTION_NAME']}")
    
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        debug=app.config['DEBUG']
    ) 