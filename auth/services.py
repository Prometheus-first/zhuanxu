"""
认证服务 - 处理用户登录、令牌生成等
"""
import jwt
from datetime import datetime, timedelta
from auth.models import User, UserRole
from utils.database import db
from config.config import Config

class AuthService:
    """认证服务类"""
    
    @staticmethod
    def create_user(username, password, real_name, role):
        """创建新用户"""
        try:
            # 检查用户是否已存在
            existing_user = db.db.users.find_one({'username': username})
            if existing_user:
                return {'success': False, 'message': '用户名已存在'}
            
            # 创建用户对象
            user = User(username, password, real_name, role)
            
            # 保存到数据库
            db.db.users.insert_one(user.to_db_dict())
            
            return {
                'success': True,
                'message': '用户创建成功',
                'user': user.to_dict()
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'创建用户失败: {str(e)}'
            }
    
    @staticmethod
    def authenticate_user(username, password):
        """验证用户身份"""
        try:
            # 查找用户
            user_data = db.db.users.find_one({'username': username})
            if not user_data:
                return {
                    'success': False,
                    'message': '用户名或密码错误'
                }
            
            # 创建用户对象（不包含密码）
            user = User(username, "", user_data['real_name'], user_data['role'])
            user.password_hash = user_data['password_hash']
            user.permissions = user_data['permissions']
            user.created_at = user_data['created_at']
            user.last_login = user_data.get('last_login')
            user.is_active = user_data.get('is_active', True)
            
            # 检查账号是否激活
            if not user.is_active:
                return {
                    'success': False,
                    'message': '账号已被禁用'
                }
            
            # 验证密码
            if not user.check_password(password):
                return {
                    'success': False,
                    'message': '用户名或密码错误'
                }
            
            # 更新最后登录时间
            db.db.users.update_one(
                {'username': username},
                {'$set': {'last_login': datetime.now()}}
            )
            
            return {
                'success': True,
                'user': user.to_dict()
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'认证失败: {str(e)}'
            }
    
    @staticmethod
    def generate_token(user_dict):
        """生成JWT令牌"""
        try:
            payload = {
                'username': user_dict['username'],
                'real_name': user_dict['real_name'],
                'role': user_dict['role'],
                'permissions': user_dict['permissions'],
                'exp': datetime.utcnow() + timedelta(hours=24),  # 24小时过期
                'iat': datetime.utcnow()
            }
            
            token = jwt.encode(
                payload,
                Config.SECRET_KEY,
                algorithm='HS256'
            )
            
            return {
                'success': True,
                'token': token,
                'expires_in': 86400  # 24小时（秒）
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'生成令牌失败: {str(e)}'
            }
    
    @staticmethod
    def verify_token(token):
        """验证JWT令牌"""
        try:
            payload = jwt.decode(
                token,
                Config.SECRET_KEY,
                algorithms=['HS256']
            )
            return {
                'success': True,
                'payload': payload
            }
        except jwt.ExpiredSignatureError:
            return {
                'success': False,
                'message': '令牌已过期'
            }
        except jwt.InvalidTokenError:
            return {
                'success': False,
                'message': '无效的令牌'
            }
    
    @staticmethod
    def get_user_by_username(username):
        """根据用户名获取用户信息"""
        try:
            user_data = db.db.users.find_one(
                {'username': username},
                {'password_hash': 0}  # 不返回密码哈希
            )
            return user_data
        except Exception as e:
            print(f"获取用户信息失败: {str(e)}")
            return None
    
    @staticmethod
    def init_default_users():
        """初始化默认用户（仅在系统首次启动时使用）"""
        default_users = [
            {
                'username': 'admin',
                'password': 'admin123',
                'real_name': '系统管理员',
                'role': UserRole.ADMIN.value
            },
            {
                'username': 'leader1',
                'password': 'leader123',
                'real_name': '部门领导',
                'role': UserRole.LEADER.value
            },
            {
                'username': 'zhangtongyi',
                'password': 'zt123456',
                'real_name': '张童义森',
                'role': UserRole.EMPLOYEE.value
            }
        ]
        
        for user_data in default_users:
            # 检查用户是否存在
            if not db.db.users.find_one({'username': user_data['username']}):
                AuthService.create_user(
                    user_data['username'],
                    user_data['password'],
                    user_data['real_name'],
                    user_data['role']
                )
                print(f"创建默认用户: {user_data['username']}") 