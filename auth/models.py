"""
用户和角色模型定义
"""
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from enum import Enum

class UserRole(Enum):
    """用户角色枚举"""
    ADMIN = "admin"           # 系统管理员
    LEADER = "leader"         # 领导
    EMPLOYEE = "employee"     # 员工

class User:
    """用户模型"""
    
    def __init__(self, username, password, real_name, role, department="运营部"):
        self.username = username
        self.password_hash = generate_password_hash(password)
        self.real_name = real_name
        self.role = role
        self.department = department
        self.created_at = datetime.now()
        self.last_login = None
        self.is_active = True
        self.permissions = self._set_permissions()
    
    def _set_permissions(self):
        """根据角色设置权限"""
        permissions = {
            UserRole.ADMIN.value: [
                "system.manage",
                "user.manage",
                "data.view_all",
                "data.export_all"
            ],
            UserRole.LEADER.value: [
                "data.view_all",
                "data.export_all",
                "report.view",
                "employee.manage"
            ],
            UserRole.EMPLOYEE.value: [
                "data.view_own",
                "data.upload",
                "data.process"
            ]
        }
        return permissions.get(self.role, [])
    
    def check_password(self, password):
        """验证密码"""
        return check_password_hash(self.password_hash, password)
    
    def has_permission(self, permission):
        """检查是否有特定权限"""
        return permission in self.permissions
    
    def to_dict(self):
        """转换为字典"""
        return {
            'username': self.username,
            'real_name': self.real_name,
            'role': self.role,
            'department': self.department,
            'permissions': self.permissions,
            'created_at': self.created_at,
            'last_login': self.last_login,
            'is_active': self.is_active
        }
    
    def to_db_dict(self):
        """转换为数据库存储格式"""
        return {
            'username': self.username,
            'password_hash': self.password_hash,
            'real_name': self.real_name,
            'role': self.role,
            'department': self.department,
            'permissions': self.permissions,
            'created_at': self.created_at,
            'last_login': self.last_login,
            'is_active': self.is_active
        }

class Permission:
    """权限模型"""
    
    def __init__(self, name, description, resource, action):
        self.name = name              # 权限名称，如 "data.view_all"
        self.description = description # 权限描述
        self.resource = resource      # 资源类型，如 "data", "user"
        self.action = action          # 操作类型，如 "view", "create", "update"
    
    def to_dict(self):
        return {
            'name': self.name,
            'description': self.description,
            'resource': self.resource,
            'action': self.action
        }

# 预定义的系统权限
SYSTEM_PERMISSIONS = [
    Permission("system.manage", "系统管理", "system", "manage"),
    Permission("user.manage", "用户管理", "user", "manage"),
    Permission("data.view_all", "查看所有数据", "data", "view_all"),
    Permission("data.view_own", "查看自己的数据", "data", "view_own"),
    Permission("data.upload", "上传数据", "data", "upload"),
    Permission("data.process", "处理数据", "data", "process"),
    Permission("data.export_all", "导出所有数据", "data", "export_all"),
    Permission("report.view", "查看报表", "report", "view"),
    Permission("employee.manage", "员工管理", "employee", "manage")
] 