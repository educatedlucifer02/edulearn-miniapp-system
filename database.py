from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import os
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
        self.database_name = os.getenv('DATABASE_NAME', 'edulearn_miniapp')
        
    async def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = AsyncIOMotorClient(self.mongodb_url)
            self.db = self.client[self.database_name]
            
            # Test connection
            await self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            
            # Create indexes
            await self.create_indexes()
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("Database connection closed")
    
    async def create_indexes(self):
        """Create necessary database indexes"""
        try:
            # Users collection indexes
            await self.db.users.create_index("telegram_id", unique=True)
            await self.db.users.create_index("device_fingerprint")
            await self.db.users.create_index("is_banned")
            
            # Apps collection indexes
            await self.db.apps.create_index("name")
            await self.db.apps.create_index("is_active")
            
            # Courses collection indexes
            await self.db.courses.create_index("app_id")
            await self.db.courses.create_index("course_id", unique=True)
            await self.db.courses.create_index("is_active")
            
            # User courses collection indexes
            await self.db.user_courses.create_index([("user_id", ASCENDING), ("course_id", ASCENDING)], unique=True)
            
            # Media files collection indexes
            await self.db.media_files.create_index("course_id")
            await self.db.media_files.create_index("file_type")
            await self.db.media_files.create_index("telegram_file_id")
            await self.db.media_files.create_index("reference_id", unique=True)
            
            # Channel mappings collection indexes
            await self.db.channel_mappings.create_index("channel_id", unique=True)
            await self.db.channel_mappings.create_index("course_id")
            
            # User activities collection indexes
            await self.db.user_activities.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
            await self.db.user_activities.create_index("activity_type")
            
            # Admin sessions collection indexes
            await self.db.admin_sessions.create_index("token", unique=True)
            await self.db.admin_sessions.create_index("expires_at")
            
            # File references collection indexes
            await self.db.file_references.create_index("reference_id", unique=True)
            await self.db.file_references.create_index("telegram_file_id")
            await self.db.file_references.create_index("last_refreshed")
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    # Users operations
    async def create_user(self, user_data: Dict[str, Any]) -> str:
        """Create new user"""
        user_data['created_at'] = datetime.utcnow()
        user_data['updated_at'] = datetime.utcnow()
        user_data['is_banned'] = False
        user_data['device_reset_count'] = 0
        user_data['last_activity'] = datetime.utcnow()
        
        result = await self.db.users.insert_one(user_data)
        return str(result.inserted_id)
    
    async def get_user_by_telegram_id(self, telegram_id: str) -> Optional[Dict[str, Any]]:
        """Get user by telegram ID"""
        user = await self.db.users.find_one({"telegram_id": telegram_id})
        if user:
            user['_id'] = str(user['_id'])
        return user
    
    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        user = await self.db.users.find_one({"_id": ObjectId(user_id)})
        if user:
            user['_id'] = str(user['_id'])
        return user
    
    async def update_user(self, user_id: str, update_data: Dict[str, Any]) -> bool:
        """Update user data"""
        update_data['updated_at'] = datetime.utcnow()
        result = await self.db.users.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    async def ban_user(self, user_id: str) -> bool:
        """Ban user"""
        return await self.update_user(user_id, {"is_banned": True, "banned_at": datetime.utcnow()})
    
    async def unban_user(self, user_id: str) -> bool:
        """Unban user"""
        return await self.update_user(user_id, {"is_banned": False, "banned_at": None})
    
    async def reset_user_device(self, user_id: str) -> bool:
        """Reset user device (admin unlimited, user limited)"""
        user = await self.get_user_by_id(user_id)
        if not user:
            return False
        
        update_data = {
            "device_fingerprint": None,
            "device_info": None,
            "device_reset_count": user.get('device_reset_count', 0) + 1
        }
        
        return await self.update_user(user_id, update_data)
    
    async def get_users(self, skip: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
        """Get paginated list of users"""
        cursor = self.db.users.find().sort("created_at", DESCENDING).skip(skip).limit(limit)
        users = []
        async for user in cursor:
            user['_id'] = str(user['_id'])
            users.append(user)
        return users
    
    # Apps operations
    async def create_app(self, app_data: Dict[str, Any]) -> str:
        """Create new app"""
        app_data['created_at'] = datetime.utcnow()
        app_data['updated_at'] = datetime.utcnow()
        app_data['is_active'] = True
        
        result = await self.db.apps.insert_one(app_data)
        return str(result.inserted_id)
    
    async def get_apps(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get all apps"""
        filter_query = {"is_active": True} if active_only else {}
        cursor = self.db.apps.find(filter_query).sort("name", ASCENDING)
        apps = []
        async for app in cursor:
            app['_id'] = str(app['_id'])
            apps.append(app)
        return apps
    
    async def get_app_by_id(self, app_id: str) -> Optional[Dict[str, Any]]:
        """Get app by ID"""
        app = await self.db.apps.find_one({"_id": ObjectId(app_id)})
        if app:
            app['_id'] = str(app['_id'])
        return app
    
    async def update_app(self, app_id: str, update_data: Dict[str, Any]) -> bool:
        """Update app"""
        update_data['updated_at'] = datetime.utcnow()
        result = await self.db.apps.update_one(
            {"_id": ObjectId(app_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    async def delete_app(self, app_id: str) -> bool:
        """Soft delete app"""
        return await self.update_app(app_id, {"is_active": False})
    
    # Courses operations
    async def create_course(self, course_data: Dict[str, Any]) -> str:
        """Create new course"""
        course_data['created_at'] = datetime.utcnow()
        course_data['updated_at'] = datetime.utcnow()
        course_data['is_active'] = True
        
        result = await self.db.courses.insert_one(course_data)
        return str(result.inserted_id)
    
    async def get_courses_by_app(self, app_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get courses by app ID"""
        filter_query = {"app_id": app_id}
        if active_only:
            filter_query["is_active"] = True
        
        cursor = self.db.courses.find(filter_query).sort("name", ASCENDING)
        courses = []
        async for course in cursor:
            course['_id'] = str(course['_id'])
            courses.append(course)
        return courses
    
    async def get_course_by_id(self, course_id: str) -> Optional[Dict[str, Any]]:
        """Get course by ID"""
        course = await self.db.courses.find_one({"_id": ObjectId(course_id)})
        if course:
            course['_id'] = str(course['_id'])
        return course
    
    async def get_course_by_course_id(self, course_id: str) -> Optional[Dict[str, Any]]:
        """Get course by course_id field"""
        course = await self.db.courses.find_one({"course_id": course_id})
        if course:
            course['_id'] = str(course['_id'])
        return course
    
    async def update_course(self, course_id: str, update_data: Dict[str, Any]) -> bool:
        """Update course"""
        update_data['updated_at'] = datetime.utcnow()
        result = await self.db.courses.update_one(
            {"_id": ObjectId(course_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    async def delete_course(self, course_id: str) -> bool:
        """Soft delete course"""
        return await self.update_course(course_id, {"is_active": False})
    
    # User courses operations
    async def assign_course_to_user(self, user_id: str, course_id: str) -> bool:
        """Assign course to user"""
        try:
            await self.db.user_courses.insert_one({
                "user_id": user_id,
                "course_id": course_id,
                "assigned_at": datetime.utcnow(),
                "is_active": True
            })
            return True
        except:
            return False
    
    async def remove_course_from_user(self, user_id: str, course_id: str) -> bool:
        """Remove course assignment from user"""
        result = await self.db.user_courses.delete_one({
            "user_id": user_id,
            "course_id": course_id
        })
        return result.deleted_count > 0
    
    async def get_user_courses(self, user_id: str) -> List[Dict[str, Any]]:
        """Get courses assigned to user"""
        pipeline = [
            {"$match": {"user_id": user_id, "is_active": True}},
            {
                "$lookup": {
                    "from": "courses",
                    "localField": "course_id",
                    "foreignField": "course_id",
                    "as": "course_info"
                }
            },
            {"$unwind": "$course_info"},
            {"$match": {"course_info.is_active": True}}
        ]
        
        cursor = self.db.user_courses.aggregate(pipeline)
        courses = []
        async for doc in cursor:
            course = doc['course_info']
            course['_id'] = str(course['_id'])
            courses.append(course)
        return courses
    
    async def is_user_enrolled_in_course(self, user_id: str, course_id: str) -> bool:
        """Check if user is enrolled in course"""
        doc = await self.db.user_courses.find_one({
            "user_id": user_id,
            "course_id": course_id,
            "is_active": True
        })
        return doc is not None
    
    # Media files operations
    async def create_media_file(self, media_data: Dict[str, Any]) -> str:
        """Create media file record"""
        media_data['created_at'] = datetime.utcnow()
        media_data['updated_at'] = datetime.utcnow()
        
        result = await self.db.media_files.insert_one(media_data)
        return str(result.inserted_id)
    
    async def get_course_media_files(self, course_id: str, file_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get media files for course"""
        filter_query = {"course_id": course_id}
        if file_type:
            filter_query["file_type"] = file_type
        
        cursor = self.db.media_files.find(filter_query).sort("order", ASCENDING)
        files = []
        async for file in cursor:
            file['_id'] = str(file['_id'])
            files.append(file)
        return files
    
    async def get_media_file_by_reference_id(self, reference_id: str) -> Optional[Dict[str, Any]]:
        """Get media file by reference ID"""
        file = await self.db.media_files.find_one({"reference_id": reference_id})
        if file:
            file['_id'] = str(file['_id'])
        return file
    
    async def update_media_file(self, file_id: str, update_data: Dict[str, Any]) -> bool:
        """Update media file"""
        update_data['updated_at'] = datetime.utcnow()
        result = await self.db.media_files.update_one(
            {"_id": ObjectId(file_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    # Channel mappings operations
    async def create_channel_mapping(self, mapping_data: Dict[str, Any]) -> str:
        """Create channel mapping"""
        mapping_data['created_at'] = datetime.utcnow()
        mapping_data['last_synced'] = datetime.utcnow()
        
        result = await self.db.channel_mappings.insert_one(mapping_data)
        return str(result.inserted_id)
    
    async def get_channel_mapping(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get channel mapping by channel ID"""
        mapping = await self.db.channel_mappings.find_one({"channel_id": channel_id})
        if mapping:
            mapping['_id'] = str(mapping['_id'])
        return mapping
    
    async def update_channel_sync(self, channel_id: str) -> bool:
        """Update channel last sync time"""
        result = await self.db.channel_mappings.update_one(
            {"channel_id": channel_id},
            {"$set": {"last_synced": datetime.utcnow()}}
        )
        return result.modified_count > 0
    
    # User activity operations
    async def log_user_activity(self, activity_data: Dict[str, Any]) -> str:
        """Log user activity"""
        activity_data['timestamp'] = datetime.utcnow()
        
        result = await self.db.user_activities.insert_one(activity_data)
        return str(result.inserted_id)
    
    async def get_user_activities(self, user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get user activities"""
        cursor = self.db.user_activities.find(
            {"user_id": user_id}
        ).sort("timestamp", DESCENDING).limit(limit)
        
        activities = []
        async for activity in cursor:
            activity['_id'] = str(activity['_id'])
            activities.append(activity)
        return activities
    
    # Admin session operations
    async def create_admin_session(self, session_data: Dict[str, Any]) -> str:
        """Create admin session"""
        session_data['created_at'] = datetime.utcnow()
        
        result = await self.db.admin_sessions.insert_one(session_data)
        return str(result.inserted_id)
    
    async def get_admin_session(self, token: str) -> Optional[Dict[str, Any]]:
        """Get admin session by token"""
        session = await self.db.admin_sessions.find_one({"token": token})
        if session:
            session['_id'] = str(session['_id'])
        return session
    
    async def delete_admin_session(self, token: str) -> bool:
        """Delete admin session"""
        result = await self.db.admin_sessions.delete_one({"token": token})
        return result.deleted_count > 0
    
    # File reference operations
    async def create_file_reference(self, reference_data: Dict[str, Any]) -> str:
        """Create file reference for auto-refresh system"""
        reference_data['created_at'] = datetime.utcnow()
        reference_data['last_refreshed'] = datetime.utcnow()
        
        result = await self.db.file_references.insert_one(reference_data)
        return str(result.inserted_id)
    
    async def get_file_reference(self, reference_id: str) -> Optional[Dict[str, Any]]:
        """Get file reference by reference ID"""
        ref = await self.db.file_references.find_one({"reference_id": reference_id})
        if ref:
            ref['_id'] = str(ref['_id'])
        return ref
    
    async def update_file_reference(self, reference_id: str, update_data: Dict[str, Any]) -> bool:
        """Update file reference"""
        update_data['last_refreshed'] = datetime.utcnow()
        result = await self.db.file_references.update_one(
            {"reference_id": reference_id},
            {"$set": update_data}
        )
        return result.modified_count > 0
    
    async def get_expired_file_references(self, hours: int = 23) -> List[Dict[str, Any]]:
        """Get file references that need refresh"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        cursor = self.db.file_references.find({
            "last_refreshed": {"$lt": cutoff_time}
        })
        
        refs = []
        async for ref in cursor:
            ref['_id'] = str(ref['_id'])
            refs.append(ref)
        return refs
    
    # Statistics operations
    async def get_total_users_count(self) -> int:
        """Get total users count"""
        return await self.db.users.count_documents({})
    
    async def get_active_users_count(self, days: int = 30) -> int:
        """Get active users count"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        return await self.db.users.count_documents({
            "last_activity": {"$gte": cutoff_date}
        })
    
    async def get_total_apps_count(self) -> int:
        """Get total apps count"""
        return await self.db.apps.count_documents({"is_active": True})
    
    async def get_total_courses_count(self) -> int:
        """Get total courses count"""
        return await self.db.courses.count_documents({"is_active": True})
    
    async def get_total_media_files_count(self) -> int:
        """Get total media files count"""
        return await self.db.media_files.count_documents({})