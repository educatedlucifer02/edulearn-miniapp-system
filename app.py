from fastapi import FastAPI, Request, HTTPException, Depends, Form, File, UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import asynccontextmanager
import uvicorn
import os
import json
import asyncio
from datetime import datetime, timedelta
import hashlib
import secrets
from typing import Optional, List, Dict, Any
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import jwt
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import aiohttp
import io
from urllib.parse import quote

# Import our modules
from database import Database
from telegram_bot import TelegramBot
from storage_manager import StorageManager
from auth_manager import AuthManager
from miniapp_handler import MiniappHandler
from admin_handler import AdminHandler
from file_manager import FileManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
db = None
telegram_bot = None
storage_manager = None
auth_manager = None
miniapp_handler = None
admin_handler = None
file_manager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global db, telegram_bot, storage_manager, auth_manager, miniapp_handler, admin_handler, file_manager
    
    # Initialize database
    db = Database()
    await db.connect()
    
    # Initialize managers
    storage_manager = StorageManager(db)
    auth_manager = AuthManager(db)
    file_manager = FileManager(db, storage_manager)
    
    # Initialize handlers
    miniapp_handler = MiniappHandler(db, auth_manager, storage_manager, file_manager)
    admin_handler = AdminHandler(db, auth_manager, storage_manager, file_manager)
    
    # Initialize Telegram bot
    telegram_bot = TelegramBot(db, storage_manager, file_manager)
    await telegram_bot.initialize()
    
    # Start background tasks
    asyncio.create_task(storage_manager.start_file_refresh_task())
    
    logger.info("Application started successfully")
    yield
    
    # Shutdown
    if telegram_bot:
        await telegram_bot.stop()
    if db:
        await db.close()
    logger.info("Application shutdown complete")

app = FastAPI(
    title="EduLearn Miniapp System",
    description="Complete Educational Telegram Miniapp with Admin Panel",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

security = HTTPBearer(auto_error=False)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Telegram webhook endpoint
@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    try:
        update_data = await request.json()
        await telegram_bot.process_update(update_data)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return {"status": "error", "message": str(e)}

# Miniapp endpoints
@app.get("/miniapp", response_class=HTMLResponse)
async def miniapp_page(request: Request, user_id: str = None):
    return await miniapp_handler.render_miniapp(request, user_id)

@app.get("/api/miniapp/init")
async def miniapp_init(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.initialize_user(request, credentials)

@app.get("/api/miniapp/apps")
async def get_apps(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.get_apps(credentials)

@app.get("/api/miniapp/courses/{app_id}")
async def get_courses(app_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.get_courses(app_id, credentials)

@app.get("/api/miniapp/content/{course_id}")
async def get_course_content(course_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.get_course_content(course_id, credentials)

@app.get("/api/miniapp/stream/video/{file_id}")
async def stream_video(file_id: str, request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.stream_video(file_id, request, credentials)

@app.get("/api/miniapp/stream/pdf/{file_id}")
async def stream_pdf(file_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.stream_pdf(file_id, credentials)

@app.post("/api/miniapp/log-activity")
async def log_activity(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await miniapp_handler.log_activity(request, credentials)

# Admin Panel endpoints
@app.get("/admin", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    return await admin_handler.render_login_page(request)

@app.post("/admin/login")
async def admin_login(request: Request):
    return await admin_handler.login(request)

@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.render_dashboard(request, credentials)

@app.get("/api/admin/stats")
async def get_admin_stats(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.get_stats(credentials)

@app.get("/api/admin/users")
async def get_users(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.get_users(credentials)

@app.post("/api/admin/users/{user_id}/ban")
async def ban_user(user_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.ban_user(user_id, credentials)

@app.post("/api/admin/users/{user_id}/unban")
async def unban_user(user_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.unban_user(user_id, credentials)

@app.post("/api/admin/users/{user_id}/reset-device")
async def reset_user_device(user_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.reset_user_device(user_id, credentials)

@app.post("/api/admin/users/{user_id}/assign-course")
async def assign_course(user_id: str, request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.assign_course(user_id, request, credentials)

@app.get("/api/admin/apps")
async def get_admin_apps(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.get_apps(credentials)

@app.post("/api/admin/apps")
async def create_app(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.create_app(request, credentials)

@app.put("/api/admin/apps/{app_id}")
async def update_app(app_id: str, request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.update_app(app_id, request, credentials)

@app.delete("/api/admin/apps/{app_id}")
async def delete_app(app_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.delete_app(app_id, credentials)

@app.get("/api/admin/courses")
async def get_admin_courses(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.get_courses(credentials)

@app.post("/api/admin/courses")
async def create_course(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.create_course(request, credentials)

@app.put("/api/admin/courses/{course_id}")
async def update_course(course_id: str, request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.update_course(course_id, request, credentials)

@app.delete("/api/admin/courses/{course_id}")
async def delete_course(course_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.delete_course(course_id, credentials)

@app.get("/api/admin/media")
async def get_media_files(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.get_media_files(credentials)

@app.post("/api/admin/sync-channel")
async def sync_channel(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.sync_channel(request, credentials)

@app.get("/api/admin/user-activity/{user_id}")
async def get_user_activity(user_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    return await admin_handler.get_user_activity(user_id, credentials)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=False
    )