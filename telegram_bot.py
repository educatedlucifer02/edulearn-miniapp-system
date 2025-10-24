import os
import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from telegram import Bot, Update, BotCommand
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import TelegramError
import hashlib
import secrets
import json

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, database, storage_manager, file_manager):
        self.database = database
        self.storage_manager = storage_manager
        self.file_manager = file_manager
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.webapp_url = os.getenv('WEBAPP_URL', 'https://your-app.onrender.com')
        self.admin_password = os.getenv('ADMIN_PASSWORD', 'admin123')
        self.bot = None
        self.application = None
        
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN environment variable is required")
    
    async def initialize(self):
        """Initialize the Telegram bot"""
        try:
            self.bot = Bot(token=self.bot_token)
            self.application = Application.builder().token(self.bot_token).build()
            
            # Add command handlers
            self.application.add_handler(CommandHandler("start", self.start_command))
            self.application.add_handler(CommandHandler("help", self.help_command))
            self.application.add_handler(CommandHandler("miniapp", self.miniapp_command))
            self.application.add_handler(CommandHandler("reset_device", self.reset_device_command))
            self.application.add_handler(CommandHandler("status", self.status_command))
            self.application.add_handler(CommandHandler("admin", self.admin_command))
            
            # Add message handlers for file processing
            self.application.add_handler(MessageHandler(filters.Document.PDF | filters.Document.VIDEO | filters.VIDEO, self.handle_media))
            
            # Set bot commands
            await self.set_bot_commands()
            
            # Set webhook
            webhook_url = f"{self.webapp_url}/telegram/webhook"
            await self.bot.set_webhook(webhook_url)
            
            logger.info(f"Telegram bot initialized successfully with webhook: {webhook_url}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Telegram bot: {e}")
            raise
    
    async def set_bot_commands(self):
        """Set bot commands menu"""
        commands = [
            BotCommand("start", "Start the bot and get miniapp link"),
            BotCommand("miniapp", "Get miniapp link"),
            BotCommand("reset_device", "Reset your device (limited to 2 times)"),
            BotCommand("status", "Check your enrollment status"),
            BotCommand("help", "Show help information"),
            BotCommand("admin", "Admin panel access")
        ]
        
        await self.bot.set_my_commands(commands)
        logger.info("Bot commands set successfully")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user = update.effective_user
        chat_id = update.effective_chat.id
        
        # Check if user exists
        existing_user = await self.database.get_user_by_telegram_id(str(user.id))
        
        if not existing_user:
            # Create new user
            user_data = {
                "telegram_id": str(user.id),
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "chat_id": str(chat_id)
            }
            
            user_id = await self.database.create_user(user_data)
            logger.info(f"New user created: {user_id}")
        else:
            # Update last activity
            await self.database.update_user(existing_user['_id'], {"last_activity": datetime.utcnow()})
        
        miniapp_url = f"{self.webapp_url}/miniapp?user_id={user.id}"
        
        welcome_message = (
            f"🎓 Welcome to EduLearn, {user.first_name}!\n\n"
            "📱 Access your educational content through our miniapp:\n"
            f"🔗 [Open Miniapp]({miniapp_url})\n\n"
            "📚 Features:\n"
            "• Browse educational apps\n"
            "• Access your assigned courses\n"
            "• Watch videos and read PDFs\n"
            "• Track your progress\n\n"
            "💡 Use /help for more commands"
        )
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=welcome_message,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        help_text = (
            "🤖 **EduLearn Bot Help**\n\n"
            "📋 **Available Commands:**\n"
            "/start - Start the bot and get miniapp link\n"
            "/miniapp - Get direct miniapp access\n"
            "/reset_device - Reset your device registration (max 2 times)\n"
            "/status - Check your enrollment and device status\n"
            "/admin - Access admin panel (admin only)\n\n"
            "📱 **How to use:**\n"
            "1. Click on the miniapp link\n"
            "2. Browse available apps\n"
            "3. Access your assigned courses\n"
            "4. Watch videos and read materials\n\n"
            "🔒 **Security:**\n"
            "• One device per user\n"
            "• Device reset limited to 2 times\n"
            "• Secure session management\n\n"
            "❓ Need help? Contact support."
        )
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def miniapp_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /miniapp command"""
        user = update.effective_user
        miniapp_url = f"{self.webapp_url}/miniapp?user_id={user.id}"
        
        message = (
            f"📱 **Your Miniapp Access**\n\n"
            f"🔗 [Click here to open your miniapp]({miniapp_url})\n\n"
            "📚 Access all your educational content in one place!\n"
            "🎯 Courses are personalized based on your enrollment."
        )
        
        await update.message.reply_text(
            message,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    
    async def reset_device_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /reset_device command"""
        user = update.effective_user
        user_data = await self.database.get_user_by_telegram_id(str(user.id))
        
        if not user_data:
            await update.message.reply_text("❌ User not found. Please use /start first.")
            return
        
        reset_count = user_data.get('device_reset_count', 0)
        
        if reset_count >= 2:
            await update.message.reply_text(
                "⚠️ **Device Reset Limit Reached**\n\n"
                "You have already used your 2 device resets.\n"
                "Contact an administrator for additional resets."
            )
            return
        
        # Reset device
        success = await self.database.reset_user_device(user_data['_id'])
        
        if success:
            new_count = reset_count + 1
            remaining = 2 - new_count
            
            message = (
                f"✅ **Device Reset Successful**\n\n"
                f"🔄 Reset #{new_count} completed\n"
                f"📱 You can now login from a new device\n"
                f"⏳ Remaining resets: {remaining}"
            )
            
            # Log activity
            await self.database.log_user_activity({
                "user_id": user_data['_id'],
                "activity_type": "device_reset",
                "details": {"reset_count": new_count}
            })
            
            await update.message.reply_text(message, parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ Failed to reset device. Please try again later.")
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        user = update.effective_user
        user_data = await self.database.get_user_by_telegram_id(str(user.id))
        
        if not user_data:
            await update.message.reply_text("❌ User not found. Please use /start first.")
            return
        
        # Get user courses
        courses = await self.database.get_user_courses(user_data['_id'])
        
        # Build status message
        status_message = (
            f"📊 **Your Status - {user.first_name}**\n\n"
            f"🆔 User ID: {user_data['telegram_id']}\n"
            f"📅 Joined: {user_data['created_at'].strftime('%Y-%m-%d')}\n"
            f"🔄 Device Resets: {user_data.get('device_reset_count', 0)}/2\n"
            f"📱 Device Status: {'✅ Registered' if user_data.get('device_fingerprint') else '❌ Not Registered'}\n"
            f"🚫 Banned: {'Yes' if user_data.get('is_banned') else 'No'}\n\n"
        )
        
        if courses:
            status_message += "📚 **Enrolled Courses:**\n"
            for course in courses:
                status_message += f"• {course['name']} (ID: {course['course_id']})\n"
        else:
            status_message += "📚 **No courses assigned yet**\n"
        
        status_message += "\n🔗 Use /miniapp to access your content"
        
        await update.message.reply_text(status_message, parse_mode='Markdown')
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /admin command"""
        admin_url = f"{self.webapp_url}/admin"
        
        message = (
            "👨‍💼 **Admin Panel Access**\n\n"
            f"🔗 [Open Admin Panel]({admin_url})\n\n"
            "🔐 Use your admin credentials to login\n"
            "⚙️ Manage apps, courses, users, and media files"
        )
        
        await update.message.reply_text(
            message,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    
    async def handle_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle media files (for admin/channel indexing)"""
        # This is primarily for channel/group media indexing
        # Will be processed by the storage manager
        if update.effective_chat.type in ['group', 'supergroup', 'channel']:
            await self.storage_manager.process_channel_media(update)
    
    async def process_update(self, update_data: dict):
        """Process incoming webhook update"""
        try:
            update = Update.de_json(update_data, self.bot)
            await self.application.process_update(update)
        except Exception as e:
            logger.error(f"Error processing update: {e}")
    
    async def stop(self):
        """Stop the bot"""
        if self.application:
            await self.application.stop()
            logger.info("Telegram bot stopped")
    
    async def send_message_to_user(self, telegram_id: str, message: str, parse_mode: str = 'Markdown') -> bool:
        """Send message to user by telegram ID"""
        try:
            await self.bot.send_message(
                chat_id=telegram_id,
                text=message,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send message to {telegram_id}: {e}")
            return False
    
    async def send_notification_to_admins(self, message: str):
        """Send notification to all admins"""
        # Get admin users (you can implement admin user detection logic)
        admin_ids = os.getenv('ADMIN_IDS', '').split(',')
        
        for admin_id in admin_ids:
            if admin_id.strip():
                await self.send_message_to_user(admin_id.strip(), message)
    
    async def notify_course_assignment(self, user_id: str, course_name: str):
        """Notify user about course assignment"""
        user = await self.database.get_user_by_id(user_id)
        if user:
            message = (
                f"🎉 **New Course Assigned!**\n\n"
                f"📚 Course: {course_name}\n"
                f"🚀 Start learning now!\n\n"
                f"🔗 Use /miniapp to access your content"
            )
            
            await self.send_message_to_user(user['telegram_id'], message)
    
    async def notify_device_reset_by_admin(self, user_id: str):
        """Notify user about admin device reset"""
        user = await self.database.get_user_by_id(user_id)
        if user:
            message = (
                "🔧 **Device Reset by Admin**\n\n"
                "📱 Your device registration has been reset by an administrator.\n"
                "🔄 You can now login from a new device.\n\n"
                "🔗 Use /miniapp to access your content"
            )
            
            await self.send_message_to_user(user['telegram_id'], message)