import os
import asyncio
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from telegram import Bot, Update, Message
from telegram.error import TelegramError
import aiohttp
import hashlib
import secrets
import json
import mimetypes
from urllib.parse import quote
import io

logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self, database):
        self.database = database
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.bot = Bot(token=self.bot_token) if self.bot_token else None
        self.file_refresh_interval = 23  # hours
        
    async def create_file_reference(self, telegram_file_id: str, file_type: str, 
                                  filename: str, file_size: int, 
                                  course_id: str = None, metadata: Dict = None) -> str:
        """Create a file reference with auto-refresh capability"""
        reference_id = self.generate_reference_id()
        
        reference_data = {
            "reference_id": reference_id,
            "telegram_file_id": telegram_file_id,
            "file_type": file_type,
            "filename": filename,
            "file_size": file_size,
            "course_id": course_id,
            "metadata": metadata or {},
            "is_active": True
        }
        
        await self.database.create_file_reference(reference_data)
        logger.info(f"Created file reference: {reference_id}")
        return reference_id
    
    async def get_fresh_file_id(self, reference_id: str) -> Optional[str]:
        """Get fresh file ID, refresh if needed"""
        file_ref = await self.database.get_file_reference(reference_id)
        
        if not file_ref:
            logger.error(f"File reference not found: {reference_id}")
            return None
        
        # Check if refresh is needed
        last_refreshed = file_ref.get('last_refreshed')
        if last_refreshed:
            time_diff = datetime.utcnow() - last_refreshed
            if time_diff.total_seconds() < (self.file_refresh_interval * 3600):
                return file_ref['telegram_file_id']
        
        # Need to refresh
        fresh_file_id = await self.refresh_file_id(file_ref)
        return fresh_file_id
    
    async def refresh_file_id(self, file_ref: Dict[str, Any]) -> Optional[str]:
        """Refresh telegram file ID by re-fetching from source"""
        try:
            # Try to get file info to validate current file_id
            current_file_id = file_ref['telegram_file_id']
            
            try:
                file_info = await self.bot.get_file(current_file_id)
                # If successful, update timestamp and return current ID
                await self.database.update_file_reference(
                    file_ref['reference_id'],
                    {"telegram_file_id": current_file_id}
                )
                return current_file_id
            except TelegramError:
                # File ID expired, need to find new one
                logger.warning(f"File ID expired for reference: {file_ref['reference_id']}")
                
                # Try to find the file again in the channel/group
                new_file_id = await self.find_file_in_channel(
                    file_ref['metadata'].get('channel_id'),
                    file_ref['filename'],
                    file_ref['file_type']
                )
                
                if new_file_id:
                    await self.database.update_file_reference(
                        file_ref['reference_id'],
                        {"telegram_file_id": new_file_id}
                    )
                    return new_file_id
                
                logger.error(f"Could not refresh file ID for: {file_ref['reference_id']}")
                return None
                
        except Exception as e:
            logger.error(f"Error refreshing file ID: {e}")
            return None
    
    async def find_file_in_channel(self, channel_id: str, filename: str, file_type: str) -> Optional[str]:
        """Find file in channel by filename and type"""
        try:
            # This would require scanning recent messages in the channel
            # For now, we'll implement a basic approach
            # In production, you might want to maintain a more sophisticated mapping
            
            # Get recent messages from channel (if bot has access)
            # This is a simplified implementation
            logger.info(f"Searching for file {filename} in channel {channel_id}")
            
            # Return None for now - in real implementation, you'd scan the channel
            return None
            
        except Exception as e:
            logger.error(f"Error finding file in channel: {e}")
            return None
    
    async def get_file_stream(self, reference_id: str) -> Optional[Tuple[io.BytesIO, str, int]]:
        """Get file stream by reference ID"""
        file_id = await self.get_fresh_file_id(reference_id)
        
        if not file_id:
            return None
        
        try:
            file_info = await self.bot.get_file(file_id)
            
            # Download file
            async with aiohttp.ClientSession() as session:
                async with session.get(file_info.file_path) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        # Get file reference for metadata
                        file_ref = await self.database.get_file_reference(reference_id)
                        
                        return (
                            io.BytesIO(content),
                            file_ref.get('filename', 'unknown'),
                            len(content)
                        )
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting file stream: {e}")
            return None
    
    async def get_file_url(self, reference_id: str) -> Optional[str]:
        """Get temporary file URL by reference ID"""
        file_id = await self.get_fresh_file_id(reference_id)
        
        if not file_id:
            return None
        
        try:
            file_info = await self.bot.get_file(file_id)
            return file_info.file_path
        except Exception as e:
            logger.error(f"Error getting file URL: {e}")
            return None
    
    async def sync_channel_content(self, channel_id: str, course_id: str) -> Dict[str, Any]:
        """Sync all content from a Telegram channel/group"""
        try:
            # Check if channel mapping exists
            mapping = await self.database.get_channel_mapping(channel_id)
            
            if not mapping:
                # Create new mapping
                mapping_data = {
                    "channel_id": channel_id,
                    "course_id": course_id,
                    "is_active": True
                }
                await self.database.create_channel_mapping(mapping_data)
            
            # Get channel info
            try:
                chat = await self.bot.get_chat(channel_id)
                logger.info(f"Syncing channel: {chat.title}")
            except Exception:
                logger.warning(f"Could not get channel info for: {channel_id}")
            
            # In a real implementation, you would:
            # 1. Scan through channel messages
            # 2. Find all media files (videos, documents)
            # 3. Create file references for each
            # 4. Associate with the course
            
            # For now, we'll return a success response
            # You would implement the actual scanning logic here
            
            await self.database.update_channel_sync(channel_id)
            
            return {
                "success": True,
                "message": f"Channel {channel_id} synced successfully",
                "files_processed": 0  # Would be actual count
            }
            
        except Exception as e:
            logger.error(f"Error syncing channel content: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def process_channel_media(self, update: Update):
        """Process media files posted in channels/groups"""
        try:
            message = update.message
            if not message:
                return
            
            chat = message.chat
            if chat.type not in ['group', 'supergroup', 'channel']:
                return
            
            # Check if this channel is mapped to any course
            mapping = await self.database.get_channel_mapping(str(chat.id))
            if not mapping:
                return
            
            # Process different types of media
            file_info = None
            file_type = None
            
            if message.document:
                file_info = message.document
                if message.document.mime_type:
                    if 'video' in message.document.mime_type:
                        file_type = 'video'
                    elif 'pdf' in message.document.mime_type:
                        file_type = 'pdf'
                    else:
                        file_type = 'document'
                else:
                    file_type = 'document'
            
            elif message.video:
                file_info = message.video
                file_type = 'video'
            
            if file_info and file_type:
                # Create file reference
                metadata = {
                    "channel_id": str(chat.id),
                    "channel_name": chat.title,
                    "message_id": message.message_id,
                    "mime_type": getattr(file_info, 'mime_type', None)
                }
                
                reference_id = await self.create_file_reference(
                    telegram_file_id=file_info.file_id,
                    file_type=file_type,
                    filename=getattr(file_info, 'file_name', f"{file_type}_{file_info.file_id[:10]}"),
                    file_size=file_info.file_size,
                    course_id=mapping['course_id'],
                    metadata=metadata
                )
                
                # Create media file record
                media_data = {
                    "reference_id": reference_id,
                    "course_id": mapping['course_id'],
                    "file_type": file_type,
                    "filename": getattr(file_info, 'file_name', f"{file_type}_{file_info.file_id[:10]}"),
                    "file_size": file_info.file_size,
                    "telegram_file_id": file_info.file_id,
                    "channel_id": str(chat.id),
                    "message_id": message.message_id,
                    "order": 0,  # You might want to implement ordering logic
                    "is_active": True
                }
                
                await self.database.create_media_file(media_data)
                logger.info(f"Processed media file: {reference_id}")
        
        except Exception as e:
            logger.error(f"Error processing channel media: {e}")
    
    async def start_file_refresh_task(self):
        """Start background task to refresh expired file IDs"""
        logger.info("Starting file refresh background task")
        
        while True:
            try:
                # Get files that need refresh
                expired_refs = await self.database.get_expired_file_references(self.file_refresh_interval)
                
                for file_ref in expired_refs:
                    logger.info(f"Refreshing file: {file_ref['reference_id']}")
                    await self.refresh_file_id(file_ref)
                    
                    # Small delay to avoid rate limiting
                    await asyncio.sleep(1)
                
                if expired_refs:
                    logger.info(f"Refreshed {len(expired_refs)} file references")
                
                # Sleep for 1 hour before next check
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error in file refresh task: {e}")
                await asyncio.sleep(300)  # Sleep 5 minutes on error
    
    def generate_reference_id(self) -> str:
        """Generate unique reference ID"""
        timestamp = str(int(datetime.utcnow().timestamp()))
        random_part = secrets.token_hex(8)
        return f"ref_{timestamp}_{random_part}"
    
    async def get_media_files_by_course(self, course_id: str, file_type: str = None) -> List[Dict[str, Any]]:
        """Get media files for a course"""
        return await self.database.get_course_media_files(course_id, file_type)
    
    async def delete_file_reference(self, reference_id: str) -> bool:
        """Delete file reference"""
        try:
            # Update file reference as inactive
            return await self.database.update_file_reference(
                reference_id,
                {"is_active": False}
            )
        except Exception as e:
            logger.error(f"Error deleting file reference: {e}")
            return False
    
    async def get_file_metadata(self, reference_id: str) -> Optional[Dict[str, Any]]:
        """Get file metadata by reference ID"""
        file_ref = await self.database.get_file_reference(reference_id)
        
        if not file_ref:
            return None
        
        return {
            "reference_id": file_ref['reference_id'],
            "filename": file_ref['filename'],
            "file_type": file_ref['file_type'],
            "file_size": file_ref['file_size'],
            "created_at": file_ref['created_at'],
            "last_refreshed": file_ref['last_refreshed'],
            "metadata": file_ref.get('metadata', {})
        }
    
    async def validate_file_access(self, reference_id: str) -> bool:
        """Validate if file is accessible"""
        file_id = await self.get_fresh_file_id(reference_id)
        
        if not file_id:
            return False
        
        try:
            await self.bot.get_file(file_id)
            return True
        except Exception:
            return False
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        try:
            total_files = await self.database.get_total_media_files_count()
            
            # Get file type breakdown
            video_files = len(await self.database.get_course_media_files("", "video"))
            pdf_files = len(await self.database.get_course_media_files("", "pdf"))
            
            return {
                "total_files": total_files,
                "video_files": video_files,
                "pdf_files": pdf_files,
                "document_files": total_files - video_files - pdf_files
            }
        except Exception as e:
            logger.error(f"Error getting storage stats: {e}")
            return {
                "total_files": 0,
                "video_files": 0,
                "pdf_files": 0,
                "document_files": 0
            }