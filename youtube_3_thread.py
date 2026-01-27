import os
import time
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
import psycopg2
from psycopg2.extras import execute_values
import logging
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Configuration from .env - Updated to match your new format
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Load ALL API keys from .env (YOUTUBE_API_KEY_1 through YOUTUBE_API_KEY_10)
API_KEYS = [
    os.getenv(f'YOUTUBE_API_KEY_{i}') for i in range(1, 11)
]
API_KEYS = [key for key in API_KEYS if key]  # Remove None/empty keys

INPUT_FILE = "Channel_name_23012026.csv"
CHECKPOINT_FILE = "youtube_checkpoint_db.json"
BATCH_SIZE = 3  # Save to DB after every 3 channels
RATE_LIMIT_RETRY_DELAY = 60  # 1 minute delay on rate limit

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class YouTubeScraper:
    def __init__(self):
        self.api_key_index = 0
        self.checkpoint = self.load_checkpoint()
        self.setup_database()
        
    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def setup_database(self):
        """Create recycle_bin.youtube_scraped_data table."""
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            
            # Create schema if not exists
            cur.execute("""
            CREATE SCHEMA IF NOT EXISTS recycle_bin;
            """)
            
            # Create main table with comprehensive structure
            cur.execute("""
            CREATE TABLE IF NOT EXISTS recycle_bin.youtube_scraped_data (
                channel_id VARCHAR(255),
                channel_handle VARCHAR(255),
                channel_title TEXT,
                channel_description TEXT,
                subscriber_count BIGINT,
                video_count BIGINT,
                view_count BIGINT,
                uploads_playlist_id VARCHAR(255),
                country VARCHAR(10),
                published_at TIMESTAMP,
                topic_categories TEXT,
                made_for_kids BOOLEAN,
                privacy_status VARCHAR(50),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                video_id VARCHAR(255),
                title TEXT,
                description TEXT,
                video_published TIMESTAMP,
                video_url TEXT,
                channel_title_video TEXT,
                tags TEXT,
                likes BIGINT,
                comments BIGINT,
                views BIGINT,
                duration VARCHAR(50),
                definition VARCHAR(20),
                category_id VARCHAR(10),
                license VARCHAR(50),
                video_made_for_kids BOOLEAN,
                PRIMARY KEY (channel_id, video_id)
            );
            """)
            
            conn.commit()
            cur.close()
            logger.info("recycle_bin.youtube_scraped_data table ready")
    
    def setup_youtube_api(self, api_key):
        """Setup YouTube API client."""
        return build("youtube", "v3", developerKey=api_key)
    
    def get_next_api_key(self):
        """Get next API key with rotation."""
        key = API_KEYS[self.api_key_index % len(API_KEYS)]
        self.api_key_index += 1
        return key
    
    def test_api_key(self, api_key):
        """Test if API key works (simple quota check)."""
        try:
            youtube = self.setup_youtube_api(api_key)
            # Minimal request to test quota
            request = youtube.channels().list(part="id", id="UC_x5XG1OV2P6uZZ5FSM9Ttw", maxResults=1)
            request.execute()
            return True
        except HttpError as e:
            if 'quotaExceeded' in str(e).lower() or 'dailyLimitExceeded' in str(e).lower():
                return False
            return True  # Other errors are okay, just retry
        except:
            return False
    
    def get_working_api_key(self):
        """Get first working API key."""
        for attempt in range(len(API_KEYS)):
            api_key = self.get_next_api_key()
            if self.test_api_key(api_key):
                logger.info(f"Using API key {len(API_KEYS) - len([k for k in API_KEYS if k == api_key]) + 1}")
                return api_key
            logger.warning(f"API key {attempt + 1} exhausted")
        
        logger.error("ALL API KEYS EXHAUSTED - Stopping scraper")
        return None
    
    def get_channel_id_from_handle(self, youtube, handle):
        """Convert @handle to channel ID."""
        try:
            handle_clean = handle.strip().lstrip('@')
            
            # Try with handle search first
            request = youtube.search().list(
                part="snippet", 
                q=handle_clean, 
                type="channel", 
                maxResults=1
            )
            response = request.execute()
            if response.get('items'):
                return response['items'][0]['snippet']['channelId']
            
            return None
        except Exception as e:
            logger.error(f"Error finding channel {handle}: {e}")
            return None
    
    def get_channel_info(self, youtube, channel_id):
        """Get comprehensive channel information."""
        try:
            request = youtube.channels().list(
                part="snippet,statistics,contentDetails,topicDetails,status",
                id=channel_id
            )
            response = request.execute()
            if not response.get('items'):
                return None
            
            channel = response['items'][0]
            snippet = channel.get('snippet', {})
            statistics = channel.get('statistics', {})
            content_details = channel.get('contentDetails', {})
            topic_details = channel.get('topicDetails', {})
            status = channel.get('status', {})
            
            return {
                'channel_id': channel_id,
                'channel_handle': snippet.get('customUrl', ''),
                'channel_title': snippet.get('title', ''),
                'channel_description': snippet.get('description', '')[:1000],
                'subscriber_count': int(statistics.get('subscriberCount', 0) or 0),
                'video_count': int(statistics.get('videoCount', 0) or 0),
                'view_count': int(statistics.get('viewCount', 0) or 0),
                'uploads_playlist_id': content_details.get('relatedPlaylists', {}).get('uploads'),
                'country': snippet.get('country', ''),
                'published_at': snippet.get('publishedAt'),
                'topic_categories': '|'.join(topic_details.get('topicCategories', [])),
                'made_for_kids': status.get('madeForKids', False),
                'privacy_status': status.get('privacyStatus', '')
            }
        except Exception as e:
            logger.error(f"Error getting channel info {channel_id}: {e}")
            return None
    
    def get_channel_videos(self, youtube, uploads_playlist_id):
        """Fetch all videos from channel uploads."""
        videos = []
        next_page_token = None
        
        while True:
            try:
                request = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    videos.append({
                        'video_id': item['contentDetails']['videoId'],
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'][:1000],
                        'video_published': item['snippet']['publishedAt'],
                        'video_url': f"https://www.youtube.com/watch?v={item['contentDetails']['videoId']}",
                        'channel_title_video': item['snippet']['channelTitle']
                    })
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
                time.sleep(0.2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching videos: {e}")
                break
        
        return videos
    
    def get_video_statistics(self, youtube, video_ids):
        """Get statistics for video batch."""
        stats = {}
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            try:
                request = youtube.videos().list(
                    part="statistics,snippet,contentDetails,status",
                    id=",".join(batch)
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    stats[item['id']] = {
                        'likes': int(item.get('statistics', {}).get('likeCount', 0) or 0),
                        'comments': int(item.get('statistics', {}).get('commentCount', 0) or 0),
                        'views': int(item.get('statistics', {}).get('viewCount', 0) or 0),
                        'tags': ','.join(item.get('snippet', {}).get('tags', [])),
                        'duration': item.get('contentDetails', {}).get('duration', ''),
                        'definition': item.get('snippet', {}).get('definition', ''),
                        'category_id': item.get('snippet', {}).get('categoryId', ''),
                        'license': item.get('status', {}).get('license', ''),
                        'video_made_for_kids': item.get('status', {}).get('madeForKids', False)
                    }
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error getting video stats batch {i//50 + 1}: {e}")
        
        return stats
    
    def save_data_batch(self, records):
        """Save batch of complete records to recycle_bin.youtube_scraped_data."""
        if not records:
            return
        
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            
            # Prepare data for bulk insert
            insert_data = []
            columns = [
                'channel_id', 'channel_handle', 'channel_title', 'channel_description',
                'subscriber_count', 'video_count', 'view_count', 'uploads_playlist_id',
                'country', 'published_at', 'topic_categories', 'made_for_kids', 'privacy_status',
                'video_id', 'title', 'description', 'video_published', 'video_url', 'channel_title_video',
                'tags', 'likes', 'comments', 'views', 'duration', 'definition', 'category_id',
                'license', 'video_made_for_kids'
            ]
            
            for record in records:
                row = tuple(record.get(col, None) for col in columns)
                insert_data.append(row)
            
            try:
                execute_values(cur, f"""
                INSERT INTO recycle_bin.youtube_scraped_data 
                ({', '.join(columns)}) 
                VALUES %s
                ON CONFLICT (channel_id, video_id) DO UPDATE SET
                    channel_title = EXCLUDED.channel_title,
                    title = EXCLUDED.title,
                    views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    comments = EXCLUDED.comments,
                    scraped_at = CURRENT_TIMESTAMP
                """, insert_data, page_size=100)
                
                conn.commit()
                logger.info(f"Saved {len(records)} records to recycle_bin.youtube_scraped_data")
                
            except Exception as e:
                logger.error(f"Error saving batch: {e}")
                conn.rollback()
            finally:
                cur.close()
    
    def is_channel_processed(self, channel_id):
        """Check if channel already has data."""
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM recycle_bin.youtube_scraped_data WHERE channel_id = %s", 
                (channel_id,)
            )
            count = cur.fetchone()[0]
            cur.close()
            return count > 0
    
    def load_checkpoint(self):
        """Load checkpoint safely."""
        try:
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded checkpoint: row {data.get('processed_rows', 0)}")
                    return data
        except Exception as e:
            logger.warning(f"Error loading checkpoint: {e}")
        return {"processed_rows": 0}
    
    def save_checkpoint(self, processed_rows, last_handle):
        """Save checkpoint."""
        checkpoint = {
            "processed_rows": processed_rows,
            "last_handle": last_handle,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
    
    def process_channel(self, handle, row_index):
        """Process single channel completely."""
        api_key = self.get_working_api_key()
        if not api_key:
            return False
        
        youtube = self.setup_youtube_api(api_key)
        records = []
        
        try:
            # Get channel ID
            channel_id = self.get_channel_id_from_handle(youtube, handle)
            if not channel_id:
                logger.warning(f"No channel ID for {handle}")
                return True
            
            # Skip if already processed
            if self.is_channel_processed(channel_id):
                logger.info(f"Channel {channel_id} already processed")
                return True
            
            # Get channel info
            channel_info = self.get_channel_info(youtube, channel_id)
            if not channel_info or not channel_info.get('uploads_playlist_id'):
                logger.warning(f"No uploads playlist for {channel_id}")
                return True
            
            # Get videos
            videos = self.get_channel_videos(youtube, channel_info['uploads_playlist_id'])
            if not videos:
                logger.info(f"No videos for {channel_id}")
                return True
            
            # Get video stats
            video_ids = [v['video_id'] for v in videos]
            video_stats = self.get_video_statistics(youtube, video_ids)
            
            # Combine data
            for video in videos:
                record = {**channel_info, **video}
                stats = video_stats.get(video['video_id'], {})
                record.update(stats)
                record['description'] = video.get('description', '')  # Video desc
                record['video_published'] = video.get('video_published')
                records.append(record)
            
            # Save batch
            self.save_data_batch(records)
            logger.info(f"✓ {handle} ({len(videos)} videos)")
            return True
            
        except HttpError as e:
            if 'quotaExceeded' in str(e).lower() or 'dailyLimitExceeded' in str(e).lower():
                logger.error(f"🌡️ RATE LIMIT EXHAUSTED for {handle}. All keys stopped.")
                return False
            logger.error(f"HTTP error {handle}: {e}")
            return True
        except Exception as e:
            logger.error(f"❌ Error {handle}: {e}")
            return True
    
    def run(self):
        """Main execution."""
        try:
            df = pd.read_csv(INPUT_FILE)
        except Exception as e:
            logger.error(f"❌ Cannot read {INPUT_FILE}: {e}")
            return
        
        if 'channel_user' not in df.columns:
            for col in df.columns:
                if df[col].astype(str).str.startswith('@').any():
                    df.rename(columns={col: 'channel_user'}, inplace=True)
                    break
            else:
                logger.error("❌ No @handle column found")
                return
        
        start_row = self.checkpoint.get("processed_rows", 0)
        total_channels = len(df)
        logger.info(f"🚀 Starting from row {start_row}/{total_channels}")
        
        batch_counter = 0
        for i in range(start_row, total_channels):
            handle = str(df.iloc[i]['channel_user']).strip()
            logger.info(f"[{i+1}/{total_channels}] {handle}")
            
            success = self.process_channel(handle, i)
            if success:
                batch_counter += 1
                self.save_checkpoint(i + 1, handle)
                
                if batch_counter % BATCH_SIZE == 0:
                    logger.info(f"📝 Batch checkpoint: {batch_counter} channels")
                    batch_counter = 0
            else:
                logger.error("🛑 ALL API QUOTAS EXHAUSTED. Restart later.")
                break
            
            time.sleep(1.5)  # Polite rate limiting
        
        logger.info("🎉 Scraping completed!")
        logger.info(f"📊 Check data: SELECT COUNT(*) FROM recycle_bin.youtube_scraped_data;")

if __name__ == "__main__":
    scraper = YouTubeScraper()
    scraper.run()