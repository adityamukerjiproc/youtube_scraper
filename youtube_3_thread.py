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
from logging.handlers import RotatingFileHandler
from contextlib import contextmanager

# ========= ENV & GLOBAL CONFIG =========

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

API_KEYS = [os.getenv(f'YOUTUBE_API_KEY_{i}') for i in range(1, 11)]
API_KEYS = [k.strip() for k in API_KEYS if k and k.strip()]

INPUT_FILE = "Channel_name_23012026.csv"
CHECKPOINT_FILE = "youtube_checkpoint_db.json"
BATCH_SIZE = 10
RATE_LIMIT_RETRY_DELAY = 60  # not heavily used, but available

os.makedirs("logs", exist_ok=True)
LOG_FILE = os.path.join("logs", f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# ========= LOGGING SETUP =========

logger = logging.getLogger("youtube_scraper")
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_fmt)

# Rotating file handler (10 MB, keep 5 backups)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(console_fmt)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Suppress googleapiclient discovery cache noise[web:16][web:17]
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

logger.info(f"🚀 YouTube scraper started")
logger.info(f"📁 Log file: {LOG_FILE}")
logger.info(f"🔑 Loaded {len(API_KEYS)} API keys")


# ========= SCRAPER CLASS =========

class YouTubeScraper:
    def __init__(self):
        self.api_key_index = 0
        self.checkpoint = self.load_checkpoint()
        self.setup_database()
        logger.info(f"✅ Initialized. Resume from row {self.checkpoint.get('processed_rows', 0)}")

    @contextmanager
    def get_db_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def setup_database(self):
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'recycle_bin'
            """)
            if not cur.fetchone():
                raise Exception("Schema 'recycle_bin' missing. CREATE SCHEMA recycle_bin;")

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
            logger.info("✅ DB check complete, table ready")

    def setup_youtube_api(self, api_key):
        # Disable discovery cache to avoid file_cache warnings[web:17]
        return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    def _next_key_index(self):
        idx = self.api_key_index % len(API_KEYS)
        self.api_key_index += 1
        return idx

    def test_api_key(self, api_key, key_num):
        """Test single key; return True if usable, False if quota-exhausted/invalid."""
        try:
            yt = self.setup_youtube_api(api_key)
            req = yt.channels().list(part="id", id="UC_x5XG1OV2P6uZZ5FSM9Ttw", maxResults=1)
            req.execute()
            return True
        except HttpError as e:
            msg = str(e).lower()
            if 'quotaexceeded' in msg or 'dailylimitexceeded' in msg:
                logger.warning(f"🔒 Key {key_num} quota exhausted")
                return False
            if 'badrequest' in msg or 'invalid' in msg:
                logger.warning(f"❌ Key {key_num} invalid/bad request")
                return False
            logger.warning(f"⚠️ Key {key_num} other HttpError: {e}")
            return False
        except Exception as e:
            logger.warning(f"⚠️ Key {key_num} error: {e}")
            return False

    def get_working_api_key(self):
        """Rotate through all keys and return first working one. Stop if none."""
        if not API_KEYS:
            logger.error("🛑 No API keys loaded")
            return None

        tried = set()
        while len(tried) < len(API_KEYS):
            idx = self._next_key_index()
            if idx in tried:
                continue
            tried.add(idx)

            api_key = API_KEYS[idx]
            key_num = idx + 1

            if self.test_api_key(api_key, key_num):
                logger.info(f"🔑 Using API key {key_num}")
                return api_key

        logger.error("🛑 ALL API KEYS EXHAUSTED - Stopping")
        return None

    def get_channel_id_from_handle(self, youtube, handle):
        try:
            handle_clean = handle.strip().lstrip('@')
            req = youtube.search().list(
                part="snippet",
                q=handle_clean,
                type="channel",
                maxResults=1
            )
            res = req.execute()
            if res.get('items'):
                return res['items'][0]['snippet']['channelId']
            return None
        except HttpError as e:
            msg = str(e).lower()
            if 'quotaexceeded' in msg:
                logger.warning(f"Quota exceeded while resolving handle {handle}")
            raise
        except Exception as e:
            logger.error(f"Error finding channel {handle}: {e}")
            return None

    def get_channel_info(self, youtube, channel_id):
        try:
            req = youtube.channels().list(
                part="snippet,statistics,contentDetails,topicDetails,status",
                id=channel_id
            )
            res = req.execute()
            if not res.get('items'):
                return None

            ch = res['items'][0]
            snippet = ch.get('snippet', {})
            stats = ch.get('statistics', {})
            content_details = ch.get('contentDetails', {})
            topic_details = ch.get('topicDetails', {})
            status = ch.get('status', {})

            return {
                'channel_id': channel_id,
                'channel_handle': snippet.get('customUrl', ''),
                'channel_title': snippet.get('title', ''),
                'channel_description': snippet.get('description', '')[:1000],
                'subscriber_count': int(stats.get('subscriberCount', 0) or 0),
                'video_count': int(stats.get('videoCount', 0) or 0),
                'view_count': int(stats.get('viewCount', 0) or 0),
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
        videos = []
        token = None
        while True:
            try:
                req = youtube.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=token
                )
                res = req.execute()
                for item in res.get('items', []):
                    videos.append({
                        'video_id': item['contentDetails']['videoId'],
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'][:1000],
                        'video_published': item['snippet']['publishedAt'],
                        'video_url': f"https://www.youtube.com/watch?v={item['contentDetails']['videoId']}",
                        'channel_title_video': item['snippet']['channelTitle']
                    })
                token = res.get('nextPageToken')
                if not token:
                    break
                time.sleep(0.2)
            except Exception as e:
                logger.error(f"Error fetching videos: {e}")
                break
        return videos

    def get_video_statistics(self, youtube, video_ids):
        stats = {}
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            try:
                req = youtube.videos().list(
                    part="statistics,snippet,contentDetails,status",
                    id=",".join(batch)
                )
                res = req.execute()
                for item in res.get('items', []):
                    st = item.get('statistics', {})
                    sn = item.get('snippet', {})
                    cd = item.get('contentDetails', {})
                    stt = item.get('status', {})
                    stats[item['id']] = {
                        'likes': int(st.get('likeCount', 0) or 0),
                        'comments': int(st.get('commentCount', 0) or 0),
                        'views': int(st.get('viewCount', 0) or 0),
                        'tags': ','.join(sn.get('tags', [])),
                        'duration': cd.get('duration', ''),
                        'definition': sn.get('definition', ''),
                        'category_id': sn.get('categoryId', ''),
                        'license': stt.get('license', ''),
                        'video_made_for_kids': stt.get('madeForKids', False)
                    }
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error getting video stats batch {i//50 + 1}: {e}")
        return stats

    def save_data_batch(self, records):
        if not records:
            return
        with self.get_db_connection() as conn:
            cur = conn.cursor()
            columns = [
                'channel_id', 'channel_handle', 'channel_title', 'channel_description',
                'subscriber_count', 'video_count', 'view_count', 'uploads_playlist_id',
                'country', 'published_at', 'topic_categories', 'made_for_kids', 'privacy_status',
                'video_id', 'title', 'description', 'video_published', 'video_url', 'channel_title_video',
                'tags', 'likes', 'comments', 'views', 'duration', 'definition', 'category_id',
                'license', 'video_made_for_kids'
            ]
            data = [tuple(r.get(c) for c in columns) for r in records]
            try:
                execute_values(cur, f"""
                    INSERT INTO recycle_bin.youtube_scraped_data
                    ({", ".join(columns)})
                    VALUES %s
                    ON CONFLICT (channel_id, video_id) DO UPDATE SET
                        channel_title = EXCLUDED.channel_title,
                        title = EXCLUDED.title,
                        views = EXCLUDED.views,
                        likes = EXCLUDED.likes,
                        comments = EXCLUDED.comments,
                        scraped_at = CURRENT_TIMESTAMP
                """, data, page_size=100)
                conn.commit()
                logger.info(f"💾 Saved {len(records)} records")
            except Exception as e:
                logger.error(f"Error saving batch: {e}")
                conn.rollback()
            finally:
                cur.close()

    def is_channel_processed(self, channel_id):
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
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading checkpoint: {e}")
        return {"processed_rows": 0}

    def save_checkpoint(self, processed_rows, last_handle):
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
        api_key = self.get_working_api_key()
        if not api_key:
            return False  # all keys exhausted

        youtube = self.setup_youtube_api(api_key)
        records = []
        try:
            channel_id = self.get_channel_id_from_handle(youtube, handle)
            if not channel_id:
                logger.warning(f"No channel ID for {handle}")
                return True

            if self.is_channel_processed(channel_id):
                logger.info(f"Channel {channel_id} already processed")
                return True

            ch_info = self.get_channel_info(youtube, channel_id)
            if not ch_info or not ch_info.get('uploads_playlist_id'):
                logger.warning(f"No uploads playlist for {channel_id}")
                return True

            videos = self.get_channel_videos(youtube, ch_info['uploads_playlist_id'])
            if not videos:
                logger.info(f"No videos for {channel_id}")
                return True

            video_ids = [v['video_id'] for v in videos]
            vstats = self.get_video_statistics(youtube, video_ids)

            for v in videos:
                rec = {**ch_info, **v}
                rec.update(vstats.get(v['video_id'], {}))
                rec['description'] = v.get('description', '')
                rec['video_published'] = v.get('video_published')
                records.append(rec)

            self.save_data_batch(records)
            logger.info(f"✅ {handle} ({len(videos)} videos)")
            return True

        except HttpError as e:
            msg = str(e).lower()
            if 'quotaexceeded' in msg or 'dailylimitexceeded' in msg:
                logger.error(f"🌡️ RATE LIMIT EXHAUSTED while processing {handle}")
                return False
            logger.error(f"HTTP error for {handle}: {e}")
            return True
        except Exception as e:
            logger.error(f"Unexpected error for {handle}: {e}")
            return True

    def run(self):
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
        total = len(df)
        logger.info(f"🚀 Starting from row {start_row}/{total}")

        batch_counter = 0
        for i in range(start_row, total):
            handle = str(df.iloc[i]['channel_user']).strip()
            logger.info(f"[{i+1}/{total}] {handle}")

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

            time.sleep(1.5)

        logger.info("🎉 Scraping completed!")
        logger.info("📊 Check data: SELECT COUNT(*) FROM recycle_bin.youtube_scraped_data;")


if __name__ == "__main__":
    scraper = YouTubeScraper()
    scraper.run()