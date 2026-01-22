import threading
import queue
import pandas as pd
import time
import json
import os
from datetime import datetime
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build


# Configuration
INPUT_FILE = "Channel_name_23012026.csv"  # Your input file with @handles
OUTPUT_FILE = None  # Set dynamically
CHECKPOINT_FILE = "youtube_checkpoint_new.json"

API_KEYS = [
    "AIzaSyBEDGArnsn19HcOZ0HoUHDHgy45iTOFyck", 
    "AIzaSyAzaNI7Q7M_XoI-fnJSOg0EohIwKkLG5h0", 
    "AIzaSyDINNKXsUKqrpUbpUH-TN95Rp_RIyvwqAw", 
    "AIzaSyC6EgEOuF0KkyI-h4aIdycR94qsSkNckJ0", 
    "AIzaSyCPY5EHlO8qpbOkWufklO_udfHYN5jrKd8", 
    "AIzaSyDaApW6zqkYu5J__Ty-8InJNXD2F8eIwQU", 
    "AIzaSyAdv6qRk29Kx9aXgsbDK3FURTFhYwcwtsc", 
    "AIzaSyB6btqibyAD1WwxrjZs-d5nONuM2gESFSc", 
    "AIzaSyA4i8r1HHmZvezt3hYci7A-2Y5xJyryvsQ", 
    "AIzaSyBQBpZXcYZ5X0_EZ81nxIE7569CyMFaoh0"
]

NUM_THREADS = len(API_KEYS)
BATCH_SIZE = 5


# Thread-safe globals
task_queue = queue.Queue()
checkpoint_lock = threading.Lock()
api_key_lock = threading.Lock()
api_key_index = 0


def setup_youtube_api(api_key):
    """Set up YouTube API client with specific key."""
    return build("youtube", "v3", developerKey=api_key)


def get_next_api_key():
    """Get next API key with rotation."""
    global api_key_index
    with api_key_lock:
        key = API_KEYS[api_key_index]
        api_key_index = (api_key_index + 1) % len(API_KEYS)
        return key


def get_channel_id_from_handle(youtube, handle):
    """Convert @handle to channel ID."""
    try:
        handle_clean = handle.strip()
        if handle_clean.startswith('@'):
            handle_clean = handle_clean[1:]
        
        # First try with @
        request = youtube.search().list(
            part="snippet",
            q=f"@{handle_clean}",
            type="channel",
            maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
        
        # Fallback without @
        request = youtube.search().list(
            part="snippet",
            q=handle_clean,
            type="channel",
            maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
        
        print(f"Could not find channel ID for handle: {handle}")
        return None
    except Exception as e:
        print(f"Error finding channel from handle {handle}: {e}")
        return None


def get_channel_info(youtube, channel_id):
    """Get detailed channel information."""
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
        
        uploads_playlist_id = content_details.get('relatedPlaylists', {}).get('uploads')
        
        return {
            'channel_id': str(channel_id),
            'channel_title': snippet.get('title', ''),
            'channel_handle': snippet.get('customUrl', ''),
            'channel_description': snippet.get('description', '')[:1000],  # Truncate long desc
            'subscriber_count': statistics.get('subscriberCount', '0'),
            'video_count': statistics.get('videoCount', '0'),
            'view_count': statistics.get('viewCount', '0'),
            'uploads_playlist_id': uploads_playlist_id,
            'country': snippet.get('country', ''),
            'published_at': snippet.get('publishedAt', ''),
            'topic_categories': '|'.join(topic_details.get('topicCategories', [])),
            'made_for_kids': status.get('madeForKids', False),
            'privacy_status': status.get('privacyStatus', '')
        }
    except Exception as e:
        print(f"Error getting channel info: {e}")
        return None


def get_channel_videos(youtube, uploads_playlist_id):
    """Get all videos from uploads playlist."""
    videos = []
    next_page_token = None
    total_fetched = 0
    
    try:
        while True:
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            items = response.get('items', [])
            total_fetched += len(items)
            
            for item in items:
                video_id = item['contentDetails']['videoId']
                videos.append({
                    'video_id': str(video_id),  # Save as text
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'published': item['snippet']['publishedAt'],
                    'video_url': f"https://www.youtube.com/watch?v={video_id}",
                    'channel_id': item['snippet']['channelId'],
                    'channel_title': item['snippet']['channelTitle']
                })
            
            print(f"  Fetched {total_fetched} videos so far...")
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
            time.sleep(0.5)
    except Exception as e:
        print(f"Error getting videos: {e}")
    
    print(f"  Total videos fetched: {len(videos)}")
    return videos


def get_video_statistics(youtube, video_ids):
    """Get statistics for video batch."""
    results = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            request = youtube.videos().list(
                part="statistics,snippet,contentDetails,status",
                id=",".join(batch)
            )
            response = request.execute()
            for item in response.get('items', []):
                stats = item.get('statistics', {})
                snippet = item.get('snippet', {})
                content_details = item.get('contentDetails', {})
                status = item.get('status', {})
                
                results.append({
                    'video_id': str(item['id']),  # Save as text
                    'likes': stats.get('likeCount', '0'),
                    'comments': stats.get('commentCount', '0'),
                    'views': stats.get('viewCount', '0'),
                    'tags': ','.join(snippet.get('tags', [])),
                    'duration': content_details.get('duration', ''),
                    'definition': snippet.get('definition', ''),
                    'category_id': snippet.get('categoryId', ''),
                    'license': status.get('license', ''),
                    'made_for_kids': status.get('madeForKids', False)
                })
        except Exception as e:
            print(f"Error getting video statistics: {e}")
        time.sleep(1)
    return results


def load_checkpoint():
    """Load checkpoint if exists."""
    global CHECKPOINT_FILE
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {"processed_rows": 0, "results": []}


def save_checkpoint(processed_rows, results):
    """Save checkpoint."""
    global CHECKPOINT_FILE
    try:
        checkpoint = {
            "processed_rows": processed_rows,
            "results": results
        }
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        print(f"Checkpoint saved. Processed {processed_rows} channels.")
    except Exception as e:
        print(f"Error saving checkpoint: {e}")


def save_results_to_csv(results, output_file):
    """Save results to CSV."""
    global OUTPUT_FILE
    if results:
        try:
            df = pd.DataFrame(results)
            # Comprehensive column order for your analytics
            column_order = [
                'channel_handle', 'channel_title', 'channel_id', 'channel_description',
                'subscriber_count', 'video_count', 'view_count', 'country', 
                'topic_categories', 'published_at',
                'channel_user', 'video_id', 'title', 'description', 'published',
                'video_url', 'tags', 'likes', 'comments', 'views',
                'duration', 'definition', 'category_id', 'license', 'made_for_kids'
            ]
            actual_columns = [col for col in column_order if col in df.columns]
            for col in df.columns:
                if col not in actual_columns:
                    actual_columns.append(col)
            df = df[actual_columns]
            df.to_csv(output_file, index=False, encoding='utf-8')
            print(f"Data saved to {output_file} ({len(results)} videos)")
            OUTPUT_FILE = output_file
        except Exception as e:
            print(f"Error saving CSV: {e}")


def worker(thread_id, df, results, processed_rows, output_file):
    """Worker thread."""
    global checkpoint_lock
    
    while True:
        try:
            i = task_queue.get_nowait()
        except queue.Empty:
            break

        handle = str(df.iloc[i]['channel_user']).strip()
        print(f"[Thread-{thread_id}] Processing {i+1}/{len(df)}: {handle}")
        retries = 3
        success = False

        while retries > 0:
            api_key = get_next_api_key()
            youtube = setup_youtube_api(api_key)
            
            try:
                channel_id = get_channel_id_from_handle(youtube, handle)
                if not channel_id:
                    break

                channel_info = get_channel_info(youtube, channel_id)
                if not channel_info or not channel_info.get('uploads_playlist_id'):
                    break

                videos = get_channel_videos(youtube, channel_info['uploads_playlist_id'])
                if not videos:
                    break

                video_ids = [v['video_id'] for v in videos]
                video_stats = get_video_statistics(youtube, video_ids)
                video_stats_dict = {vs['video_id']: vs for vs in video_stats}

                video_batch = []
                for video in videos:
                    video_result = {
                        **channel_info,  # Channel-level data
                        'channel_user': handle,
                        **video  # Video basic info
                    }
                    stats = video_stats_dict.get(video['video_id'], {})
                    video_result.update(stats)  # Video stats
                    
                    video_batch.append(video_result)

                with checkpoint_lock:
                    results.extend(video_batch)
                    processed_rows[0] = i + 1
                    save_checkpoint(processed_rows[0], results)
                    if processed_rows[0] % BATCH_SIZE == 0 or i == len(df) - 1:
                        save_results_to_csv(results, output_file)

                time.sleep(2)
                success = True
                break

            except HttpError as e:
                reason = e.error_details[0].get('reason', '') if hasattr(e, 'error_details') and e.error_details else str(e)
                print(f"[Thread-{thread_id}] HttpError: {reason}")
                if any(x in reason.lower() for x in ["quotaexceeded", "dailylimitExceeded"]):
                    print(f"[Thread-{thread_id}] Quota exceeded, rotating key...")
                retries -= 1
                time.sleep(2)
            except Exception as ex:
                print(f"[Thread-{thread_id}] Error: {ex}")
                retries -= 1
                time.sleep(2)

        if not success:
            print(f"[Thread-{thread_id}] Failed {handle} after retries.")

        task_queue.task_done()


def main():
    global OUTPUT_FILE
    
    # Read input
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"Error reading {INPUT_FILE}: {e}")
        print("Create CSV with 'channel_user' column containing @handles.")
        return
    
    OUTPUT_FILE = f"youtube_raw_new_channels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    start_time = datetime.now()
    
    # Auto-detect column
    if 'channel_user' not in df.columns:
        for col in df.columns:
            if df[col].astype(str).str.startswith('@').any():
                df.rename(columns={col: 'channel_user'}, inplace=True)
                print(f"Using '{col}' as 'channel_user'")
                break
        else:
            print("No column with @handles found.")
            return

    print(f"Processing {len(df)} channels with {NUM_THREADS} threads...")

    # Load checkpoint
    checkpoint = load_checkpoint()
    processed_rows = [checkpoint["processed_rows"]]
    results = checkpoint["results"]

    # Fill queue
    for i in range(processed_rows[0], len(df)):
        task_queue.put(i)

    # Start threads
    threads = []
    for t_id in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(t_id, df, results, processed_rows, OUTPUT_FILE))
        threads.append(t)
        t.start()

    # Wait
    for t in threads:
        t.join()
    
    total_time = datetime.now() - start_time
    print(f"Completed in {total_time}")

    # Final save and cleanup
    save_results_to_csv(results, OUTPUT_FILE)
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint removed.")


if __name__ == "__main__":
    main()