import threading
import queue
import pandas as pd
import time
import json
import os
import random
from datetime import datetime
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

# Configuration
INPUT_FILE = "input/Channel_name_07092025.csv"  # Your input file with @handles
OUTPUT_FILE = f"youtube_raw_new_channels_{datetime.now()}.csv"   # Output as CSV
CHECKPOINT_FILE = "youtube_checkpoint_new.json"

API_KEYS = ["AIzaSyBEDGArnsn19HcOZ0HoUHDHgy45iTOFyck", "AIzaSyAzaNI7Q7M_XoI-fnJSOg0EohIwKkLG5h0", "AIzaSyDINNKXsUKqrpUbpUH-TN95Rp_RIyvwqAw "]  # Replace with real keys

# API_KEYS = ["AIzaSyBEDGArnsn19HcOZ0HoUHDHgy45iTOFyck", "AIzaSyAzaNI7Q7M_XoI-fnJSOg0EohIwKkLG5h0", "AIzaSyDINNKXsUKqrpUbpUH-TN95Rp_RIyvwqAw ",
#             "AIzaSyC6EgEOuF0KkyI-h4aIdycR94qsSkNckJ0", "AIzaSyCPY5EHlO8qpbOkWufklO_udfHYN5jrKd8", "AIzaSyDaApW6zqkYu5J__Ty-8InJNXD2F8eIwQU", 
#             "AIzaSyAdv6qRk29Kx9aXgsbDK3FURTFhYwcwtsc", "AIzaSyB6btqibyAD1WwxrjZs-d5nONuM2gESFSc", "AIzaSyA4i8r1HHmZvezt3hYci7A-2Y5xJyryvsQ", ]  # Replace with real keys

NUM_THREADS = len(API_KEYS)
BATCH_SIZE = 5  # Process this many channels before saving checkpoint

# Thread-safe queue and lock
task_queue = queue.Queue()
checkpoint_lock = threading.Lock()
api_key_lock = threading.Lock()
api_key_index = 0

def setup_youtube_api():
    """Set up the YouTube API client."""
    return build('youtube', 'v3', developerKey=API_KEYS)

def get_channel_id_from_handle(youtube, handle):
    """Convert @handle to a channel ID."""
    try:
        handle_clean = handle.strip()
        if handle_clean.startswith('@'):
            handle_clean = handle_clean[1:]
        request = youtube.search().list(
            part="snippet",
            q=f"@{handle_clean}",
            type="channel",
            maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
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
            part="snippet,statistics,contentDetails",
            id=channel_id
        )
        response = request.execute()
        if not response.get('items'):
            return None
        channel = response['items'][0]
        snippet = channel.get('snippet', {})
        statistics = channel.get('statistics', {})
        uploads_playlist_id = channel.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads')
        return {
            'channel_id': channel_id,
            'channel': snippet.get('title', ''),
            'channel_user': snippet.get('customUrl', ''),
            'channel_description': snippet.get('description', ''),
            'subscriber_count': statistics.get('subscriberCount', 0),
            'video_count': statistics.get('videoCount', 0),
            'view_count': statistics.get('viewCount', 0),
            'uploads_playlist_id': uploads_playlist_id
        }
    except Exception as e:
        print(f"Error getting channel info: {e}")
        return None

def get_channel_videos(youtube, uploads_playlist_id):
    """Get ALL videos from a channel's uploads playlist without limitation."""
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
            items_count = len(response.get('items', []))
            total_fetched += items_count
            for item in response.get('items', []):
                video_id = item['contentDetails']['videoId']
                videos.append({
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'published': item['snippet']['publishedAt'],
                    'video_url': f"https://www.youtube.com/watch?v={video_id}"
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
    """Get statistics for a list of videos."""
    results = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            request = youtube.videos().list(
                part="statistics,snippet,contentDetails",
                id=",".join(batch)
            )
            response = request.execute()
            for item in response.get('items', []):
                stats = item.get('statistics', {})
                results.append({
                    'video_id': item['id'],
                    'likes': stats.get('likeCount', 0),
                    'comments': stats.get('commentCount', 0),
                    'views': stats.get('viewCount', 0),
                    'tags': ','.join(item.get('snippet', {}).get('tags', []))
                })
        except Exception as e:
            print(f"Error getting video statistics: {e}")
        time.sleep(1)
    return results

def load_checkpoint():
    """Load the checkpoint file if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_rows": 0, "results": []}

def save_checkpoint(processed_rows, results):
    """Save progress to the checkpoint file."""
    try:
        checkpoint = {
            "processed_rows": processed_rows,
            "results": results
        }
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)
        print(f"Checkpoint saved. Processed {processed_rows} channels.")
    except Exception as e:
        print(f"Error in Checkpoint: {e}")

def save_results_to_csv(results):
    """Save results to CSV file."""
    if results:
        df = pd.DataFrame(results)
        column_order = ['channel_user', 'video_id', 'title', 'description', 
                        'published', 'channel', 'channel_id', 'tags', 
                        'likes', 'comments', 'views', 'video_url']
        actual_columns = [col for col in column_order if col in df.columns]
        for col in df.columns:
            if col not in actual_columns:
                actual_columns.append(col)
        df = df[actual_columns]
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"Data saved to {OUTPUT_FILE}")


def get_next_api_key():
    global api_key_index
    with api_key_lock:
        key = API_KEYS[api_key_index]
        api_key_index = (api_key_index + 1) % len(API_KEYS)
        return key

def setup_youtube_api(api_key):
    from googleapiclient.discovery import build
    return build("youtube", "v3", developerKey=api_key)

# def worker(thread_id, df, results, processed_rows):
#     global checkpoint_lock
#     while True:
#         try:
#             i = task_queue.get_nowait()
#         except queue.Empty:
#             break  # Queue is empty, exit the loop
#     # while not task_queue.empty():
#     #     i = task_queue.get()
#         handle = str(df.iloc[i]['channel_user']).strip()
#         print(f"[Thread-{thread_id}] Processing {i+1}/{len(df)}: {handle}")
#         retries = 3
#         while retries > 0:
#             api_key = get_next_api_key()
#             youtube = setup_youtube_api(api_key)
#             try:
#                 channel_id = get_channel_id_from_handle(youtube, handle)
#                 if not channel_id:
#                     break

#                 channel_info = get_channel_info(youtube, channel_id)
#                 if not channel_info or not channel_info.get('uploads_playlist_id'):
#                     break

#                 videos = get_channel_videos(youtube, channel_info['uploads_playlist_id'])
#                 if not videos:
#                     break

#                 video_ids = [v['video_id'] for v in videos]
#                 video_stats = get_video_statistics(youtube, video_ids)
#                 video_stats_dict = {vs['video_id']: vs for vs in video_stats}

#                 for video in videos:
#                     video_result = {
#                         'channel_user': handle,
#                         'video_id': video['video_id'],
#                         'title': video['title'],
#                         'description': video['description'],
#                         'published': video['published'],
#                         'channel': channel_info['channel'],
#                         'channel_id': channel_id,
#                         'video_url': video['video_url']
#                     }
#                     stats = video_stats_dict.get(video['video_id'], {})
#                     video_result.update({
#                         'tags': stats.get('tags', ''),
#                         'likes': stats.get('likes', 0),
#                         'comments': stats.get('comments', 0),
#                         'views': stats.get('views', 0)
#                     })

#                     with checkpoint_lock:
#                         results.append(video_result)

#                 with checkpoint_lock:
#                     processed_rows[0] = i + 1
#                     save_checkpoint(processed_rows[0], results)
#                     if processed_rows[0] % BATCH_SIZE == 0 or i == len(df) - 1:
#                         save_results_to_csv(results)
#                 time.sleep(2)
#                 break  # success, break retry loop
#             except HttpError as e:
#                 error_reason = e.error_details[0]['reason'] if hasattr(e, 'error_details') else str(e)
#                 if "quotaExceeded" in str(e) or "dailyLimitExceeded" in str(e):
#                     print(f"[Thread-{thread_id}] API key quota exceeded, rotating key...")
#                 else:
#                     print(f"[Thread-{thread_id}] HttpError: {error_reason}")
#                 retries -= 1
#                 time.sleep(1)
#             except Exception as ex:
#                 print(f"[Thread-{thread_id}] Other error: {ex}")
#                 retries -= 1
#                 time.sleep(1)
#         task_queue.task_done()

def worker(thread_id, df, results, processed_rows):
    global checkpoint_lock
    while True:
        try:
            i = task_queue.get_nowait()
        except queue.Empty:
            break  # Queue is empty, exit the loop

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
                    break  # skip this handle

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
                        'channel_user': handle,
                        'video_id': video['video_id'],
                        'title': video['title'],
                        'description': video['description'],
                        'published': video['published'],
                        'channel': channel_info['channel'],
                        'channel_id': channel_id,
                        'video_url': video['video_url']
                    }
                    stats = video_stats_dict.get(video['video_id'], {})
                    video_result.update({
                        'tags': stats.get('tags', ''),
                        'likes': stats.get('likes', 0),
                        'comments': stats.get('comments', 0),
                        'views': stats.get('views', 0)
                    })
                    video_batch.append(video_result)

                with checkpoint_lock:
                    results.extend(video_batch)
                    processed_rows[0] = i + 1
                    save_checkpoint(processed_rows[0], results)
                    if processed_rows[0] % BATCH_SIZE == 0 or i == len(df) - 1:
                        save_results_to_csv(results)

                time.sleep(2)
                success = True
                break  # break retry loop

            except HttpError as e:
                if hasattr(e, 'error_details'):
                    reason = e.error_details[0].get('reason', '')
                else:
                    reason = str(e)
                print(f"[Thread-{thread_id}] HttpError: {reason}")
                if "quotaExceeded" in reason or "dailyLimitExceeded" in reason:
                    print(f"[Thread-{thread_id}] API key quota exceeded, rotating key...")
                retries -= 1
                time.sleep(2)
            except Exception as ex:
                print(f"[Thread-{thread_id}] Other error: {ex}")
                retries -= 1
                time.sleep(2)

        if not success:
            print(f"[Thread-{thread_id}] Failed to process {handle} after retries.")

        task_queue.task_done()


def main():
    try:
        df = pd.read_csv(INPUT_FILE)
        # df = pd.read_excel(INPUT_FILE)
        # df = df.iloc[10:12]  # Adjust this as needed
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    start_time = datetime.now()
    if 'channel_user' not in df.columns:
        for col in df.columns:
            if df[col].astype(str).str.startswith('@').any():
                df.rename(columns={col: 'channel_user'}, inplace=True)
                print(f"Using column '{col}' as 'channel_user'")
                break
        else:
            print("Could not find a column with channel handles (@username).")
            return

    checkpoint = load_checkpoint()
    processed_rows = [checkpoint["processed_rows"]]  # use list to make mutable
    results = checkpoint["results"]

    for i in range(processed_rows[0], len(df)):
        task_queue.put(i)

    threads = []
    for t_id in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(t_id, df, results, processed_rows))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    end_time = datetime.now()
    total_time  = end_time-start_time

    print(f"Total time taken: {total_time}")

    save_results_to_csv(results)
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint file removed after successful completion.")

    end_time = datetime.now()
    total_time  = end_time-start_time

    print(f"Total time taken: {total_time}")


if __name__ == "__main__":
    main()