import glob
import json
import os
import shutil
import sqlite3
from googleapiclient.discovery import build
import schedule
import time
from datetime import datetime, timedelta
from config import YOUTUBE_API_KEY, YOUTUBE_RAW_CHANNEL_IDS
from st_components.imports_and_utils import *

# YouTube API 密钥从config.py中获取
# 要查询的YouTube频道ID列表
CHANNEL_IDS = YOUTUBE_RAW_CHANNEL_IDS

# 创建YouTube API客户端
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# 初始化SQLite数据库
def init_db():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS videos
                 (channel_id TEXT, video_id TEXT, title TEXT, published_at TEXT, channel_title TEXT, 
                 downloaded INTEGER DEFAULT 0, description TEXT, duration INTEGER, save_path TEXT, 
                 processed INTEGER DEFAULT 0, twitter INTEGER DEFAULT 0)''')
    conn.commit()
    conn.close()

# def get_channel_id(youtube, channel_handle):
#     try:
#         request = youtube.search().list(
#             part="snippet",
#             type="channel",
#             q=channel_handle
#         )
#         response = request.execute()
        
#         if response['items']:
#             return response['items'][0]['snippet']['channelId']
#         else:
#             print(f"No channel found for handle: {channel_handle}")
#             return None
#     except Exception as e:
#         print(f"Error getting channel ID for {channel_handle}: {e}")
#         return None

# # 预处理CHANNEL_IDS
# def preprocess_channel_ids():
#     raw_ids = YOUTUBE_RAW_CHANNEL_IDS
#     for channel in raw_ids:
#         if channel.startswith('@'):
#             channel_id = get_channel_id(youtube, channel)
#             if channel_id:
#                 CHANNEL_IDS.append(channel_id)
#             else:
#                 print(f"无法获取频道ID: {channel}")
#         else:
#             CHANNEL_IDS.append(channel)

# # 在主程序开始时调用预处理函数
# preprocess_channel_ids()

import requests
from datetime import datetime
import time

PIPED_API_URL = "https://pipedapi-libre.kavin.rocks"

def get_latest_videos():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    for channel_id in CHANNEL_IDS:
        try:
            print(f"获取频道ID: {channel_id}")
            
            url = f"{PIPED_API_URL}/channel/{channel_id}"
            response = requests.get(url)
            response.raise_for_status()
            channel_data = response.json()

            channel_title = channel_data['name']
            videos = channel_data['relatedStreams'][:10]

            for video in videos:
                video_id = video['url'].split('=')[-1]
                duration = video.get('duration', 0)  # Get duration, default to 0 if not available

                # Ignore videos longer than 15 minutes (900 seconds)
                if duration > 900:
                    print(f"忽略长视频: {video['title']} (时长: {duration} 秒)")
                    continue
                if duration < 0:
                    print(f"忽略时长为负数，可能是直播的视频: {video['title']} (时长: {duration} 秒)")
                    continue

                c.execute("SELECT * FROM videos WHERE channel_id=? AND video_id=?", (channel_id, video_id))
                if c.fetchone() is None:
                    video_title = video['title']
                    published_at = datetime.fromtimestamp(video['uploaded'] / 1000).isoformat()
                    description = video.get('shortDescription', '')
                    downloaded = 0

                    print(f"发现新视频!")
                    print(f"频道: {channel_title}")
                    print(f"标题: {video_title}")
                    print(f"视频ID: {video_id}")
                    print(f"发布时间: {published_at}")
                    print(f"时长: {duration} 秒")
                    print(f"描述: {description}")
                    print("---")

                    c.execute("INSERT INTO videos (channel_id, video_id, title, published_at, channel_title, downloaded, duration, description) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                              (channel_id, video_id, video_title, published_at, channel_title, downloaded, duration, description))
                    conn.commit()
                else:
                    print(f"检查时间: {datetime.now()} - 视频 {video_id} 已存在于数据库中")

            if not videos:
                print(f"检查时间: {datetime.now()} - 频道 {channel_id} 没有新视频")

        except Exception as e:
            print(e)
            print(f"检查频道 {channel_id} 时发生错误: {str(e)}")

        # Add a delay to avoid overwhelming the Piped API
        time.sleep(2)

    conn.close()

def download_videos():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    try:
        # 获取所有未下载的视频
        c.execute("SELECT channel_id, video_id, title, published_at, channel_title FROM videos WHERE downloaded = 0")
        undownloaded_videos = c.fetchall()

        for video in undownloaded_videos:
            channel_id, video_id, video_title, published_at, channel_title = video
            
            print(f"处理未下载的视频:")
            print(f"频道: {channel_title}")
            print(f"标题: {video_title}")
            print(f"视频ID: {video_id}")
            print(f"发布时间: {published_at}")
            
            # 在这里添加下载和处理视频的代码
            # 例如：download_and_process_video(video_id)
            
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            try:
                save_path = f"output/{video_id}"
                step1_ytdlp.download_video_ytdlp(video_url, save_path)
                # 如果下载成功，将downloaded设置为1
                c.execute("UPDATE videos SET downloaded = 1, save_path = ? WHERE video_id = ?", (save_path, video_id))
                print(f"视频 {video_id} 下载成功")
            except Exception as e:
                # 如果出现下载错误，将downloaded设置为2
                c.execute("UPDATE videos SET downloaded = 2 WHERE video_id = ?", (video_id,))
                print(f"下载视频 {video_id} 时发生错误: {str(e)}")
            conn.commit()
    except Exception as e:
        import traceback
        print("Stacktrace:")
        print(traceback.format_exc())
        print(f"处理未下载视频时发生错误: {str(e)}")
    finally:
        conn.close()

def process_videos():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    try:
        # 获取所有已下载但未处理的视频
        c.execute("SELECT channel_id, video_id, title, published_at, channel_title, downloaded, save_path FROM videos WHERE downloaded = 1 AND processed = 0")
        unprocessed_videos = c.fetchall()

        for video in unprocessed_videos:
            channel_id, video_id, video_title, published_at, channel_title, downloaded, save_path = video
            
            print(f"处理已下载但未处理的视频:")
            print(f"频道: {channel_title}")
            print(f"标题: {video_title}")
            print(f"视频ID: {video_id}")
            print(f"保存路径: {save_path}")

            # 查找视频文件
            video_file = step1_ytdlp.find_video_files(save_path)
            if not video_file:
                print(f"无法找到视频文件: {video_id}")
                continue

            try:
                
                print(f"开始处理视频: {video_title}")
                
                step2_whisper.transcribe(video_file)
                
                step3_1_spacy_split.split_by_spacy()
                step3_2_splitbymeaning.split_sentences_by_meaning()
                
                step4_1_summarize.get_summary()
                from config import PAUSE_BEFORE_TRANSLATE
                if PAUSE_BEFORE_TRANSLATE:
                    input("⚠️ PAUSE_BEFORE_TRANSLATE. Go to `output/log/terminology.json` to edit terminology. Then press ENTER to continue...")
                step4_2_translate_all.translate_all()
                
                step5_splitforsub.split_for_sub_main()
                step6_generate_final_timeline.align_timestamp_main()
                
                step7_merge_sub_to_vid.merge_subtitles_to_video(save_path)
                
                def cleanup_and_move_files(video_id, save_path):
                    print(f"开始清理和移动文件...")
                    
                    # Create history directory for this video
                    history_dir = os.path.join('history', video_id)
                    os.makedirs(history_dir, exist_ok=True)

                    # Move audio files
                    audio_output_dir = 'output/audio'
                    if os.path.exists(audio_output_dir):
                        shutil.rmtree(os.path.join(history_dir, 'audio'), ignore_errors=True)
                        shutil.move(audio_output_dir, history_dir)
                    else:
                        print(f"Warning: Audio directory not found at {audio_output_dir}")
                    
                    # Move gpt_log files
                    gpt_log_output_dir = 'output/gpt_log'
                    if os.path.exists(gpt_log_output_dir):
                        shutil.rmtree(os.path.join(history_dir, 'gpt_log'), ignore_errors=True)
                        shutil.move(gpt_log_output_dir, history_dir)
                    else:
                        print(f"Warning: GPT log directory not found at {gpt_log_output_dir}")
                    
                    # Move log files
                    log_output_dir = 'output/log'
                    if os.path.exists(log_output_dir):
                        shutil.rmtree(os.path.join(history_dir, 'log'), ignore_errors=True)
                        shutil.move(log_output_dir, history_dir)
                    else:
                        print(f"Warning: Log directory not found at {log_output_dir}")
                    
                    # Move srt files
                    for srt_file in glob.glob('output/*.srt'):
                        dest_file = os.path.join(history_dir, os.path.basename(srt_file))
                        if os.path.exists(dest_file):
                            os.remove(dest_file)
                        shutil.move(srt_file, history_dir)
                    
                    # Move output video with subs
                    output_video = 'output/output_video_with_subs.mp4'
                    if os.path.exists(output_video):
                        dest_file = os.path.join(history_dir, 'output_video_with_subs.mp4')
                        if os.path.exists(dest_file):
                            os.remove(dest_file)
                        shutil.move(output_video, history_dir)
                    
                    # Delete the output folder for this video
                    shutil.rmtree(save_path, ignore_errors=True)
                    
                    print(f"清理和移动文件完成。所有相关文件已移至 {history_dir}")

                # Call the cleanup function
                cleanup_and_move_files(video_id, save_path)
                # 更新处理状态
                c.execute("UPDATE videos SET processed = 1 WHERE video_id = ?", (video_id,))
                conn.commit()
            
                print(f"视频处理完成: {video_title}")
            except Exception as e:
                print(f"处理视频 {video_id} 时发生错误: {str(e)}")
                c.execute("UPDATE videos SET processed = 2 WHERE video_id = ?", (video_id,))
                conn.commit()
    except Exception as e:
        conn.commit()
    finally:
        conn.close()
def get_tweet_text(video_id):
    summary_path = f'history/{video_id}/gpt_log/summary.json'

    try:
        with open(summary_path, 'r', encoding='utf-8') as f:
            summary_data = json.load(f)
        
        if isinstance(summary_data, list) and len(summary_data) > 0:
            tweet_text = summary_data[0]['response']['theme']
            terms = summary_data[0]['response']['terms']
        else:
            tweet_text = summary_data['response']['theme']
            terms = summary_data['response']['terms']
        
        if not tweet_text:
            raise KeyError("Theme not found in summary data")
        
        tweet_text += "\n\n"
        for term in terms:
            tweet_text += f"\n#{term['original']} ({term['translation']})：{term['explanation']}\n"
        
        return tweet_text
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Error reading summary for {video_id}: {str(e)}")
        return None

def post_twitters():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    c.execute("SELECT channel_id, video_id, title, published_at, channel_title FROM videos WHERE downloaded = 1 AND processed = 1 AND twitter = 0")
    videos = c.fetchall()

    import tweepy
    for video in videos:
        channel_id, video_id, title, published_at, channel_title = video
        
        # Construct paths
        video_path = f'history/{video_id}/output_video_with_subs.mp4'
        
        tweet_text = get_tweet_text(video_id)
        if not tweet_text:
            print(f"无法获取推文文本，跳过推文: {video_id}")
            c.execute("UPDATE videos SET twitter = 3 WHERE video_id = ?", (video_id,))  # 3表示无法获取推文文本
            conn.commit()
            continue
        
        # Post tweet with video
        try:
            # post_twitters_x_api_client(video_id, tweet_text, video_path)
            post_twitters_twitter_api_client(video_id, tweet_text, video_path)
            c.execute("UPDATE videos SET twitter = 1 WHERE video_id = ?", (video_id,))
            conn.commit()
            print(f"数据库更新成功，视频 {video_id} 标记为已发送推文")
            
            print(f"已成功为视频 {video_id} 发送推文")
        except tweepy.errors.TweepyException as e:
            print(f"发送推文时出错: {e}")
            c.execute("UPDATE videos SET twitter = 2 WHERE video_id = ?", (video_id,))
            conn.commit()
        
    conn.close()
    
def post_twitters_x_api_client(video_id, tweet_text, video_path):
    import tweepy
    from config import TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, TWITTER_BEARER_TOKEN, TWITTER_MEDIA_ADDITIONAL_OWNERS

    # Authenticate to Twitter using OAuth 2.0
    client = tweepy.Client(
        consumer_key=TWITTER_API_KEY,
        consumer_secret=TWITTER_API_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
        bearer_token=TWITTER_BEARER_TOKEN
    )
    
    print(f"开始为视频 {video_id} 发送推文...")
    auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    
    print(f"正在上传视频: {video_path}")
    media = api.media_upload(video_path, media_category="amplify_video")
    print(f"视频上传成功，media_id: {media.media_id_string}")
    
    print(f"正在发送推文，文本内容: {tweet_text[:50]}...")
    tweet = client.create_tweet(text=tweet_text, media_ids=[media.media_id_string])
    print(f"推文发送成功，tweet_id: {tweet.data['id']}")

    
def post_twitters_twitter_api_client(video_id, tweet_text, video_path):
    from twitter.account import Account
    from config import TWITTER_COOKIES_CT0, TWITTER_COOKIES_AUTH_TOKEN
    
    print(f"开始为视频 {video_id} 发送推文...")
    
    print("正在初始化Twitter账户...")
    account = Account(cookies = {
        "ct0": TWITTER_COOKIES_CT0,
        "auth_token": TWITTER_COOKIES_AUTH_TOKEN
    })
    print("Twitter账户初始化成功")

    res = account.tweet(tweet_text, media=[
        {
            "media": video_path
        }
    ])
    
    if 'errors' in res:
        print("推文发送失败，错误信息:")
        for error in res['errors']:
            print(f"错误代码: {error['code']}, 错误信息: {error['message']}")
    else:
        print(f"推文发送成功，tweet_id: {res['data']['rest_id']}")
        
def run_scheduler():
    # 初始化数据库
    init_db()
    # 立即运行一次
    get_latest_videos()
    # 处理未下载的视频
    download_videos()
    # 处理已下载但未处理的视频
    process_videos()
    # 发送twitter
    post_twitters()
    
    # 每10分钟运行一次检查最新视频和处理未下载视频
    def check_and_process():
        get_latest_videos()
        download_videos()
        process_videos()
        post_twitters()

    schedule.every(10).minutes.do(check_and_process)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    print("开始监控多个YouTube频道的新视频...")
    run_scheduler()