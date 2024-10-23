import asyncio
import glob
import json
import os
import shutil
import sqlite3
from googleapiclient.discovery import build
import schedule
import time
from datetime import datetime, timedelta
from config import YOUTUBE_API_KEY, YOUTUBE_RAW_CHANNEL_IDS, TWITTER_MEDIA_ADDITIONAL_OWNERS
from st_components.imports_and_utils import *

# YouTube API 密钥从config.py中获取
# 要查询的YouTube频道ID列表
CHANNEL_IDS = YOUTUBE_RAW_CHANNEL_IDS

HOW_MANY_VIDEOS_TO_CHECK = 5

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


import requests
from datetime import datetime
import time

PIPED_API_URL = "https://pipedapi-libre.kavin.rocks"


def get_latest_videos():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    now = datetime.now()
    day_of_week = now.weekday()
    week_of_month = (now.day - 1) // 7 + 1
    month = now.month

    if day_of_week < 5:
        channel_index = day_of_week
    else:
        channel_index = week_of_month - 1
        if month % 2 != 0:
            channel_index = (channel_index + 1) % 5

    channel_id = CHANNEL_IDS[channel_index]

    print(f"[GET_LATEST] 使用channel_id: {channel_id} 于日期: {now.strftime('%Y-%m-%d')}")

    try:
        print(f"[GET_LATEST] [{channel_id}] 获取频道信息")

        url = f"{PIPED_API_URL}/channel/{channel_id}"
        response = requests.get(url)
        response.raise_for_status()
        channel_data = response.json()

        channel_title = channel_data['name']
        videos = channel_data['relatedStreams'][:HOW_MANY_VIDEOS_TO_CHECK]

        for video in videos:
            video_id = video['url'].split('=')[-1]
            duration = video.get('duration', 0)

            if duration > 900:
                print(f"[GET_LATEST] [{video_id}] 忽略长视频: {video['title']} (时长: {duration} 秒)")
                continue
            if duration < 0:
                print(
                    f"[GET_LATEST] [{video_id}] 忽略时长为负数，可能是直播的视频: {video['title']} (时长: {duration} 秒)")
                continue

            c.execute("SELECT * FROM videos WHERE channel_id=? AND video_id=?", (channel_id, video_id))
            if c.fetchone() is None:
                video_title = video['title']
                published_at = datetime.fromtimestamp(video['uploaded'] / 1000).isoformat()
                description = video.get('shortDescription', '')
                downloaded = 0

                print(f"[GET_LATEST] [{video_id}] 发现新视频!")
                print(f"[GET_LATEST] [{video_id}] 频道: {channel_title}")
                print(f"[GET_LATEST] [{video_id}] 标题: {video_title}")
                print(f"[GET_LATEST] [{video_id}] 发布时间: {published_at}")
                print(f"[GET_LATEST] [{video_id}] 时长: {duration} 秒")
                print(f"[GET_LATEST] [{video_id}] 描述: {description}")

                c.execute(
                    "INSERT INTO videos (channel_id, video_id, title, published_at, channel_title, downloaded, duration, description) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (channel_id, video_id, video_title, published_at, channel_title, downloaded, duration, description))
                conn.commit()
            else:
                print(f"[GET_LATEST] [{video_id}] 视频已存在于数据库中")

        if not videos:
            print(f"[GET_LATEST] [{channel_id}] 没有新视频")

    except Exception as e:
        print(f"[GET_LATEST] [{channel_id}] 检查频道时发生错误: {str(e)}")

    conn.close()


def download_videos():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    try:
        c.execute("SELECT channel_id, video_id, title, published_at, channel_title FROM videos WHERE downloaded = 0")
        undownloaded_videos = c.fetchall()

        for video in undownloaded_videos:
            channel_id, video_id, video_title, published_at, channel_title = video

            print(f"[DOWNLOAD] [{video_id}] 处理未下载的视频:")
            print(f"[DOWNLOAD] [{video_id}] 频道: {channel_title}")
            print(f"[DOWNLOAD] [{video_id}] 标题: {video_title}")
            print(f"[DOWNLOAD] [{video_id}] 发布时间: {published_at}")

            video_url = f"https://www.youtube.com/watch?v={video_id}"
            try:
                save_path = f"output/{video_id}"
                step1_ytdlp.download_video_ytdlp(video_url, save_path)
                c.execute("UPDATE videos SET downloaded = 1, save_path = ? WHERE video_id = ?", (save_path, video_id))
                print(f"[DOWNLOAD] [{video_id}] 下载成功")
            except Exception as e:
                c.execute("UPDATE videos SET downloaded = 2 WHERE video_id = ?", (video_id,))
                print(f"[DOWNLOAD] [{video_id}] 下载时发生错误: {str(e)}")
            conn.commit()
    except Exception as e:
        import traceback
        print("[DOWNLOAD] Stacktrace:")
        print(traceback.format_exc())
        print(f"[DOWNLOAD] 处理未下载视频时发生错误: {str(e)}")
    finally:
        conn.close()


def process_videos():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    try:
        c.execute(
            "SELECT channel_id, video_id, title, published_at, channel_title, downloaded, save_path, description FROM videos WHERE downloaded = 1 AND processed = 0")
        unprocessed_videos = c.fetchall()

        for video in unprocessed_videos:
            channel_id, video_id, video_title, published_at, channel_title, downloaded, save_path, description = video

            print(f"[PROCESS] [{video_id}] 处理已下载但未处理的视频:")
            print(f"[PROCESS] [{video_id}] 频道: {channel_title}")
            print(f"[PROCESS] [{video_id}] 标题: {video_title}")
            print(f"[PROCESS] [{video_id}] 保存路径: {save_path}")

            video_file = step1_ytdlp.find_video_files(save_path)
            if not video_file:
                print(f"[PROCESS] [{video_id}] 无法找到视频文件")
                continue

            try:
                print(f"[PROCESS] [{video_id}] 开始处理视频")

                step2_whisper.transcribe(save_path)
                print(f"[PROCESS] [{video_id}] 转录完成")

                step3_1_spacy_split.split_by_spacy()
                step3_2_splitbymeaning.split_sentences_by_meaning()
                print(f"[PROCESS] [{video_id}] 分句完成")

                step4_1_summarize.get_summary(description)
                from config import PAUSE_BEFORE_TRANSLATE
                if PAUSE_BEFORE_TRANSLATE:
                    input(
                        f"[PROCESS] [{video_id}] ⚠️ PAUSE_BEFORE_TRANSLATE. Go to `output/log/terminology.json` to edit terminology. Then press ENTER to continue...")
                step4_2_translate_all.translate_all()
                print(f"[PROCESS] [{video_id}] 翻译完成")

                step5_splitforsub.split_for_sub_main()
                step6_generate_final_timeline.align_timestamp_main()
                print(f"[PROCESS] [{video_id}] 字幕时间轴生成完成")

                step7_merge_sub_to_vid.merge_subtitles_to_video(save_path)
                print(f"[PROCESS] [{video_id}] 字幕合并到视频完成")

                def cleanup_and_move_files(video_id, save_path):
                    print(f"[PROCESS] [{video_id}] 开始清理和移动文件...")

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

                print(f"[PROCESS] [{video_id}] 视频处理完成")
            except Exception as e:
                print(f"[PROCESS] [{video_id}] 处理时发生错误: {str(e)}")
                c.execute("UPDATE videos SET processed = 2 WHERE video_id = ?", (video_id,))
                conn.commit()
    except Exception as e:
        print(f"[PROCESS] 处理视频时发生错误: {str(e)}")
    finally:
        conn.close()


def get_tweet_text(video_id, limit_to_280=False):
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

        if limit_to_280:

            full_tweet = ""
            if len(tweet_text) > 140:
                limited_tweet = tweet_text[:137] + "..."
                full_tweet = tweet_text[137:]
            else:
                limited_tweet = tweet_text + "\n\n"

            for term in terms:
                term_text = f"\n#{term['original'].replace(' ', '')} ({term['translation']})：{term['explanation']}\n"
                if len(limited_tweet) + len(term_text) <= 140:
                    limited_tweet += term_text
                else:
                    full_tweet += term_text

            return [limited_tweet, full_tweet]
        else:
            tweet_text += "\n\n"
            for term in terms:
                tweet_text += f"\n#{term['original'].replace(' ', '')} ({term['translation']})：{term['explanation']}\n"
            return [tweet_text]

    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Error reading summary for {video_id}: {str(e)}")
        return None


def send_single_tweet(c, conn, video, source="regular"):
    channel_id, video_id, title, published_at, channel_title, duration = video
    video_path = f'history/{video_id}/output_video_with_subs.mp4'

    tweet_textes = get_tweet_text(video_id, duration >= 600)
    print(f"[{source.upper()}] [{video_id}] 获取到的推文文本: {tweet_textes}")
    if len(tweet_textes) == 0:
        print(f"[{source.upper()}] [{video_id}] 无法获取推文文本，跳过推文")
        c.execute("UPDATE videos SET twitter = 2 WHERE video_id = ?", (video_id,))
        conn.commit()
        return

    try:
        if duration >= 600:
            print(f"[{source.upper()}] [{video_id}] 使用 Twikit Client 发送推文...")
            tweet_id = asyncio.run(post_twitters_twikit_client(video_id, tweet_textes, video_path))
        else:
            print(f"[{source.upper()}] [{video_id}] 使用 X API Client 发送推文...")
            tweet_id = post_twitters_x_api_client(video_id, tweet_textes, video_path)

        if not tweet_id:
            print(f"[{source.upper()}] [{video_id}] 使用 X API Client 发送失败，尝试使用 Twitter API Client...")
            tweet_id = post_twitters_twitter_api_client(video_id, tweet_textes, video_path)

            if not tweet_id:
                print(f"[{source.upper()}] [{video_id}] 使用 Twitter API Client 也失败，等待60秒后重试 X API Client...")
                time.sleep(60)
                tweet_id = post_twitters_x_api_client(video_id, tweet_textes, video_path)

        if not tweet_id:
            print(f"[{source.upper()}] [{video_id}] 所有尝试都失败，将视频标记为发送失败")
            c.execute("SELECT twitter FROM videos WHERE video_id = ?", (video_id,))
            current_value = c.fetchone()[0]
            new_value = 3 if current_value < 3 else current_value + 1
            c.execute("UPDATE videos SET twitter = ? WHERE video_id = ?", (new_value, video_id))
        else:
            c.execute("UPDATE videos SET twitter = ? WHERE video_id = ?", (tweet_id, video_id,))
            conn.commit()
            print(f"[{source.upper()}] [{video_id}] 数据库更新成功，视频标记为已发送推文")
    except Exception as e:
        print(f"[{source.upper()}] [{video_id}] 发送推文时出错: {e}")
        c.execute("UPDATE videos SET twitter = 2 WHERE video_id = ?", (video_id,))
        conn.commit()


def post_twitters():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    c.execute(
        "SELECT channel_id, video_id, title, published_at, channel_title, duration FROM videos WHERE downloaded = 1 AND processed = 1 AND twitter = 0")
    videos = c.fetchall()

    for video in videos:
        send_single_tweet(c, conn, video, source="post_twitters")

    conn.close()


def retry_failed_tweets():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    c.execute(
        "SELECT channel_id, video_id, title, published_at, channel_title, duration FROM videos WHERE downloaded = 1 AND processed = 1 AND twitter >= 3 AND twitter <= 10 ORDER BY published_at ASC LIMIT 10"
    )
    failed_tweets = c.fetchall()

    for video in failed_tweets:
        print(f"[RETRY] 重试发送推文: {video[1]}")
        send_single_tweet(c, conn, video, source="retry_failed_tweets")
        time.sleep(5)

    conn.close()


def post_twitters_x_api_client(video_id, tweet_textes, video_path):
    tweet_text = tweet_textes[0]
    try:
        import tweepy
        from config import TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET, \
            TWITTER_BEARER_TOKEN, TWITTER_MEDIA_ADDITIONAL_OWNERS

        print(f"[X API Client] [{video_id}] 开始发送推文...")
        auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth)

        print(f"[X API Client] [{video_id}] 正在上传视频: {video_path}")
        media = api.media_upload(video_path, media_category="amplify_video", chunked=True,
                                 additional_owners=TWITTER_MEDIA_ADDITIONAL_OWNERS)
        print(f"[X API Client] [{video_id}] 视频上传成功，media_id: {media.media_id_string}")

        # wait for 4 seconds
        time.sleep(4)

        print(f"[X API Client] [{video_id}] 正在发送推文，文本内容: {tweet_text[:50]}...")
        client = tweepy.Client(
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            access_token=TWITTER_ACCESS_TOKEN,
            access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
        )
        tweet = client.create_tweet(text=tweet_text, media_ids=[media.media_id_string],
                                    media_tagged_user_ids=TWITTER_MEDIA_ADDITIONAL_OWNERS)
        print(f"[X API Client] [{video_id}] 推文发送成功，tweet_id: {tweet.data['id']}")

        # Check if there's a second tweet text
        if len(tweet_textes) > 1:
            reply_text = tweet_textes[1]
            print(f"[X API Client] [{video_id}] 正在发送回复推文，文本内容: {reply_text[:50]}...")
            reply = client.create_tweet(text=reply_text, in_reply_to_tweet_id=tweet.data['id'])
            print(f"[X API Client] [{video_id}] 回复推文发送成功，reply_id: {reply.data['id']}")
        return tweet.data['id']
    except Exception as e:
        print(f"[X API Client] [{video_id}] 错误: {e}")
        return None


def post_twitters_twitter_api_client(video_id, tweet_textes, video_path):
    tweet_text = tweet_textes[0]
    try:
        from twitter.account import Account
        from config import TWITTER_COOKIES_CT0, TWITTER_COOKIES_AUTH_TOKEN

        print(f"[Twitter API Client] [{video_id}] 开始发送推文...")

        print(f"[Twitter API Client] [{video_id}] 正在初始化Twitter账户...")
        account = Account(cookies={
            "ct0": TWITTER_COOKIES_CT0,
            "auth_token": TWITTER_COOKIES_AUTH_TOKEN
        },
            debug=True)
        print(f"[Twitter API Client] [{video_id}] Twitter账户初始化成功")

        res = account.tweet(tweet_text, media=[
            {
                "media": video_path,
                "media_category": "amplify_video",
                "tagged_users": TWITTER_MEDIA_ADDITIONAL_OWNERS
            }
        ])

        if 'errors' in res:
            print(f"[Twitter API Client] [{video_id}] 推文发送失败，错误信息:")
            for error in res['errors']:
                print(f"[Twitter API Client] [{video_id}] 错误代码: {error['code']}, 错误信息: {error['message']}")
        else:
            tweet_id = res['data']['notetweet_create']['tweet_results']['result']['rest_id']
            print(f"[Twitter API Client] [{video_id}] 推文发送成功，tweet_id: {tweet_id}")
            # Check if there's a second tweet text
            if len(tweet_textes) > 1:
                reply_text = tweet_textes[1]
                print(f"[Twitter API Client] [{video_id}] 正在发送回复推文，文本内容: {reply_text[:50]}...")
                try:
                    reply = account.reply(text=reply_text, tweet_id=tweet_id)
                    if 'errors' in reply:
                        print(f"[Twitter API Client] [{video_id}] 回复推文发送失败，错误信息:")
                        for error in reply['errors']:
                            print(
                                f"[Twitter API Client] [{video_id}] 错误代码: {error['code']}, 错误信息: {error['message']}")
                    else:
                        reply_id = reply['data']['notetweet_create']['tweet_results']['result']['rest_id']
                        print(f"[Twitter API Client] [{video_id}] 回复推文发送成功，reply_id: {reply_id}")
                except Exception as e:
                    print(f"[Twitter API Client] [{video_id}] 发送回复推文时出错: {e}")
            return tweet_id
    except Exception as e:
        print(f"[Twitter API Client] [{video_id}] 错误: {e}")
        return None


async def post_twitters_twikit_client(video_id, tweet_textes, video_path):
    tweet_text = tweet_textes[0]
    try:
        from twikit import Client

        print(f"[Twikit Client] [{video_id}] 开始发送推文...")
        print(f"[Twikit Client] [{video_id}] 正在初始化 Twikit 客户端...")
        client = Client('en-US')
        client.load_cookies('cookies.json')
        print(f"[Twikit Client] [{video_id}] Twikit 客户端初始化成功")

        print(f"[Twikit Client] [{video_id}] 正在上传视频: {video_path}")
        media_id = await client.upload_media(video_path, media_category="amplify_video", is_long_video=True,
                                             wait_for_completion=True)
        print(f"[Twikit Client] [{video_id}] 视频上传成功，media_id: {media_id}")

        print(f"[Twikit Client] [{video_id}] 正在发送推文，文本内容: {tweet_text[:50]}...")
        tweet = await client.create_tweet(tweet_text, media_ids=[media_id], is_note_tweet=True)

        tweet_id = tweet.id
        print(f"[Twikit Client] [{video_id}] 推文发送成功，tweet_id: {tweet_id}")
        # Check if there's a second tweet text
        time.sleep(5)
        if len(tweet_textes) > 1:
            reply_text = tweet_textes[1]
            print(f"[Twikit Client] [{video_id}] 正在发送回复推文，文本内容: {reply_text[:50]}...")
            reply = await client.create_tweet(reply_text, reply_to=tweet.id, is_note_tweet=True)
            print(f"[Twikit Client] [{video_id}] 回复推文发送成功，reply_id: {reply.id}")
        return tweet_id
    except Exception as e:
        print(f"[Twikit Client] [{video_id}] 错误: {e}")
        return None


def watch_dog():
    conn = sqlite3.connect('youtube_videos.db')
    c = conn.cursor()

    # Get the latest 5 videos
    c.execute(f"SELECT processed FROM videos ORDER BY published_at DESC LIMIT {HOW_MANY_VIDEOS_TO_CHECK}")
    latest_videos = c.fetchall()

    # Count how many of the latest 5 videos have processed == 2
    processed_count = sum(1 for video in latest_videos if video[0] == 2)

    if processed_count >= 3:
        print("[Watch Dog] 检测到最新5个视频中有3个或更多处理失败，正在清理输出文件夹...")
        folders_to_delete = ['output/log', 'output/gpt_log', 'output/audio']
        for folder in folders_to_delete:
            if os.path.exists(folder):
                try:
                    shutil.rmtree(folder)
                    print(f"[Watch Dog] 已删除文件夹: {folder}")
                except Exception as e:
                    print(f"[Watch Dog] 删除文件夹 {folder} 时出错: {e}")
            else:
                print(f"[Watch Dog] 文件夹不存在: {folder}")

        # Update processed status from 2 to 3 for the failed videos
        c.execute("UPDATE videos SET processed = 3 WHERE processed = 2")
        conn.commit()
        print("[Watch Dog] 已将处理失败的视频状态从2更新为3")
    else:
        print("[Watch Dog] 最新视频处理正常，无需清理")

    conn.close()


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
        watch_dog()
        get_latest_videos()
        download_videos()
        process_videos()
        post_twitters()

    schedule.every(5).minutes.do(check_and_process)
    schedule.every(3).minutes.do(retry_failed_tweets)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    print("开始监控多个YouTube频道的新视频...")
    run_scheduler()
