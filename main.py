import googleapiclient.discovery
import mysql.connector
import re

# API Key connection to interact with YouTube API
api_service_name = "youtube"
api_version = "v3"
api_key = "YOUR_API_KEY"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Function to retrieve channel information from YouTube
def channel_information(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        channel_data = dict(
            channel_name=i['snippet']['title'],
            Channel_id=i["id"],
            channel_Description=i['snippet']['description'],
            channel_Thumbnail=i['snippet']['thumbnails']['default']['url'],
            channel_playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
            channel_subscribers=i['statistics']['subscriberCount'],
            channel_video_count=i['statistics']['videoCount'],
            channel_views=i['statistics']['viewCount'],
            channel_publishedat=i['snippet']['publishedAt']
        )
    return channel_data

# Function to retrieve playlist information of a channel from YouTube
def playlist_information(channel_id):
    playlist_info = []
    nextPageToken = None

    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=nextPageToken
        )
        response = request.execute()

        for i in response['items']:
            data = dict(
                playlist_id=i['id'],
                playlist_name=i['snippet']['title'],
                publishedat=i['snippet']['publishedAt'],
                channel_ID=i['snippet']['channelId'],
                channel_name=i['snippet']['channelTitle'],
                videoscount=i['contentDetails']['itemCount']
            )
            playlist_info.append(data)
        nextPageToken = response.get('nextPageToken')
        if nextPageToken is None:
            break

    return playlist_info

# Function to retrieve video IDs of a channel from YouTube
def get_video_ids(channel_id):
    response = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    playlist_videos = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    videos_ids = []

    while True:
        response1 = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_videos,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            videos_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break

    return videos_ids

# Function to retrieve video information of all video IDs from YouTube
def video_information(video_IDS):
    video_info = []
    for video_id in video_IDS:
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        ).execute()

        for i in response['items']:
            data = dict(
                channel_id=i['snippet']['channelId'],
                video_id=i['id'],
                video_name=i['snippet']['title'],
                video_Description=i['snippet']['description'],
                Thumbnail=i['snippet']['thumbnails']['default']['url'],
                Tags=i['snippet'].get('tags'),
                publishedAt=convert_iso_to_mysql_datetime(i['snippet']['publishedAt']),  # Ensure conversion
                Duration=convert_duration(i['contentDetails']['duration']),
                View_Count=i['statistics'].get('viewCount', 0),
                Like_Count=i['statistics'].get('likeCount', 0),
                Favorite_Count=i['statistics'].get('favoriteCount', 0),
                Comment_Count=i['statistics'].get('commentCount', 0),
                Caption_Status=i['contentDetails'].get('caption')
            )
            video_info.append(data)
    return video_info

# Function to convert duration from ISO 8601 format to HH:MM:SS format
def convert_duration(duration):
    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(regex, duration)
    if not match:
        return '00:00:00'
    hours, minutes, seconds = match.groups()
    hours = int(hours[:-1]) if hours else 0
    minutes = int(minutes[:-1]) if minutes else 0
    seconds = int(seconds[:-1]) if seconds else 0
    return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)

# Function to retrieve comments information for all video IDs from YouTube
def comments_information(video_IDS):
    comments_info = []
    for video_id in video_IDS:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
            response = request.execute()

            for i in response.get('items', []):
                data = dict(
                    video_id=i['snippet']['videoId'],
                    comment_id=i['snippet']['topLevelComment']['id'],
                    comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_publishedat=convert_iso_to_mysql_datetime(
                        i['snippet']['topLevelComment']['snippet']['publishedAt']
                    )
                )
                comments_info.append(data)

        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 403 and 'commentsDisabled' in str(e):
                print(f"Comments are disabled for video ID: {video_id}")
            else:
                print(f"Error retrieving comments for video ID: {video_id} - {e}")

    return comments_info


# Function to connect to MySQL and create/use a database
def connect_to_mysql():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="PASSWORD"
        )
        mycursor = mydb.cursor()
        mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube')
        mycursor.execute('USE youtube')
        print("Connected to MySQL and database 'youtube' is ready.")
        return mydb
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Function to create required tables
def create_tables():
    connection = connect_to_mysql()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR(50) PRIMARY KEY,
            channel_name VARCHAR(100),
            channel_description TEXT,
            channel_thumbnail VARCHAR(255),
            channel_playlist_id VARCHAR(50),
            channel_subscribers BIGINT,
            channel_video_count INT,
            channel_views BIGINT,
            channel_publishedat DATETIME
        );""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS playlist (
            playlist_id VARCHAR(50) PRIMARY KEY,
            playlist_name VARCHAR(100),
            publishedat DATETIME,
            channel_id VARCHAR(50),
            channel_name VARCHAR(100),
            videoscount BIGINT
        );""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(50) PRIMARY KEY,
            channel_id VARCHAR(50),
            video_name VARCHAR(200),
            video_description TEXT,
            thumbnail VARCHAR(255),
            tags TEXT,
            publishedat DATETIME,
            duration VARCHAR(20),
            view_count BIGINT,
            like_count INT,
            favorite_count INT,
            comment_count INT,
            caption_status VARCHAR(50),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );""")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR(50) PRIMARY KEY,
            video_id VARCHAR(50),
            comment_text TEXT,
            comment_author VARCHAR(100),
            comment_publishedat DATETIME,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        );""")
        connection.commit()
        print("Tables created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating tables: {err}")
    finally:
        cursor.close()
        connection.close()

from datetime import datetime

def convert_iso_to_mysql_datetime(iso_date):
    """
    Converts ISO 8601 format to MySQL-compatible datetime format.
    """
    return datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

def insert_channel_data(channel_data):
    """
    Inserts channel data into the 'channels' table in MySQL.
    """
    connection = connect_to_mysql()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()

        # Convert the datetime
        channel_publishedat = convert_iso_to_mysql_datetime(channel_data['channel_publishedat'])

        query = """
        INSERT INTO channels (channel_id, channel_name, channel_description, channel_thumbnail, 
                              channel_playlist_id, channel_subscribers, channel_video_count, 
                              channel_views, channel_publishedat)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            channel_name = VALUES(channel_name),
            channel_description = VALUES(channel_description),
            channel_thumbnail = VALUES(channel_thumbnail),
            channel_subscribers = VALUES(channel_subscribers),
            channel_video_count = VALUES(channel_video_count),
            channel_views = VALUES(channel_views);
        """
        data = (
            channel_data['Channel_id'],
            channel_data['channel_name'],
            channel_data['channel_Description'],
            channel_data['channel_Thumbnail'],
            channel_data['channel_playlist_id'],
            int(channel_data['channel_subscribers']),
            int(channel_data['channel_video_count']),
            int(channel_data['channel_views']),
            channel_publishedat  # Converted datetime
        )
        cursor.execute(query, data)
        connection.commit()
        print(f"Channel '{channel_data['channel_name']}' inserted successfully.")
    except mysql.connector.Error as err:
        print(f"Error inserting channel data: {err}")
    finally:
        cursor.close()
        connection.close()
        
def convert_iso_to_mysql_datetime(iso_date):
    """
    Converts ISO 8601 format to MySQL-compatible datetime format.
    Handles optional fractional seconds.
    """
    try:
        # Attempt to parse with fractional seconds
        return datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Fallback to parse without fractional seconds
        return datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")


def insert_playlist_data(playlist_data):
    connection = connect_to_mysql()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO playlist (playlist_id, playlist_name, publishedat, channel_id, channel_name, videoscount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            playlist_name = VALUES(playlist_name),
            publishedat = VALUES(publishedat),
            videoscount = VALUES(videoscount);
        """
        for playlist in playlist_data:
            publishedat = convert_iso_to_mysql_datetime(playlist['publishedat'])
            data = (
                playlist['playlist_id'],
                playlist['playlist_name'],
                publishedat,  # Converted datetime
                playlist['channel_ID'],
                playlist['channel_name'],
                int(playlist['videoscount'])
            )
            cursor.execute(query, data)
        connection.commit()
        print("Playlist data inserted successfully.")
    except mysql.connector.Error as err:
        print(f"Error inserting playlist data: {err}")
    finally:
        cursor.close()
        connection.close()
def insert_video_data(video_data):
    """
    Inserts video data into the 'videos' table in MySQL.
    """
    connection = connect_to_mysql()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO videos (video_id, channel_id, video_name, video_description, thumbnail, tags, 
                            publishedat, duration, view_count, like_count, favorite_count, comment_count, caption_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            view_count = VALUES(view_count),
            like_count = VALUES(like_count),
            comment_count = VALUES(comment_count);
        """
        for video in video_data:
            data = (
                video['video_id'],
                video['channel_id'],
                video['video_name'],
                video['video_Description'],
                video['Thumbnail'],
                ','.join(video['Tags']) if video['Tags'] else None,
                video['publishedAt'],
                video['Duration'],
                int(video['View_Count']),
                int(video['Like_Count']) if video['Like_Count'] else None,
                int(video['Favorite_Count']) if video['Favorite_Count'] else None,
                int(video['Comment_Count']),
                video['Caption_Status']
            )
            cursor.execute(query, data)
        connection.commit()
        print("Video data inserted successfully.")
    except mysql.connector.Error as err:
        print(f"Error inserting video data: {err}")
    finally:
        cursor.close()
        connection.close()
def insert_comments_data(comments_data):
    """
    Inserts comment data into the 'comments' table in MySQL.
    """
    connection = connect_to_mysql()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        query = """
        INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_publishedat)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            comment_text = VALUES(comment_text),
            comment_author = VALUES(comment_author);
        """
        for comment in comments_data:
            data = (
                comment['comment_id'],
                comment['video_id'],
                comment['comment_text'],
                comment['comment_author'],
                comment['comment_publishedat']
            )
            cursor.execute(query, data)
        connection.commit()
        print("Comments data inserted successfully.")
    except mysql.connector.Error as err:
        print(f"Error inserting comments data: {err}")
    finally:
        cursor.close()
        connection.close()



if __name__ == "__main__":
    channel_id = "UClDuPFyU0XWwy5EviF8dEVw"

    # Insert channel data
    channel_data = channel_information(channel_id)
    insert_channel_data(channel_data)

    # Insert playlist data
    playlist_data = playlist_information(channel_id)
    insert_playlist_data(playlist_data)

    # Insert video data
    video_ids = get_video_ids(channel_id)
    video_data = video_information(video_ids)
    insert_video_data(video_data)

    # Insert comments data
    comments_data = comments_information(video_ids)
    insert_comments_data(comments_data)

    print("All data inserted successfully into MySQL!")

