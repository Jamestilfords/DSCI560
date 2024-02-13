import praw
import mysql.connector
import re
from html import unescape
from datetime import datetime
import time
import pytesseract
from PIL import Image
import requests
from io import BytesIO
from collections import Counter
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

def get_direct_image_url(submission):
    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        pass  
    elif 'i.redd.it' in submission.url or 'i.imgur.com' in submission.url:
        return submission.url  
    else:
        return None

def find_image_urls(text):
    pattern = r'https?:\/\/.*\.(?:png|jpg|jpeg|gif)'
    return re.findall(pattern, text)

def extract_keywords(text, top_n=5):
    words = [word for word in re.findall(r'\w+', text.lower()) if word not in stop_words and len(word) > 1]
    most_common_words = Counter(words).most_common(top_n)
    return ', '.join(word for word, count in most_common_words)

def extract_text_from_image_url(image_url):
    response = requests.get(image_url)
    img = Image.open(BytesIO(response.content))
    text = pytesseract.image_to_string(img)
    return text

def clean_text(text):
    text = unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\W+', ' ', text)
    return text

reddit = praw.Reddit(
    client_id='5PNqPGsoCQZsN_ZckA_0KA',
    client_secret='_wojGyStfz4Wak3l8wtEpR_bL41WgQ',
    user_agent='James_Lab4'
)

def create_database():
    db = mysql.connector.connect(host="localhost", user="root", password="joe")
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS reddit")
    cursor.close()
    db.close()

def create_tables():
    db = mysql.connector.connect(host="localhost", user="root", password="joe", database="reddit")
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT,
            content TEXT,
            timestamp DATETIME,
            keywords TEXT,
            image_text TEXT
        )
    """)
    cursor.close()
    db.close()

def fetch_and_store_posts(total_posts, posts_per_batch=1000):
    db = mysql.connector.connect(host="localhost", user="root", password="joe", database="reddit")
    cursor = db.cursor()

    cursor.execute("TRUNCATE TABLE posts")
    print("Emptied the posts table.")

    fetched_posts = 0
    while fetched_posts < total_posts:
        posts_to_fetch = min(posts_per_batch, total_posts - fetched_posts)

        for submission in reddit.subreddit('tech').new(limit=posts_to_fetch):
            cleaned_content = ""
            image_text = ""

            if submission.is_self:
                cleaned_content = clean_text(submission.selftext)
            else:
                direct_image_url = get_direct_image_url(submission)
                if direct_image_url:
                    image_text = extract_text_from_image_url(direct_image_url)
                    print(f"Extracted image text for submission {submission.id}: {image_text}")
                else:
                    cleaned_content = submission.url

            full_text = submission.title + ' ' + cleaned_content + ' ' + image_text
            keywords = extract_keywords(full_text)

            post_data = (
                submission.title,
                cleaned_content,
                datetime.fromtimestamp(submission.created_utc),
                keywords,
                image_text
            )

            cursor.execute("""
                INSERT INTO posts (title, content, timestamp, keywords, image_text)
                VALUES (%s, %s, %s, %s, %s)
            """, post_data)

            db.commit()
            print(f"Successfully stored post {fetched_posts + 1}.")

            fetched_posts += 1

        if fetched_posts < total_posts:
            print("Waiting before fetching the next batch...")
            time.sleep(60)

    cursor.close()
    db.close()


create_database()
create_tables()
fetch_and_store_posts(5000, 1000)
