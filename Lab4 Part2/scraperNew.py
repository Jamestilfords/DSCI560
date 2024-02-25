import praw
import prawcore
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
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from sklearn.cluster import KMeans
import numpy as np
import sys
import os
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
import argparse
import threading
import json

should_continue = True

nltk.download('stopwords', quiet=True)
stop_words = set(stopwords.words('english'))

reddit = praw.Reddit(client_id='5PNqPGsoCQZsN_ZckA_0KA', client_secret='_wojGyStfz4Wak3l8wtEpR_bL41WgQ', user_agent='James_Lab4')

def get_direct_image_url(submission):
    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        return None
    elif 'i.redd.it' in submission.url or 'i.imgur.com' in submission.url:
        return submission.url
    return None

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

def extract_keywords(text, top_n=5):
    words = [word for word in re.findall(r'\w+', text.lower()) if word not in stop_words and len(word) > 1]
    most_common_words = Counter(words).most_common(top_n)
    return ', '.join(word for word, count in most_common_words)

def create_database():
    db = mysql.connector.connect(host="localhost", user="root", password="james")
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS ScrapeReddit")
    cursor.close()
    db.close()


def create_tables():
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS posts")
    cursor.execute("""
        CREATE TABLE posts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT,
            content TEXT,
            timestamp DATETIME,
            keywords TEXT,
            image_text TEXT,
            vector TEXT,
            cluster_id INT
        )
    """)
    cursor.close()
    db.close()


def store_vectors_and_cluster(model):
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT id, title, content, image_text FROM posts")
    documents = cursor.fetchall()
    # Generate vectors for each document
    vectors = [(doc[0], model.infer_vector((doc[1] + ' ' + doc[2] + ' ' + doc[3]).split())) for doc in documents]
    # Prepare data for clustering
    X = np.array([vector[1] for vector in vectors])
    kmeans = KMeans(n_clusters=5, random_state=0).fit(X)
    # Update each document with its vector and assigned cluster
    for idx, (doc_id, vector) in enumerate(vectors):
        vector_json = json.dumps(vector.tolist())  # Correctly serialize the vector
        cursor.execute("UPDATE posts SET vector = %s, cluster_id = %s WHERE id = %s", (vector_json, int(kmeans.labels_[idx]), doc_id))
    db.commit()
    cursor.close()
    db.close()


def train_doc2vec_model():
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT id, title, content, image_text FROM posts")
    documents = [TaggedDocument(words=(row[1] + ' ' + row[2] + ' ' + row[3]).split(), tags=[row[0]]) for row in cursor.fetchall()]
    cursor.close()
    db.close()
    model = Doc2Vec(documents, vector_size=300, window=2, min_count=1, workers=8, epochs=20)
    model.save("doc2vec_model")
    print("Doc2Vec model trained and saved.")
    return model

def store_vectors_and_cluster(model):
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT id, title, content, image_text FROM posts")
    documents = cursor.fetchall()
    
    # Generate vectors for each document and store them in an array for clustering
    vectors = [(doc[0], model.infer_vector((doc[1] + ' ' + doc[2] + ' ' + doc[3]).split())) for doc in documents]
    X = np.array([v[1] for v in vectors])  # Extract just the vectors for clustering
    
    # Perform clustering
    kmeans = KMeans(n_clusters=5, random_state=0).fit(X)
    
    # Update each document with its vector and assigned cluster ID
    for idx, (doc_id, vector) in enumerate(vectors):
        vector_json = json.dumps(vector.tolist())  # Serialize vector to JSON string
        cluster_id = int(kmeans.labels_[idx])
        cursor.execute("UPDATE posts SET vector = %s, cluster_id = %s WHERE id = %s", (vector_json, cluster_id, doc_id))
    
    db.commit()
    cursor.close()
    db.close()

def visualize_clusters():
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT vector, cluster_id FROM posts")
    vectors = np.array([np.fromstring(row[0][1:-1], sep=',') for row in cursor.fetchall()])
    labels = np.array([row[1] for row in cursor.fetchall()])
    tsne = TSNE(n_components=2, random_state=0)
    tsne_results = tsne.fit_transform(vectors)
    plt.figure(figsize=(16,10))
    for i in range(max(labels)+1):
        plt.scatter(tsne_results[labels == i, 0], tsne_results[labels == i, 1], label=f'Cluster {i}')
    plt.legend()
    plt.show()

def fetch_and_store_posts(total_posts, posts_per_batch=1000):
    global should_continue
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("TRUNCATE TABLE posts")
    fetched_posts = 0
    while fetched_posts < total_posts and should_continue:
        posts_to_fetch = min(posts_per_batch, total_posts - fetched_posts)
        for submission in reddit.subreddit('tech').new(limit=posts_to_fetch):
            if not should_continue:
                break
            cleaned_content = ""
            image_text = ""
            if submission.is_self:
                cleaned_content = clean_text(submission.selftext)
            else:
                direct_image_url = get_direct_image_url(submission)
                if direct_image_url:
                    image_text = extract_text_from_image_url(direct_image_url)
                else:
                    cleaned_content = submission.url
            full_text = submission.title + ' ' + cleaned_content + ' ' + image_text
            keywords = extract_keywords(full_text)
            post_data = (
                submission.title,
                cleaned_content,
                datetime.fromtimestamp(submission.created_utc),
                keywords,
                image_text,
                "[]",  # Initialize vector as an empty JSON array
                0  # Initialize cluster_id as 0
            )
            cursor.execute("""
                INSERT INTO posts (title, content, timestamp, keywords, image_text, vector, cluster_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, post_data)
            db.commit()
            fetched_posts += 1
            print(f"Successfully stored post {fetched_posts}.")
    model = train_doc2vec_model()
    store_vectors_and_cluster(model)
    cursor.close()
    db.close()

def input_thread():
    global should_continue
    while should_continue:
        user_input = input("Type 'quit' to stop the automation: ")
        if user_input.strip().lower() == 'quit':
            should_continue = False
            print("Stopping automation...")
            break


def automate(interval_minutes):
    global should_continue
    input_thread_instance = threading.Thread(target=input_thread)
    input_thread_instance.start()
    should_continue = True

    while should_continue:
        fetch_and_store_posts(5000, 1000)
        if not should_continue:
            break
        print("Waiting for the next interval...")
        time.sleep(interval_minutes * 60)
    
    input_thread_instance.join()

if __name__ == "__main__":
    create_database()
    create_tables()
    parser = argparse.ArgumentParser()
    parser.add_argument("interval", type=int, help="Interval in minutes for the automation task")
    args = parser.parse_args()
    automate(args.interval)