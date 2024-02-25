import mysql.connector
import json
import numpy as np
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
from gensim.models.doc2vec import Doc2Vec
import argparse
import re
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.util import ngrams
from nltk import pos_tag, ne_chunk
from nltk.tree import Tree
from sklearn.feature_extraction.text import TfidfVectorizer


nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

# Load the trained Doc2Vec model
model = Doc2Vec.load("doc2vec_model")

def clean_text(text):
    """Clean and preprocess text."""
    text = re.sub(r'\W+', ' ', text)  # Remove all non-word characters
    text = text.lower()  # Lowercase
    words = nltk.word_tokenize(text)  # Tokenize
    pos_tags = pos_tag(words)
    # Named Entity Recognition (NER)
    named_entities = ne_chunk(pos_tags, binary=True)
    words = [word for word, tag in pos_tags if word not in stop_words]
    named_entities = ['_'.join(c[0] for c in chunk) for chunk in named_entities if isinstance(chunk, Tree)]
    # Combine words and named entities, removing duplicates
    all_terms = list(set(words + named_entities))
    # Lemmatize words (excluding named entities for simplicity)
    all_terms = [lemmatizer.lemmatize(word) for word in all_terms]
    # Generate n-grams (e.g., bigrams)
    bigrams = ['_'.join(gram) for gram in ngrams(all_terms, 2)]  
    # Combine words, named entities, and bigrams
    final_terms = all_terms + bigrams
    
    return ' '.join(final_terms)


def load_vectors_from_db():
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT id, vector FROM posts")
    vectors = []
    ids = []
    for row in cursor.fetchall():
        ids.append(row[0])
        vectors.append(json.loads(row[1]))
    db.close()
    return ids, np.array(vectors, dtype=np.float32)

def cluster_vectors(vectors, n_clusters=50):
    vectors = vectors.astype(np.float32)  # Convert vectors to float32 if not already
    kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(vectors)
    return kmeans

def visualize_clusters(vectors, labels):
    tsne = TSNE(n_components=2, random_state=0)
    tsne_results = tsne.fit_transform(vectors)
    plt.figure(figsize=(16,10))
    for i in range(max(labels)+1):
        plt.scatter(tsne_results[labels == i, 0], tsne_results[labels == i, 1], label=f'Cluster {i}')
    plt.legend()
    plt.title('t-SNE visualization of post clusters')
    plt.savefig("clusters_visualization.png")  # Save to file
    # plt.show()  # Commented out; use plt.savefig instead

def find_closest_cluster(text, model, kmeans):
    """Find the closest cluster for the given text with debugging information."""
    processed_text = clean_text(text)  # Clean the text
    processed_words = processed_text.split()  # Split into words
    vector = model.infer_vector(processed_words)
    vector_reshaped = np.array(vector, dtype=np.float32).reshape(1, -1)  # Ensure vector is float32 and reshape
    distances = kmeans.transform(vector_reshaped)  # Get distances to all clusters
    print(vector_reshaped.dtype)  # Should output 'float32"
    cluster_label = kmeans.predict(vector_reshaped)[0]
    print(f"Chosen Cluster: {cluster_label}, Distances: {distances}")  # Debugging print
    return cluster_label


def display_posts_from_cluster(cluster_id):
    cluster_id = int(cluster_id)  # Ensure cluster_id is an integer
    print(f"Retrieving posts for cluster {cluster_id}")
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT title, content FROM posts WHERE cluster_id = %s LIMIT 10", (cluster_id,))  # Limit to 10 posts for brevity
    posts = cursor.fetchall()
    print(f"Retrieved {len(posts)} posts")
    if posts:
        print(f"Posts from cluster {cluster_id}:")
        for title, content in posts:
            print(f"Title: {title}")
            print(f"Content: {content[:100]}...")  # Show first 100 characters of content
            print("-" * 50)
    else:
        print(f"No posts found in cluster {cluster_id}.")
    db.close()


def plot_elbow_method(vectors, max_clusters=20):
    wcss = []  # List to store the within-cluster sum of squares
    for i in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0)
        kmeans.fit(vectors)
        wcss.append(kmeans.inertia_)  # Append the WCSS for the current number of clusters
    
    # Plotting the results onto a line graph
    plt.figure(figsize=(10, 6))
    plt.plot(range(1, max_clusters + 1), wcss, marker='o', linestyle='-', color='blue')
    plt.title('Elbow Method For Optimal k')
    plt.xlabel('Number of clusters')
    plt.ylabel('WCSS')  # Within-cluster sum of squares
    plt.xticks(range(1, max_clusters + 1))
    plt.grid(True)
    plt.savefig("elbow_method.png")  # Save the elbow plot
    plt.show()

def print_sample_vectors(vectors, n=5):
    """Prints sample vectors to inspect their diversity."""
    for i in range(n):
        print(f"Vector {i}: {vectors[i][:10]}...")  # Print first 10 elements of the vector

def print_cluster_sizes(labels):
    """Prints the size of each cluster."""
    unique, counts = np.unique(labels, return_counts=True)
    cluster_sizes = dict(zip(unique, counts))
    for cluster, size in cluster_sizes.items():
        print(f"Cluster {cluster}: {size} posts")

def update_cluster_labels_in_db(ids, labels):
    """Update the database with the new cluster labels."""
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    for id, label in zip(ids, labels):
        # Convert label to int to ensure compatibility with MySQL
        label = int(label)  # Convert numpy.int32 to native Python int
        cursor.execute("UPDATE posts SET cluster_id = %s WHERE id = %s", (label, id))
    db.commit()
    cursor.close()
    db.close()

def main():
    ids, vectors = load_vectors_from_db()
# Load text content from DB for TF-IDF
    db = mysql.connector.connect(host="localhost", user="root", password="james", database="ScrapeReddit")
    cursor = db.cursor()
    cursor.execute("SELECT content FROM posts")
    documents = [row[0] for row in cursor.fetchall()]
    db.close()

    print_sample_vectors(vectors)
    
    # plot_elbow_method(vectors, max_clusters=20)  

    n_clusters = 8  
    kmeans = KMeans(n_clusters=n_clusters, random_state=0)
    labels = kmeans.labels_
    try:
        update_cluster_labels_in_db(ids, labels)
    except mysql.connector.Error as err:
        print("Error occurred: {}".format(err))

    print_cluster_sizes(labels)
    visualize_clusters(vectors, labels)
    
    while True:
        query = input("Enter keywords or 'quit' to exit: ")
        if query.lower() == 'quit':
            break
        cluster_id = find_closest_cluster(query, model, kmeans)
        display_posts_from_cluster(cluster_id)

if __name__ == "__main__":
    main()