import random
import time
import os
from faker import Faker
import mysql.connector
import psycopg2
import itertools
import sys
from dotenv import load_dotenv

DEBUG = True
random.seed(41)
Faker.seed(41)
fake = Faker()
load_dotenv()

# DB_TYPE = 'mariadb'
DB_TYPE = 'postgresql'

DEBUG_SELECT = False

TEST_INSERT = True
TEST_SELECT = True
TEST_UPDATE = False
TEST_DELETE = True

NUM_USERS = 100000
NUM_POSTS = 100000
NUM_COMMENTS = 100000
NUM_LIKES = 100000
NUM_FOLLOWERS = 100000
NUM_MESSAGES = 100000

if NUM_FOLLOWERS > NUM_USERS * NUM_USERS - NUM_USERS or NUM_LIKES > NUM_POSTS * NUM_USERS:
    exit(1)

DB_CONFIG = {
    'mariadb': {
        'user': os.getenv("MYSQL_ROOT"),
        'password': os.getenv("MYSQL_ROOT_PASSWORD"),
        'host': os.getenv("DB_HOST"),
        'database': os.getenv("MYSQL_DATABASE"),
        'port': os.getenv("MYSQL_PORT")
    },
    'postgresql': {
        'dbname': os.getenv("POSTGRES_DB"),
        'user': os.getenv("POSTGRES_USER"),
        'password': os.getenv("POSTGRES_PASSWORD"),
        'host': os.getenv("DB_HOST"),
        'port': os.getenv("POSTGRES_PORT")
    }
}

class Database:
    def __init__(self, db_type='mariadb'):
        self.db_type = db_type
        self.conn = self.connect()
        self.cursor = self.conn.cursor()

    def connect(self):
        if self.db_type == 'mariadb':
            return mysql.connector.connect(**DB_CONFIG['mariadb'])
        elif self.db_type == 'postgresql':
            return psycopg2.connect(**DB_CONFIG['postgresql'])
        else:
            raise ValueError("Unsupported DB type")
    
    def execute(self, query, values=None):
        if self.db_type == 'mariadb':
            self.cursor.execute("SET PROFILING = 1;")
            self.cursor.execute(query, values or [])
            if DEBUG_SELECT and query.lower().startswith("select"):
                results = self.cursor.fetchall()
                for row in results:
                    print(row)
            else:
                self.cursor.fetchall()
            self.cursor.execute("SHOW PROFILES;")
            profiles = self.cursor.fetchall()
            print(f"{float(profiles[-1][1]) * 1000:.3f} ms\n")
        else:
            explain_query = f"EXPLAIN ANALYZE {query}"
            self.cursor.execute(explain_query, values or [])
            explain_output = self.cursor.fetchall()
            print(f"{float(explain_output[-1][0].split(" ")[2]):.3f} ms\n")
        return None

    def commit(self):
        self.conn.commit()
    
    def close(self):
        self.cursor.close()
        self.conn.close()

def progress_bar(iterable, prefix='Progress: ', length=50, fill='█'):
    def print_progress(iteration):
        percent = ("{0:.1f}").format(100 * (iteration / len(iterable)))
        filled_length = int(length * iteration // len(iterable))
        bar = fill * filled_length + '-' * (length - filled_length)
        sys.stdout.write(f'\r{prefix} |{bar}| {percent}% Complete')
        sys.stdout.flush()
    start = time.time()
    for i, item in enumerate(iterable, 1):
        print_progress(i)
        yield item
    end = time.time()
    sys.stdout.write('\n')
    sys.stdout.write(f"Generated in {end-start:.3f} seconds\n\n")

def generate_users(num_users):
    return [(i+1, fake.user_name()+str(time.time()), fake.email()+str(time.time()), fake.sha256(), fake.image_url(), fake.text()) for i in progress_bar(range(num_users))]

def generate_posts(users, num_posts):
    return [(i+1, random.choice(users)[0], fake.text(), fake.image_url()) for i in progress_bar(range(num_posts))]

def generate_comments(posts, users, num_comments):
    return [(i+1, random.choice(posts)[0], random.choice(users)[0], fake.text()) for i in progress_bar(range(num_comments))]

def generate_likes(posts, users, num_likes):
    return [(idx, post[0], user[0]) for idx, (post, user) in enumerate(progress_bar(list(itertools.islice(itertools.product(posts, users), num_likes))))]

def generate_followers(users, num_followers):
    return [(pair[0][0], pair[1][0]) for pair in progress_bar(list(itertools.islice(itertools.combinations(users, 2), num_followers)))]

def generate_messages(users, num_messages):
    return [(i+1, random.choice(users)[0], random.choice(users)[0], fake.text()) for i in progress_bar(range(num_messages))]

def insert_data(db, table, columns, data, placeholders):
    values_placeholder = ", ".join([placeholders] * len(data))
    query = f"INSERT INTO {table} {columns} VALUES {values_placeholder}"
    flattened_values = [value for row in data for value in row]
    db.execute(query, flattened_values)

def delete_data(db, table):
    query = f"DELETE FROM {table}"
    db.execute(query)
    db.commit()

def select_queries(db):
    queries = [
        f"SELECT * FROM Users LIMIT {NUM_USERS // 2}",
        f"SELECT username, email FROM Users WHERE user_id < {NUM_USERS // 2}",
        "SELECT COUNT(*) FROM Posts",
        f"SELECT Users.username, Posts.content FROM Users JOIN Posts ON Users.user_id = Posts.user_id LIMIT {NUM_POSTS // 2}",
        "SELECT Users.username, COUNT(Comments.comment_id) FROM Users JOIN Comments ON Users.user_id = Comments.user_id GROUP BY Users.username",
        f"SELECT Posts.post_id, COUNT(Likes.like_id) AS like_count FROM Posts LEFT JOIN Likes ON Posts.post_id = Likes.post_id GROUP BY Posts.post_id ORDER BY like_count DESC LIMIT {NUM_POSTS // 2}",
        "SELECT Users.username FROM Users WHERE EXISTS (SELECT 1 FROM Followers WHERE Followers.follower_user_id = Users.user_id)",
        "SELECT Messages.sender_id, Messages.receiver_id, COUNT(Messages.message_id) FROM Messages GROUP BY Messages.sender_id, Messages.receiver_id HAVING COUNT(Messages.message_id) > 0",
        "SELECT Users.username, Posts.content FROM Users JOIN Posts ON Users.user_id = Posts.user_id WHERE Posts.post_id IN (SELECT post_id FROM Likes GROUP BY post_id HAVING COUNT(user_id) > 0)",
        "SELECT Users.username, COUNT(Posts.post_id) AS post_count FROM Users JOIN Posts ON Users.user_id = Posts.user_id GROUP BY Users.username HAVING COUNT(Posts.post_id) > 0"
    ]
    
    for i, query in enumerate(queries):
        print(f"Query {i+1}: {query}")
        db.execute(query)

def run_tests(db_type='mariadb'):
    db = Database(db_type)
    print(f"Connected to {db_type}")

    if TEST_INSERT:
        print("Testing INSERT:")    

        print(f"Generating {NUM_USERS} Users")
        users = generate_users(NUM_USERS)

        print(f"Inserting {NUM_USERS} Users")
        insert_data(db, 'Users', '(user_id, username, email, password_hash, profile_picture, bio)', users, '(%s, %s, %s, %s, %s, %s)')
        
        print(f"Generating {NUM_POSTS} Posts")
        posts = generate_posts(users, NUM_POSTS)

        print(f"Inserting {NUM_POSTS} Posts")
        insert_data(db, 'Posts', '(post_id, user_id, content, media_url)', posts, '(%s, %s, %s, %s)')
        
        print(f"Generating {NUM_COMMENTS} Comments")
        comments = generate_comments(posts, users, NUM_COMMENTS)

        print(f"Inserting {NUM_COMMENTS} Comments")
        insert_data(db, 'Comments', '(comment_id, post_id, user_id, content)', comments, '(%s, %s, %s, %s)')
        
        print(f"Generating {NUM_LIKES} Likes")
        likes = generate_likes(posts, users, NUM_LIKES)

        print(f"Inserting {NUM_LIKES} Likes")
        insert_data(db, 'Likes', '(like_id, post_id, user_id)', likes, '(%s, %s, %s)')
        
        print(f"Generating {NUM_FOLLOWERS} Followers")
        followers = generate_followers(users, NUM_FOLLOWERS)

        print(f"Inserting {NUM_FOLLOWERS} Followers")
        insert_data(db, 'Followers', '(follower_user_id, following_user_id)', followers, '(%s, %s)')
        
        print(f"Generating {NUM_MESSAGES} Messages")
        messages = generate_messages(users, NUM_MESSAGES)

        print(f"Inserting {NUM_MESSAGES} Messages")
        insert_data(db, 'Messages', '(message_id, sender_id, receiver_id, content)', messages, '(%s, %s, %s, %s)')
        
        db.commit()

    if TEST_SELECT:

        print("Testing SELECT:")

        select_queries(db)


    if TEST_UPDATE:
        pass

    if TEST_DELETE:

        print("Testing DELETE:")

        print(f"Deleting {NUM_MESSAGES} Messages")
        delete_data(db, 'Messages')
        
        print(f"Deleting {NUM_FOLLOWERS} Followers")
        delete_data(db, 'Followers')

        print(f"Deleting {NUM_LIKES} Likes")
        delete_data(db, 'Likes')

        print(f"Deleting {NUM_COMMENTS} Comments")
        delete_data(db, 'Comments')

        print(f"Deleting {NUM_POSTS} Posts")
        delete_data(db, 'Posts')

        print(f"Deleting {NUM_USERS} Users")
        delete_data(db, 'Users')

    db.close()

if __name__ == "__main__":
    run_tests(DB_TYPE)
