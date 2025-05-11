import random
import time
import os
from faker import Faker
import pymongo
import more_itertools
import math
from functools import partial
import sys
from dotenv import load_dotenv

DEBUG = True
random.seed(41)
Faker.seed(41)
fake = Faker()
load_dotenv()

DB_TYPE = 'mongodb7'
# DB_TYPE = 'mongodb8'

TEST_INSERT = True
TEST_SELECT = True
TEST_UPDATE = True
TEST_DELETE = True
DELETE = True

NUM_USERS = 10000
NUM_POSTS = 10000
NUM_COMMENTS = 10000
NUM_LIKES = 10000
NUM_FOLLOWERS = 10000
NUM_MESSAGES = 10000

if NUM_FOLLOWERS > NUM_USERS * NUM_USERS - NUM_USERS or NUM_LIKES > NUM_POSTS * NUM_USERS:
    exit(1)

class Database:
    def __init__(self, db_type='mongodb7'):
        self.db_type = db_type
        port = os.getenv("MONGO_7_PORT") if db_type == 'mongodb7' else os.getenv("MONGO_8_PORT")
        self.client = pymongo.MongoClient(f"mongodb://{os.getenv("MONGO_INITDB_ROOT_USERNAME")}:{os.getenv("MONGO_INITDB_ROOT_PASSWORD")}@{os.getenv("DB_HOST")}:{port}/")
        self.db = self.client[os.getenv("MONGO_DB_NAME")]
        self.db.command("profile", 2)
    
    def close(self):
        self.client.close()

    def print_query_time(self):
        print_value = f"{self.db.system.profile.find_one(sort=[("ts", -1)]).get("millis", "N/A")}"
        print(f"{print_value} ms\n")
        return print_value

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
    return [{"user_id": i+1, "username": fake.user_name() + str(time.time()), "email": fake.email() + str(time.time()), "password_hash": fake.sha256(), "profile_picture": fake.image_url(), "bio": fake.text()} for i in progress_bar(range(num_users))]

def generate_posts(users, num_posts):
    return [{"post_id": i+1, "user_id": random.choice(users)["user_id"], "content": fake.text(), "media_url": fake.image_url()} for i in progress_bar(range(num_posts))]

def generate_comments(posts, users, num_comments):
    return [{"comment_id": i+1, "post_id": random.choice(posts)["post_id"], "user_id": random.choice(users)["user_id"], "content": fake.text()} for i in progress_bar(range(num_comments))]

def generate_likes(posts, users, num_likes):
    return [{"like_id": i+1, "post_id": pair[0]["post_id"], "user_id": pair[1]["user_id"]} for i in progress_bar(random.sample(range(len(posts) * len(users)), num_likes)) if (pair := more_itertools.nth_product(i, posts, users))]

def generate_followers(users, num_followers):
    return [{"follower_user_id": pair[0]["user_id"], "following_user_id": pair[1]["user_id"]} for i in progress_bar(random.sample(range(math.comb(len(users), 2)), num_followers)) if (pair := more_itertools.nth_combination(users, 2, i))]

def generate_messages(users, num_messages):
    return [{"message_id": i+1, "sender_id": random.choice(users)["user_id"], "receiver_id": random.choice(users)["user_id"], "content": fake.text()} for i in progress_bar(range(num_messages))]

def insert_data(db, collection_name, data):
    collection = db.db[collection_name]
    collection.insert_many(data)
    db.print_query_time()

def delete_data(db, collection_name):
    collection = db.db[collection_name]
    collection.delete_many({})
    db.print_query_time()

def select_queries(db):
    queries = [
        f"SELECT * FROM Users LIMIT {NUM_USERS * 50 // 100}",
        f"SELECT username, email FROM Users WHERE user_id < {NUM_USERS * 50 // 100}",
        "SELECT COUNT(*) FROM Posts",
        f"SELECT Users.username, Posts.content FROM Users JOIN Posts ON Users.user_id = Posts.user_id LIMIT {NUM_POSTS * 50 // 100}",
        "SELECT Users.username, COUNT(Comments.comment_id) FROM Users JOIN Comments ON Users.user_id = Comments.user_id GROUP BY Users.username",
        f"SELECT Posts.post_id, COUNT(Likes.like_id) AS like_count FROM Posts LEFT JOIN Likes ON Posts.post_id = Likes.post_id GROUP BY Posts.post_id ORDER BY like_count DESC LIMIT {NUM_POSTS * 50 // 100}",
        f"SELECT Users.username FROM Users WHERE EXISTS (SELECT 1 FROM Followers WHERE Followers.follower_user_id = Users.user_id)",
        "SELECT Messages.sender_id, Messages.receiver_id, COUNT(Messages.message_id) FROM Messages GROUP BY Messages.sender_id, Messages.receiver_id HAVING COUNT(Messages.message_id) > 0",
        "SELECT Users.username, Posts.content FROM Users JOIN Posts ON Users.user_id = Posts.user_id JOIN Likes ON Posts.post_id = Likes.post_id GROUP BY Users.username, Posts.content HAVING COUNT(Likes.like_id) > 1",
        "SELECT Users.username, COUNT(Posts.post_id) AS post_count FROM Users JOIN Posts ON Users.user_id = Posts.user_id GROUP BY Users.username HAVING COUNT(Posts.post_id) > 0"
    ]

    mongo_queries = [
        partial(db.db.Users.find().limit, NUM_USERS * 50 // 100),
        partial(db.db.Users.find, {"user_id": {"$lt": NUM_USERS * 50 // 100}}, {"username": 1, "email": 1}),
        partial(db.db.Posts.count_documents, {}),
        partial(db.db.Users.aggregate, [
            {
                "$lookup": {
                    "from": "Posts",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "posts"
                }
            },
            {"$unwind": "$posts"},
            {"$project": {"username": 1, "posts.content": 1}},
            {"$limit": NUM_POSTS * 50 // 100}
        ]),
        partial(db.db.Users.aggregate, [
            {
                "$lookup": {
                    "from": "Comments",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "comments"
                }
            },
            {
                "$project": {
                    "username": 1,
                    "comment_count": {"$size": "$comments"}
                }
            }
        ]),
        partial(db.db.Posts.aggregate, [
            {
                "$lookup": {
                    "from": "Likes",
                    "localField": "post_id",
                    "foreignField": "post_id",
                    "as": "likes"
                }
            },
            {
                "$project": {
                    "post_id": 1,
                    "like_count": {"$size": "$likes"}
                }
            },
            {"$sort": {"like_count": -1}},
            {"$limit": NUM_POSTS * 50 // 100}
        ]),
        partial(db.db.Users.aggregate, [
            {
                "$lookup": {
                    "from": "Followers",
                    "localField": "user_id",
                    "foreignField": "follower_user_id",
                    "as": "followers"
                }
            },
            {
                "$match": {"followers": {"$ne": []}}
            },
            {"$project": {"username": 1}}
        ]),
        partial(db.db.Messages.aggregate, [
            {
                "$group": {
                    "_id": {"sender_id": "$sender_id", "receiver_id": "$receiver_id"},
                    "message_count": {"$sum": 1}
                }
            },
            {"$match": {"message_count": {"$gt": 0}}},
            {
                "$project": {
                    "sender_id": "$_id.sender_id",
                    "receiver_id": "$_id.receiver_id",
                    "message_count": 1,
                    "_id": 0
                }
            }
        ]),
        partial(db.db.Posts.aggregate, [
            {
                "$lookup": {
                    "from": "Likes",
                    "localField": "post_id",
                    "foreignField": "post_id",
                    "as": "likes_info"
                }
            },
            {
                "$match": {
                    "likes_info": { "$ne": [] }
                }
            },
            {
                "$lookup": {
                    "from": "Users",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "user_info"
                }
            },
            {
                "$unwind": "$user_info"
            },
            {
                "$project": {
                    "_id": 0,
                    "username": "$user_info.username",
                    "content": "$content"
                }
            }
        ]),
        partial(db.db.Users.aggregate, [
            {
                "$lookup": {
                    "from": "Posts",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "posts"
                }
            },
            {
                "$project": {
                    "username": 1,
                    "post_count": {"$size": "$posts"}
                }
            },
            {"$match": {"post_count": {"$gt": 0}}}
        ])
    ]

    times_of_the_times = []
    for i, query in enumerate(queries):
        
        print(f"\nQuery {i+1}: {query}")

        times = []
        for _ in range(10):
            mongo_queries[i]()
            times.append(db.print_query_time())
        times_of_the_times.append(times)
    
    print("\nAll times:")
    for i, times in enumerate(times_of_the_times):
        print(f"{i+1} {'\t'.join([f'{float(t):.3f}' for t in times])}\n")
        # print(f"{i+1} {'\t'.join([t for t in times])}\n")


    # for i, query in enumerate(queries):
    #     print(f"Query {i+1}: {query}")
    #     mongo_queries[i]()
    #     db.print_query_time()


from functools import partial

def update_queries(db):
    queries = [
        f"UPDATE Users SET bio = 'Updated bio content' WHERE user_id < {NUM_USERS * 20 // 100}",
        f"UPDATE Posts SET content = 'Updated post content' WHERE post_id < {NUM_POSTS * 20 // 100}",
        f"UPDATE Posts SET media_url = 'https://new-media-url.com' WHERE post_id > {NUM_POSTS * 70 // 100}",
        f"UPDATE Users SET email = 'new.email@example.com' WHERE user_id = {NUM_USERS * 15 // 100}",
        f"UPDATE Messages SET content = 'Updated message content' WHERE message_id < {NUM_MESSAGES * 20 // 100}",
        f"UPDATE Users SET profile_picture = 'https://new-profile-url.com' WHERE user_id < {NUM_USERS * 20 // 100}",
        f"UPDATE Comments SET content = 'Updated comment content' WHERE comment_id < {NUM_COMMENTS * 20 // 100}",
        f"UPDATE Posts SET content = CONCAT(content, ' #UpdatedTag') WHERE user_id < {NUM_USERS * 20 // 100}",
        f"UPDATE Users SET password_hash = 'newpasswordhash' WHERE user_id < {NUM_USERS * 20 // 100}",
        f"UPDATE Posts SET content = CONCAT(content, ' [Archived]') WHERE post_id IN (SELECT post_id FROM Posts ORDER BY post_id LIMIT {NUM_POSTS * 10 // 100})"
    ]

    mongo_queries = [
        partial(db.db.Users.update_many,
                {"user_id": {"$lt": NUM_USERS * 20 // 100}},
                {"$set": {"bio": "Updated bio content"}}),
        
        partial(db.db.Posts.update_many,
                {"post_id": {"$lt": NUM_POSTS * 20 // 100}},
                {"$set": {"content": "Updated post content"}}),
        
        partial(db.db.Posts.update_many,
                {"post_id": {"$gt": NUM_POSTS * 70 // 100}},
                {"$set": {"media_url": "https://new-media-url.com"}}),
        
        partial(db.db.Users.update_one,
                {"user_id": NUM_USERS * 15 // 100},
                {"$set": {"email": "new.email@example.com"}}),
        
        partial(db.db.Messages.update_many,
                {"message_id": {"$lt": NUM_MESSAGES * 20 // 100}},
                {"$set": {"content": "Updated message content"}}),
        
        partial(db.db.Users.update_many,
                {"user_id": {"$lt": NUM_USERS * 20 // 100}},
                {"$set": {"profile_picture": "https://new-profile-url.com"}}),
        
        partial(db.db.Comments.update_many,
                {"comment_id": {"$lt": NUM_COMMENTS * 20 // 100}},
                {"$set": {"content": "Updated comment content"}}),

        partial(db.db.Posts.update_many,
                {"user_id": {"$lt": NUM_USERS * 20 // 100}},
                [{"$set": {"content": {"$concat": ["$content", " #UpdatedTag"]}}}]),

        partial(db.db.Users.update_many,
                {"user_id": {"$lt": NUM_USERS * 20 // 100}},
                {"$set": {"password_hash": "newpasswordhash"}}),

        partial(db.db.Posts.update_many,
                {"post_id": {"$in": list(range(NUM_POSTS * 10 // 100))}},
                [{"$set": {"content": {"$concat": ["$content", " [Archived]"]}}}])
    ]

    times_of_the_times = []
    for i, query in enumerate(queries):
        
        print(f"\nQuery {i+1}: {query}")

        times = []
        for _ in range(10):
            mongo_queries[i]()
            times.append(db.print_query_time())
        times_of_the_times.append(times)
    
    print("\nAll times:")
    for i, times in enumerate(times_of_the_times):
        print(f"{i+1} {'\t'.join([f'{float(t):.3f}' for t in times])}\n")

    # for i, query in enumerate(queries):
    #     print(f"Query {i+1}: {query}")
    #     mongo_queries[i]()
    #     db.print_query_time()

def delete_queries(db):
    queries = [
        f"DELETE FROM Posts WHERE post_id < {NUM_POSTS * 10 // 100}",
        f"DELETE FROM Comments WHERE post_id < {NUM_POSTS * 30 // 100}",
        f"DELETE FROM Users WHERE user_id < {NUM_USERS * 5 // 100}",
        f"DELETE FROM Likes WHERE post_id < {NUM_POSTS * 25 // 100}",
        f"DELETE FROM Messages WHERE sender_id < {NUM_USERS * 10 // 100} AND receiver_id > {NUM_USERS * 90 // 100}",
        f"DELETE FROM Users WHERE user_id IN (SELECT user_id FROM Posts WHERE post_id < {NUM_POSTS * 20 // 100})",
        f"DELETE FROM Followers WHERE following_user_id < {NUM_USERS * 10 // 100}",
        f"DELETE FROM Posts WHERE post_id IN (SELECT post_id FROM Likes WHERE user_id < {NUM_USERS * 10 // 100})",
        f"DELETE FROM Comments WHERE user_id < {NUM_USERS * 25 // 100}",
        f"DELETE FROM Likes WHERE post_id > {NUM_POSTS * 60 // 100} AND user_id < {NUM_USERS * 5 // 100}"
    ]

    mongo_queries = [
        partial(db.db.Posts.delete_many, {
            "post_id": {"$lt": NUM_POSTS * 10 // 100}
        }),

        partial(db.db.Comments.delete_many, {
            "post_id": {"$lt": NUM_POSTS * 30 // 100}
        }),

        partial(db.db.Users.delete_many, {
            "user_id": {"$lt": NUM_USERS * 5 // 100}
        }),

        partial(db.db.Likes.delete_many, {
            "post_id": {"$lt": NUM_POSTS * 25 // 100}
        }),

        partial(db.db.Messages.delete_many, {
            "sender_id": {"$lt": NUM_USERS * 10 // 100},
            "receiver_id": {"$gt": NUM_USERS * 90 // 100}
        }),

        partial(db.db.Users.delete_many, {
            "user_id": {
                "$in": list(
                    db.db.Posts.find(
                        {"post_id": {"$lt": NUM_POSTS * 20 // 100}},
                        {"user_id": 1, "_id": 0}
                    ).distinct("user_id")
                )
            }
        }),

        partial(db.db.Followers.delete_many, {
            "following_user_id": {"$lt": NUM_USERS * 10 // 100}
        }),

        partial(db.db.Posts.delete_many, {
            "post_id": {
                "$in": list(
                    db.db.Likes.find(
                        {"user_id": {"$lt": NUM_USERS * 10 // 100}},
                        {"post_id": 1, "_id": 0}
                    ).distinct("post_id")
                )
            }
        }),

        partial(db.db.Comments.delete_many, {
            "user_id": {"$lt": NUM_USERS * 25 // 100}
        }),

        partial(db.db.Likes.delete_many, {
            "post_id": {"$gt": NUM_POSTS * 60 // 100},
            "user_id": {"$lt": NUM_USERS * 5 // 100}
        })
    ]

    times_of_the_times = []
    for i, query in enumerate(queries):
        
        print(f"\nQuery {i+1}: {query}")

        times = []
        for _ in range(10):
            mongo_queries[i]()
            times.append(db.print_query_time())
        times_of_the_times.append(times)
    
    print("\nAll times:")
    for i, times in enumerate(times_of_the_times):
        print(f"{i+1} {'\t'.join([f'{float(t):.3f}' for t in times])}\n")

    # for i, query in enumerate(queries):
    #     print(f"Query {i+1}: {query}")
    #     mongo_queries[i]()
    #     db.print_query_time()



def run_tests(db_type='mongodb7'):
    db = Database(db_type)
    print(f"Connected to {db_type}")

    if TEST_INSERT:
        print("Testing INSERT:")    

        print(f"Generating {NUM_USERS} Users")
        users = generate_users(NUM_USERS)

        print(f"Inserting {NUM_USERS} Users")
        insert_data(db, 'Users', users)
        
        print(f"Generating {NUM_POSTS} Posts")
        posts = generate_posts(users, NUM_POSTS)

        print(f"Inserting {NUM_POSTS} Posts")
        insert_data(db, 'Posts', posts)
        
        print(f"Generating {NUM_COMMENTS} Comments")
        comments = generate_comments(posts, users, NUM_COMMENTS)

        print(f"Inserting {NUM_COMMENTS} Comments")
        insert_data(db, 'Comments', comments)
        
        print(f"Generating {NUM_LIKES} Likes")
        likes = generate_likes(posts, users, NUM_LIKES)

        print(f"Inserting {NUM_LIKES} Likes")
        insert_data(db, 'Likes', likes)
        
        print(f"Generating {NUM_FOLLOWERS} Followers")
        followers = generate_followers(users, NUM_FOLLOWERS)

        print(f"Inserting {NUM_FOLLOWERS} Followers")
        insert_data(db, 'Followers', followers)
        
        print(f"Generating {NUM_MESSAGES} Messages")
        messages = generate_messages(users, NUM_MESSAGES)

        print(f"Inserting {NUM_MESSAGES} Messages")
        insert_data(db, 'Messages', messages)
        
    if TEST_SELECT:
        print("Testing SELECT:")
        select_queries(db)

    if TEST_UPDATE:
        print("Testing UPDATE")
        update_queries(db)

    if TEST_DELETE:
        print("Testing DELETE")
        delete_queries(db)

    if DELETE:
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
