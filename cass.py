import uuid
import time
import threading

import cql

_local = threading.local()
try:
    conn = _local.conn
except AttributeError:
    conn = _local.conn = cql.connect('localhost', 9160, 'twissandra')
    

__all__ = ['get_user_by_username', 'get_friend_usernames',
    'get_follower_usernames',
    'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friend', 'DatabaseError',
    'NotFound', 'InvalidDictionary', 'PUBLIC_USERLINE_KEY']

# NOTE: Having a single userline key to store all of the public tweets is not
#       scalable.  Currently, Cassandra requires that an entire row (meaning
#       every column under a given key) to be able to fit in memory.  You can
#       imagine that after a while, the entire public timeline would exceed
#       available memory.
#
#       The fix for this is to partition the timeline by time, so we could use
#       a key like !PUBLIC!2010-04-01 to partition it per day.  We could drill
#       down even further into hourly keys, etc.  Since this is a demonstration
#       and that would add quite a bit of extra code, this excercise is left to
#       the reader.
PUBLIC_USERLINE_KEY = '!PUBLIC!'


class DatabaseError(Exception):
    """
    The base error that functions in this module will raise when things go
    wrong.
    """
    pass


class NotFound(DatabaseError):
    pass


class InvalidDictionary(DatabaseError):
    pass

def _get_line(cf, username, start, limit):
    """
    Gets a timeline or a userline given a username, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)

    # We get one more tweet than asked for, and if we exceed the limit by doing
    # so, that tweet's key (timestamp) is returned as the 'next' key for
    # pagination.
    cursor = conn.cursor()
    cursor.execute("SELECT FIRST %s REVERSED :start..'' FROM %s WHERE uname = :uname" % (limit + 1, cf), 
                   {'start': start or '', 'uname': username}) # TODO do we need "or ''" still?
    row = cursor.fetchone()
    if row is None:
        return [], None

    if len(row) > limit:
        next = cursor.description[-1][0]
    else:
        next = None

    tweets = []
    # Now we do a manual join to get the tweets themselves
    for tweet_id in (d[0] for d in cursor.description):
        tweet_cursor = conn.cursor()
        rows = tweet_cursor.execute("SELECT username, body FROM tweets WHERE id = :id", 
                                    {'id': tweet_id})
        tweet_row = tweet_cursor.fetchone()
        tweets.append({'id': tweet_id, 'body': tweet_row[1], 'username': tweet_row[0]})

    return (tweets, next)


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT password FROM users WHERE uname = :uname", 
                   {'uname': username})
    row = cursor.fetchone()
    if row is None:
        raise NotFound('User %s not found' % (username,))
    return {'password': row[0]}

def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT followed FROM following WHERE followed_by = :uname", 
                   {'uname': username})
    return [row[0] for row in cursor]

def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT followed_by FROM following WHERE followed = :uname", 
                   {'uname': username})
    return [row[0] for row in cursor]

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    return _get_line('timeline', username, start, limit)

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    return _get_line('userline', username, start, limit)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT username, body FROM tweets WHERE id = :id", 
                   {'id': tweet_id})
    row = cursor.fetchone()
    if row is None:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    return {'username': row[0], 'body': row[1]}


# INSERTING APIs

def save_user(username, password):
    """
    Saves the user record.
    """
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (uname, password) VALUES (:uname, :pw)",
                   {'pw': password, 'uname': username})

def save_tweet(username, body):
    """
    Saves the tweet record.
    """
    cursor = conn.cursor()
    tweet_id = uuid.uuid1()
    # Insert the tweet, then into the user's timeline, then into the public one
    cursor.execute("INSERT INTO tweets (id, username, body) VALUES (:id, :uname, :body)",
                   {'id': tweet_id, 'uname': username, 'body': body})
    cursor.execute("INSERT INTO userline (uname, :id) VALUES (:uname, '')",
                   {'id': tweet_id, 'uname': username})
    cursor.execute("INSERT INTO userline (uname, :id) VALUES (:uname, '')",
                   {'id': tweet_id, 'uname': PUBLIC_USERLINE_KEY})
    # Get the user's followers, and insert the tweet into all of their streams
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
        cursor.execute("INSERT INTO timeline (uname, :id) VALUES (:uname, '')",
                       {'id': tweet_id, 'uname': follower_username})

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    cursor = conn.cursor()
    for to_username in to_usernames:
        row_id = uuid.uuid1()
        cursor.execute("INSERT INTO following (id, followed, followed_by) VALUES (:id, :to, :from)",
                       {'id': row_id, 'to': to_username, 'from': from_username})

def remove_friend(from_username, to_username):
    """
    Removes a friendship relationship from one user to some others.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM following WHERE followed = :to AND followed_by = :from", 
                   {'to': to_username, 'from': from_username})
    assert cursor.rowcount == 1
    row = cursor.fetchone()
    cursor.execute("DELETE FROM following WHERE key = :id", 
                   {'id': row[0]})
