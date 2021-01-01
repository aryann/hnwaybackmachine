import logging
import requests
import sqlite3
import sys


def find_local_high_water_mark(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT MAX(id) from Items')
    return cursor.fetchone()[0] or 1


def find_remote_high_water_mark():
    response = requests.get(
        'https://hacker-news.firebaseio.com/v0/maxitem.json')
    return int(response.text)


def get_item(id):
    response = requests.get(
        f'https://hacker-news.firebaseio.com/v0/item/{id}.json')
    return response.json()


def get_missing_items(conn):
    local_high_water_mark = find_local_high_water_mark(conn)
    logging.info('local high water mark: %d', local_high_water_mark)
    remote_high_water_mark = find_remote_high_water_mark()
    logging.info('remote high water mark: %d', remote_high_water_mark)

    for id in range(local_high_water_mark + 1, remote_high_water_mark):
        yield get_item(id)


def save_item(conn, item):
    if item.get('type') == 'story':
        conn.execute("""\
            INSERT INTO Items
            (id, type, by, time, url, score, title)
            VALUES
            (?, "story", ?, ?, ?, ?, ?)""",
                     (item['id'], item.get('by'), item.get('time'), item.get('url'),
                      item.get('score'), item.get('title')))
    else:
        conn.execute("""\
            INSERT INTO Items
            (id, type, time)
            VALUES
            (?, ?, ?)""",
                     (item['id'], item.get('type'), item.get('time')))

    conn.commit()
    logging.info('committed item with id %d', item['id'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = sys.argv[1]
    conn = sqlite3.connect(db)
    try:
        for item in get_missing_items(conn):
            save_item(conn, item)
    finally:
        conn.close()
