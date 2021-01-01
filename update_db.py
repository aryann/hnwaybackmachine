import logging
import multiprocessing
import multiprocessing.pool
import multiprocessing.context
import requests
import sqlite3
import sys


def find_gaps(in_queue, db, high_water_mark):
    conn = sqlite3.connect(db)
    try:
        rows = conn.execute('SELECT id FROM Items ORDER BY id')
        try:
            next_id = next(rows)[0]
        except StopIteration:
            next_id = None

        for id in range(1, high_water_mark):
            if next_id == id:
                try:
                    next_id = next(rows)[0]
                except StopIteration:
                    next_id = None
            else:
                in_queue.put(id)
    finally:
        conn.close()


def find_remote_high_water_mark():
    response = requests.get(
        'https://hacker-news.firebaseio.com/v0/maxitem.json')
    return int(response.text)


def get_item(in_queue, out_queue):
    while True:
        next = in_queue.get()
        if next is None:
            return
        response = requests.get(
            f'https://hacker-news.firebaseio.com/v0/item/{next}.json')
        out_queue.put(response.json())


def save_items(queue, db):
    conn = sqlite3.connect(db)
    try:
        while True:
            item = queue.get()
            if item is None:
                return

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
    finally:
        conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = sys.argv[1]
    request_threads = int(sys.argv[2])

    remote_high_water_mark = find_remote_high_water_mark()
    logging.info('remote high water mark: %d', remote_high_water_mark)

    in_queue = multiprocessing.Queue(maxsize=request_threads * 100)
    out_queue = multiprocessing.Queue(maxsize=request_threads * 10)

    with multiprocessing.pool.ThreadPool(processes=request_threads + 2) as pool:
        callbacks = []

        callbacks.append(pool.apply_async(
            find_gaps, (in_queue, db, remote_high_water_mark)))
        callbacks.append(pool.apply_async(
            save_items, (out_queue, db)))

        for _ in range(request_threads):
            callbacks.append(pool.apply_async(
                get_item, (in_queue, out_queue)))

        while True:
            for callback in callbacks:
                try:
                    callback.get(timeout=0.1)
                except multiprocessing.context.TimeoutError:
                    continue
