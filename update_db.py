import asyncio
import logging
import requests
import sqlite3
import sys


async def find_gaps(conn, high_water_mark, queue):
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
            await queue.put(id)


def find_remote_high_water_mark():
    response = requests.get(
        'https://hacker-news.firebaseio.com/v0/maxitem.json')
    return int(response.text)


async def get_item(id):
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(
        None, requests.get,
        f'https://hacker-news.firebaseio.com/v0/item/{id}.json')
    response = await future
    return response.json()


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


async def get_and_save_item(conn, queue, task_id):
    while True:
        id = await queue.get()
        item = await get_item(id)
        save_item(conn, item)
        logging.info('task %4d: committed item with id %d',
                     task_id, item['id'])
        queue.task_done()


async def run(conn, num_consumers):
    remote_high_water_mark = find_remote_high_water_mark()
    logging.info('remote high water mark: %d', remote_high_water_mark)
    queue = asyncio.Queue(maxsize=10000)

    producer = asyncio.create_task(
        find_gaps(conn, remote_high_water_mark, queue))
    consumers = [asyncio.create_task(
        get_and_save_item(conn, queue, task_id))
        for task_id in range(num_consumers)]

    await asyncio.gather(producer)
    await queue.join()
    for consumer in consumers:
        consumer.cancel()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = sys.argv[1]
    num_consumers = int(sys.argv[2])

    conn = sqlite3.connect(db)
    try:
        asyncio.run(run(conn, num_consumers))
    finally:
        conn.close()
