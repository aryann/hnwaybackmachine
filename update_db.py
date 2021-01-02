import asyncio
import logging
import requests
import sqlite3
import sys
import time


_COMMIT_BATCH_SIZE = 100


async def find_gaps(conn, start, high_water_mark, queue):
    rows = conn.execute(
        'SELECT id FROM Items WHERE id >= ? ORDER BY id', (start,))
    try:
        next_id = next(rows)[0]
    except StopIteration:
        next_id = None

    for id in range(start, high_water_mark):
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


async def get_item(session, id):
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(
        None, session.get,
        f'https://hacker-news.firebaseio.com/v0/item/{id}.json')
    response = await future
    return response.json()


async def get_items(task_id, id_queue, item_queue):
    session = requests.Session()
    while not id_queue.empty():
        id = await id_queue.get()
        item = await get_item(session, id)
        logging.info('task %4d: fetched item %d', task_id, id)
        await item_queue.put(item)
        id_queue.task_done()


def save_item(conn, item):
    if not item:
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


def save_items_in_list(conn, item_buffer):
    for item in item_buffer:
        save_item(conn, item)
    conn.commit()


async def save_items(conn, item_queue, item_buffer):
    start = time.time()
    while True:
        item_buffer.append(await item_queue.get())
        item_queue.task_done()

        if len(item_buffer) % _COMMIT_BATCH_SIZE == 0:
            save_items_in_list(conn, item_buffer)
            logging.info(
                'took %.2f seconds to prepare %d items for commit',
                time.time() - start, _COMMIT_BATCH_SIZE)
            item_buffer = []
            start = time.time()


async def run(conn, num_consumers, start):
    remote_high_water_mark = find_remote_high_water_mark()
    logging.info('remote high water mark: %d', remote_high_water_mark)
    id_queue = asyncio.Queue(maxsize=10000)
    item_queue = asyncio.Queue(maxsize=10000)
    item_buffer = []

    id_producer = asyncio.create_task(
        find_gaps(conn, start, remote_high_water_mark, id_queue))
    item_producers = [asyncio.create_task(
        get_items(task_id, id_queue, item_queue))
        for task_id in range(num_consumers)]
    item_saver = asyncio.create_task(
        save_items(conn, item_queue, item_buffer))

    await asyncio.gather(id_producer, *item_producers, item_saver)

    await id_queue.join()
    await item_queue.join()

    for task in item_producers + [item_saver]:
        task.cancel()

    # Commit any outstanding items.
    save_items_in_list(conn, item_buffer)
    logging.info('committed %d final items', len(item_buffer))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = sys.argv[1]
    num_consumers = int(sys.argv[2])
    start = int(sys.argv[3])

    conn = sqlite3.connect(db)
    try:
        asyncio.run(run(conn, num_consumers, start))
    finally:
        conn.close()
