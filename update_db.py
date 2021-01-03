import aiohttp
import asyncio
import logging
import sqlite3
import sys
import time


_COMMIT_BATCH_SIZE = 500


async def find_gaps(conn, start, queue):
    remote_high_water_mark = await find_remote_high_water_mark()
    logging.info('remote high water mark: %d', remote_high_water_mark)
    rows = conn.execute(
        'SELECT id FROM Items WHERE id >= ? ORDER BY id', (start,))
    try:
        next_id = next(rows)[0]
    except StopIteration:
        next_id = None

    for id in range(start, remote_high_water_mark):
        if next_id == id:
            try:
                next_id = next(rows)[0]
            except StopIteration:
                next_id = None
        else:
            await queue.put(id)


async def find_remote_high_water_mark():
    url = 'https://hacker-news.firebaseio.com/v0/maxitem.json'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, raise_for_status=True) as response:
            return int(await response.text())


async def get_item(session, id):
    url = f'https://hacker-news.firebaseio.com/v0/item/{id}.json'
    async with session.get(url, raise_for_status=True) as response:
        return await response.json()


async def get_items(task_id, id_queue, item_queue):
    async with aiohttp.ClientSession() as session:
        while True:
            id = await id_queue.get()
            try:
                item = await get_item(session, id)
            except aiohttp.ClientError as e:
                logging.error('could not get item %d: %s', id, e)
                await id_queue.put(id)
                id_queue.task_done()
                continue

            logging.info('task %4d: fetched item %d', task_id, id)
            await item_queue.put(item)
            id_queue.task_done()


def serialize_list(items):
    if not items:
        return None
    return ','.join(str(item) for item in items)


def save_item(conn, item):
    if not item:
        return

    conn.execute("""\
                    INSERT INTO Items
                    (
                        id,
                        deleted,
                        type,
                        by,
                        time,
                        text,
                        dead,
                        parent,
                        poll,
                        kids,
                        url,
                        score,
                        title,
                        parts,
                        descendants)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 (
                     item['id'],
                     item.get('deleted'),
                     item.get('type'),
                     item.get('by'),
                     item.get('time'),
                     item.get('text'),
                     item.get('dead', False),
                     item.get('parent'),
                     item.get('poll'),
                     serialize_list(item.get('kids')),
                     item.get('url'),
                     item.get('score'),
                     item.get('title'),
                     serialize_list(item.get('parts')),
                     item.get('descendants'),
                 ))


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
            try:
                save_items_in_list(conn, item_buffer)
            except sqlite3.OperationalError as e:
                logging.error('could not commit items: %s', e)
                for item in item_buffer:
                    await item_queue.put(item)
                item_buffer.clear()
                await asyncio.sleep(1)
                continue

            logging.info(
                'committed %d items; write rate: %2.3f items/s',
                _COMMIT_BATCH_SIZE, _COMMIT_BATCH_SIZE / (time.time() - start))
            item_buffer.clear()
            start = time.time()


async def run(conn, num_consumers, start):
    id_queue = asyncio.Queue(maxsize=10000)
    item_queue = asyncio.Queue(maxsize=10000)
    item_buffer = []

    id_producer = asyncio.create_task(
        find_gaps(conn, start, id_queue), name='id-producer')
    item_producers = [asyncio.create_task(
        get_items(task_id, id_queue, item_queue), name=f'item-producer-{task_id}')
        for task_id in range(num_consumers)]
    item_saver = asyncio.create_task(
        save_items(conn, item_queue, item_buffer), name='item-saver')

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
        asyncio.run(run(conn, num_consumers, start), debug=True)
    finally:
        conn.close()
