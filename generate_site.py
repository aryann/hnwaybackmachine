import collections
import logging
import os
import pathlib
import itertools
import jinja2
import shutil
import sqlite3
import sys
import tempfile

_OUTPUT_DIR = 'site'
_DIR = os.path.dirname(os.path.realpath(__file__))
_GENERATED_DIR = os.path.join(_DIR, 'generated')


def get_stories(conn):
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""\
        SELECT
            strftime('%Y-%m-%d', time, 'unixepoch') AS day, id,
            title, url, by, score, descendants
        FROM items
        WHERE
            type = 'story' AND
            day > 0 AND
            title IS NOT NULL AND
            (dead IS NULL or NOT dead)
        ORDER BY day, score DESC""")
    yield from rows


def group_by_day(rows):
    batch = []
    for row in rows:
        if not batch:
            batch.append(row)
            continue
        if batch[-1]['day'] != row['day']:
            yield batch
            batch = []
        batch.append(row)
    if batch:
        yield batch


def render_day(template, dest, stories):
    day = stories[0]['day']
    dir = os.path.join(dest, *day.split('-'))
    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
    with open(os.path.join(dir, 'index.html'), 'w') as f:
        year, month, day = day.split('-')
        breadcrumbs = [('root', None, '../../../'),
                       ('year', year, '../../'),
                       ('month', month, '../'),
                       ('day', day, None)]
        f.write(template.render(stories=stories, breadcrumbs=breadcrumbs))
    logging.info('wrote %s', dir)


def split_dates(dates):
    result = collections.defaultdict(lambda: collections.defaultdict(list))
    for date in dates:
        year, month, day = date.split('-')
        result[year][month].append(day)
    return result


def render_indices(template, dest, dates):
    dates = split_dates(dates)
    with open(os.path.join(dest, 'index.html'), 'w') as f:
        f.write(template.render(items=dates.keys(), breadcrumbs=[]))

    for year, months in dates.items():
        with open(os.path.join(dest, year, 'index.html'), 'w') as f:
            breadcrumbs = [('root', None, '../'), ('year', year, None)]
            f.write(template.render(items=months, breadcrumbs=breadcrumbs))

        for month, days in months.items():
            breadcrumbs = [('root', None, '../../'),
                           ('year', year, '../'), ('month', month, None)]
            with open(os.path.join(dest, year, month, 'index.html'), 'w') as f:
                f.write(template.render(items=days, breadcrumbs=breadcrumbs))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = sys.argv[1]
    max_days = int(sys.argv[2]) if len(sys.argv) == 3 else None
    conn = sqlite3.connect(db)

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(_DIR, 'templates')),
        autoescape=jinja2.select_autoescape(['html'])
    )
    stories_template = env.get_template('stories.html')
    index_template = env.get_template('index.html')

    temp_dir = tempfile.mkdtemp()

    stories_by_day = group_by_day(get_stories(conn))
    if max_days:
        stories_by_day = itertools.islice(stories_by_day, max_days)

    dates = []
    for stories in stories_by_day:
        dates.append(stories[0]['day'])
        render_day(stories_template, temp_dir, stories)

    render_indices(index_template, temp_dir, dates)

    shutil.rmtree(_GENERATED_DIR)
    os.replace(temp_dir, _GENERATED_DIR)
