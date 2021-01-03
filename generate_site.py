import os
import pathlib
import jinja2
import shutil
import sqlite3
import sys
import tempfile

_OUTPUT_DIR = 'site'
_DIR = os.path.dirname(os.path.realpath(__file__))
_GENERATED_DIR = os.path.join(_DIR, 'generated')


def get_dates(conn):
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
        f.write(template.render(stories=stories))


if __name__ == '__main__':
    db = sys.argv[1]
    conn = sqlite3.connect(db)

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.join(_DIR, 'templates')),
        autoescape=jinja2.select_autoescape(['html'])
    )
    template = env.get_template('template.html')

    temp_dir = tempfile.mkdtemp()

    stories_by_day = group_by_day(get_dates(conn))
    for stories in stories_by_day:
        render_day(template, temp_dir, stories)

    shutil.rmtree(_GENERATED_DIR)
    os.replace(temp_dir, _GENERATED_DIR)
