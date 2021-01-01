CREATE TABLE Items(
    id INTEGER PRIMARY KEY,
    deleted BOOLEAN,
    type TEXT,
    by TEXT,
    time INTEGER,
    text TEXT,
    dead BOOLEAN,
    parent INTEGER,
    url TEXT,
    score INTEGER,
    title TEXT,
    descendants INTEGER
);
