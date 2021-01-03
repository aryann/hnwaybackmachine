CREATE TABLE Items(
    id INTEGER PRIMARY KEY,
    deleted BOOLEAN,
    type TEXT,
    by TEXT,
    time INTEGER,
    text TEXT,
    dead BOOLEAN,
    parent INTEGER,
    poll INTEGER,
    kids TEXT,
    url TEXT,
    score INTEGER,
    title TEXT,
    parts TEXT,
    descendants INTEGER
);
