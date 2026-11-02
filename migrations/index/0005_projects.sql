CREATE TABLE projects (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL CHECK (kind IN ('git_remote','git_dir','path')),
    key        TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    remote     TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX projects_name ON projects(name);

CREATE TABLE project_paths (
    path        TEXT PRIMARY KEY,
    project_id  TEXT NOT NULL REFERENCES projects(id),
    resolved_at INTEGER NOT NULL,
    dead_at     INTEGER
);

ALTER TABLE sessions ADD COLUMN project_id TEXT REFERENCES projects(id);
CREATE INDEX sessions_project ON sessions(project_id);
