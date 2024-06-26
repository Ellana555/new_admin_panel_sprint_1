CREATE SCHEMA content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid NOT NULL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
	file_path TEXT,
    rating FLOAT,
    type TEXT not null,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid NOT NULL PRIMARY KEY,
	genre_id uuid NOT NULL REFERENCES content.genre (id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
    created timestamp with time zone
);  

CREATE TABLE IF NOT EXISTS content.person (
    id uuid NOT NULL PRIMARY KEY,
	full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid NOT NULL PRIMARY KEY,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
	person_id uuid NOT NULL REFERENCES content.person (id) ON DELETE CASCADE,
    role TEXT,
    created timestamp with time zone
);     

CREATE INDEX film_work_creation_date_rating_idx ON content.film_work(creation_date, rating); 
CREATE INDEX film_work_title_idx ON content.film_work(title); 
CREATE UNIQUE INDEX film_work_person_idx ON content.person_film_work (film_work_id, person_id, role);
CREATE UNIQUE INDEX film_work_genre_idx ON content.genre_film_work (film_work_id, genre_id);