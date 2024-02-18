import os
import psycopg2
import sqlite3
import uuid
from datetime import datetime
from contextlib import contextmanager
from dataclasses import dataclass, field, astuple

now = datetime.utcnow()
db_path = os.environ.get("DB_NAME_SQL")

dsn = {
    "dbname": os.environ.get("DB_NAME_POSTGR"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
    "options": "-c search_path=content",
}


# Подключение SQLite
@contextmanager
def conn_sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


# Подключение Postgres
@contextmanager
def conn_postgres(**dsn):
    conn = psycopg2.connect(**dsn)
    try:
        yield conn
    finally:
        conn.close()


@dataclass
class Film_work:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    title: str = None
    description: str = None
    creation_date: datetime.date = None
    file_path: str = None
    rating: float = field(default=0.0)
    type: str = field(default="movie")
    created_at: datetime = field(default=now)
    updated_at: datetime = field(default=now)


@dataclass
class Genre:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = None
    description: str = None
    created_at: datetime = field(default=now)
    updated_at: datetime = field(default=now)


@dataclass
class Genre_film_work:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default=now)


@dataclass
class Person:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    full_name: str = None
    created_at: datetime = field(default=now)
    updated_at: datetime = field(default=now)


@dataclass
class Person_film_work:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    role: str = field(default_factory=["actor", "producer", "director"])
    created_at: datetime = field(default=now)


# Общая функция для формирования листов данных SQLite
def read_data_sqlite():
    with conn_sqlite(db_path) as conn:

        try:
            curs = conn.cursor()    
            query_fw = """
                        SELECT id, REPLACE(REPLACE(title, "'%",""), "\'", "'") AS title, 
                            REPLACE(REPLACE(description, "'%",""), "\'", "'") AS description, 
                            creation_date, file_path, rating, type 
                        FROM film_work"""
            curs.execute(query_fw)
            data_fw = curs.fetchall()
            list_data_fw = []

            for line in data_fw:
                list_data_fw.append(dict(line))

            conn.commit()
        except sqlite3.OperationalError:
            print("Введенная таблица не найдена! (см. film_work)")
        except sqlite3.ProgrammingError:
            print("Нет подключения к БД (см. film_work)")

        try:
   
            query_g = "SELECT * FROM genre"
            curs.execute(query_g)
            data_g = curs.fetchall()
            list_data_g = []

            for line in data_g:
                list_data_g.append(dict(line))

            conn.commit()
        except sqlite3.OperationalError:
            print("Введенная таблица не найдена! (см. genre)")
        except sqlite3.ProgrammingError:
            print("Нет подключения к БД (см. genre)")

        try:
  
            query_gfw = "SELECT * FROM genre_film_work"
            curs.execute(query_gfw)
            data_gfw = curs.fetchall()
            list_data_gfw = []

            for line in data_gfw:
                list_data_gfw.append(dict(line))

            conn.commit()
        except sqlite3.OperationalError:
            print("Введенная таблица не найдена! (см. genre_film_work)")
        except sqlite3.ProgrammingError:
            print("Нет подключения к БД (см. genre_film_work)")
            
        try:
  
            query_p = "SELECT * FROM person"
            curs.execute(query_p)
            data_p = curs.fetchall()
            list_data_p = []

            for line in data_p:
                list_data_p.append(dict(line))
            
            conn.commit()    
        except sqlite3.OperationalError:
            print("Введенная таблица не найдена! (см. person)")
        except sqlite3.ProgrammingError:
            print("Нет подключения к БД (см. person)")
            
        try:
  
            query_pfw = "SELECT * FROM person_film_work"
            curs.execute(query_pfw)
            data_pfw = curs.fetchall()
            list_data_pfw = []

            for line in data_pfw:
                list_data_pfw.append(dict(line))
      
        except sqlite3.OperationalError:
            print("Введенная таблица не найдена! (см. person_film_work)")
        except sqlite3.ProgrammingError:
            print("Нет подключения к БД (см. person_film_work)")


    return list_data_fw, list_data_g, list_data_gfw,\
            list_data_p, list_data_pfw
    
    
def create_data_list():

    # Сначала формируем независимые от других таблиц
    data = read_data_sqlite()

    data_genre = data[1]
    genre_list = []
    genre_ids = []

    for genre in data_genre:
        genre_list.append(Genre(**genre))
        genre_ids.append(genre["id"])

    data_person = data[3]
    person_list = []
    person_ids = []

    for person in data_person:
        person_list.append(Person(**person))
        person_ids.append(person["id"])

    data_film_work = data[0]
    film_work_list = []
    film_works_ids = []

    for film_work in data_film_work:
        film_work_list.append(Film_work(**film_work))
        film_works_ids.append(film_work["id"])

    # Теперь связующие таблицы, проверяя связь между записями
    data_genre_film_work = data[2]
    genre_film_work_list = []
    stop_list_gfw = []
    
    for genre_film_work in data_genre_film_work:
        # print(genre_film_work['id'])
        if (genre_film_work["film_work_id"] in film_works_ids) & (
            genre_film_work["genre_id"] in genre_ids
        ):
            genre_film_work_list.append(Genre_film_work(**genre_film_work))
        else:
            stop_list_gfw.append(genre_film_work)

    if len(stop_list_gfw) > 0:
        print("Нарушают связь между записями (genre_film_work):", stop_list_gfw)

    data_person_film_work = data[4]
    person_film_work_list = []
    stop_list_pfw = []
    
    for person_film_work in data_person_film_work:
        # print(genre_film_work['id'])
        if (person_film_work["film_work_id"] in film_works_ids) & (
            person_film_work["person_id"] in person_ids
        ):
            person_film_work_list.append(Person_film_work(**person_film_work))
        else:
            stop_list_pfw.append(person_film_work)

    if len(stop_list_pfw) > 0:
        print("Нарушают связь между записями (person_film_work):", stop_list_pfw)

    return (
        genre_list,
        person_list,
        film_work_list,
        genre_film_work_list,
        person_film_work_list,
    )


#############################################
### Записываем в Postgres
with conn_postgres(**dsn) as conn, conn.cursor() as cursor:
    Data = create_data_list()

    # Запись жанров
    genres = Data[0]

    bind_values_g = ",".join(
        cursor.mogrify(f"(%s, %s, %s, %s, %s)", astuple(genre)).decode("utf-8")
        for genre in genres
    )
    # При повторной загрузке, значения обновятся
    try:
        query_g = (
            f"INSERT INTO content.genre (id, name, description,"
            f"created, modified)"
            f"VALUES {bind_values_g} ON CONFLICT (id)"
            f"DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description;"
            f"UPDATE content.genre SET created = NOW(), modified = NOW()"
        )
        cursor.execute(query_g)
        conn.commit()
        print("Запись/обновление таблицы genre выполнено")
    except psycopg2.errors.UndefinedColumn:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.genre")
    except psycopg2.errors.NotNulSyntaxErrorlViolatio:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.genre")
    except psycopg2.errors.NotNullViolatio:
        print("Ошибка записи. Несовпадение сущностей таблиц, см. запись content.genre")

    # Запись персон
    persons = Data[1]

    bind_values_p = ",".join(
        cursor.mogrify(f"(%s, %s, %s, %s)", astuple(person)).decode("utf-8")
        for person in persons
    )
    try:
        # Для правильного времени, в Posgres настроено время: SET TIME ZONE 'Europe/Moscow';
        query_p = (
            f"INSERT INTO content.person (id, full_name, created, modified)"
            f"VALUES {bind_values_p} "
            f" ON CONFLICT (id) DO UPDATE SET full_name = EXCLUDED.full_name;"
            f"UPDATE content.person SET created = NOW(), modified = NOW()"
        )
        cursor.execute(query_p)
        conn.commit()
        print("Запись/обновление таблицы person выполнено")
    except psycopg2.errors.UndefinedColumn:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.person")
    except psycopg2.errors.NotNulSyntaxErrorlViolatio:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.person")
    except psycopg2.errors.NotNullViolatio:
        print("Ошибка записи. Несовпадение сущностей таблиц, см. запись content.person")

    # Запись фильмов
    films = Data[2]

    bind_values_fw = ",".join(
        cursor.mogrify(f"(%s, %s, %s, %s, %s, %s, %s, %s, %s)", astuple(film)).decode(
            "utf-8"
        )
        for film in films
    )
    try:
        query_fw = (
            f"INSERT INTO content.film_work(id, title, description, creation_date,"
            f" file_path, rating, type, created, modified)"
            f"VALUES {bind_values_fw} ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title,"
            f" description = EXCLUDED.description, creation_date = EXCLUDED.creation_date,"
            f" file_path = EXCLUDED.file_path, rating = EXCLUDED.rating, type = EXCLUDED.type;"
            f"UPDATE content.film_work SET created = NOW(), modified = NOW()"
        )
        
        cursor.execute(query_fw)
        conn.commit()
        print("Запись/обновление таблицы film_work выполнено")
    except psycopg2.errors.UndefinedColumn:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.film_work")
    except psycopg2.errors.NotNulSyntaxErrorlViolatio:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.film_work")
    except psycopg2.errors.NotNullViolatio:
        print(
            "Ошибка записи. Несовпадение сущностей таблиц, см. запись content.film_work"
        )

    # Запись связующей таблицы фильмов и жанров
    genres_films = Data[3]

    bind_values_gfw = ",".join(
        cursor.mogrify(f"(%s, %s, %s, %s)", astuple(genre_film)).decode("utf-8")
        for genre_film in genres_films
    )
    try:
        query_gfw = (
            f"INSERT INTO content.genre_film_work(id, film_work_id,"
            f" genre_id, created)  "
            f"VALUES {bind_values_gfw}ON CONFLICT (id) DO UPDATE "
            f" SET film_work_id = EXCLUDED.film_work_id, genre_id = EXCLUDED.genre_id;"
            f"UPDATE content.genre_film_work SET created = NOW()"
        )
        cursor.execute(query_gfw)
        conn.commit()
        print("Запись/обновление таблицы genre_film_work выполнено")
    except psycopg2.errors.UndefinedColumn:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.genre_film_work")
    except psycopg2.errors.NotNulSyntaxErrorlViolatio:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.genre_film_work")
    except psycopg2.errors.NotNullViolatio:
        print(
            "Ошибка записи. Несовпадение сущностей таблиц, см. запись content.genre_film_work"
        )

    # Запись связующей таблицы фильмов и персон
    persons_films = Data[4]

    bind_values_pfw = ",".join(
        cursor.mogrify(f"(%s, %s, %s, %s, %s)", astuple(person_film)).decode("utf-8")
        for person_film in persons_films
    )
    try:
        
        query_pfw = (
            f"INSERT INTO content.person_film_work(id, film_work_id, person_id,"
            f" role, created) "
            f"VALUES {bind_values_pfw} ON CONFLICT (id) DO UPDATE "
            f"SET film_work_id = EXCLUDED.film_work_id,person_id = EXCLUDED.person_id," 
            f"role = EXCLUDED.role;"
            f"UPDATE content.person_film_work SET created = NOW()"
        )
        cursor.execute(query_pfw)
        print("Запись/обновление таблицы person_film_work выполнено")

    except psycopg2.errors.UndefinedColumn:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.person_film_work")
    except psycopg2.errors.NotNulSyntaxErrorlViolatio:
        print("Ошибка записи. Ошибка синтаксиса, см. запись content.person_film_work")
    except psycopg2.errors.NotNullViolatio:
        print(
            "Ошибка записи. Несовпадение сущностей таблиц, см. запись content.person_film_work"
        )
