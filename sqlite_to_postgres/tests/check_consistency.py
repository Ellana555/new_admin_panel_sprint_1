import os
import psycopg2
import sqlite3
from datetime import datetime
from contextlib import contextmanager
from split_settings.tools import include
from dotenv import load_dotenv

load_dotenv()

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

now = datetime.utcnow()

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


# Запросы SQLite, без времени, так как оно было исправлено на актуальное
def data_sqlite():
    with conn_sqlite(db_path) as conn:
        try:

            curs = conn.cursor()
            query_g = "SELECT id, name, description FROM genre ORDER BY id"
            curs.execute(query_g)
            data_g = curs.fetchall()
            list_data_g = []
            for line in data_g:
                list_data_g.append(tuple(line))
            conn.commit()

            query_gfw = "SELECT id, film_work_id, genre_id FROM genre_film_work ORDER BY id"
            curs.execute(query_gfw)
            data_gfw = curs.fetchall()
            list_data_gfw = []
            for line in data_gfw:
                list_data_gfw.append(tuple(line))
            conn.commit()

            query_fw = """
                        SELECT id, title, description, creation_date,
                            file_path, rating, type 
                        FROM film_work
                        ORDER BY id """
            
            curs.execute(query_fw)
            data_fw = curs.fetchall()
            list_data_fw = []

            for line in data_fw:
                list_data_fw.append(tuple(line))
            conn.commit()

            query_pfw = """
                SELECT id, film_work_id, person_id, role FROM person_film_work ORDER BY id
                """
            curs.execute(query_pfw)
            data_pfw = curs.fetchall()
            list_data_pfw = []

            for line in data_pfw:
                list_data_pfw.append(tuple(line))
            conn.commit()

            query_p = "SELECT id, full_name FROM person ORDER BY id"
            curs.execute(query_p)
            data_p = curs.fetchall()
            list_data_p = []

            for line in data_p:
                list_data_p.append(tuple(line))
            conn.commit()

            return list_data_g, list_data_gfw, list_data_fw, list_data_pfw,  list_data_p
        except sqlite3.OperationalError:
            print("Введенная таблица не найдена!")
        except sqlite3.ProgrammingError:
            print("Нет подключения к БД")


# Общая функция для формирования листов данных SQLite
def data_postgres():
    with conn_postgres(**dsn) as conn, conn.cursor() as curs:
        try:

            curs = conn.cursor()
            query_g = "SELECT id, name, description FROM content.genre ORDER BY id"
            curs.execute(query_g)
            data_g = curs.fetchall()
            conn.commit()

            query_gfw = """
                SELECT id, film_work_id, genre_id FROM content.genre_film_work ORDER BY id
                """
            curs.execute(query_gfw)
            data_gfw = curs.fetchall()
            conn.commit()

            query_fw = """
                        SELECT id, title, description, 
                        creation_date, file_path, rating, type 
                        FROM content.film_work
                        ORDER BY id"""
            curs.execute(query_fw)
            data_fw = curs.fetchall()
            conn.commit()

            query_pfw = """
                    SELECT id, film_work_id, person_id, role FROM person_film_work ORDER BY id
                """
            curs.execute(query_pfw)
            data_pfw = curs.fetchall()
            conn.commit()

            query_p = "SELECT id, full_name FROM content.person ORDER BY id"
            curs.execute(query_p)
            data_p = curs.fetchall()
            conn.commit()
            return data_g, data_gfw, data_fw, data_pfw,  data_p
        except psycopg2.errors.UndefinedColumn:
            print("Ошибка записи. Ошибка синтаксиса, см. запись")
        except psycopg2.errors.NotNulSyntaxErrorlViolatio:
            print("Ошибка записи. Ошибка синтаксиса, см. запись")
        except psycopg2.errors.NotNullViolatio:
            print("Ошибка записи. Несовпадение сущностей таблиц, см. запись")



def сheck_quantity():
 
    data_lite = data_sqlite()
    data_post = data_postgres()

    assert len(data_lite[0]) == len(data_post[0])
    assert len(data_lite[1]) == len(data_post[1])
    assert len(data_lite[2]) == len(data_post[2])
    assert len(data_lite[3]) == len(data_post[3])
    assert len(data_lite[4]) == len(data_post[4])
    print('Кол-во строк в таблицах совпадает.')


def сheck_integrity():

    data_lite = data_sqlite()
    data_post = data_postgres()

    assert data_lite[0] == data_post[0]
    assert data_lite[1] == data_post[1]
    assert data_lite[2] == data_post[2]
    assert data_lite[3] == data_post[3]
    assert data_lite[4] == data_post[4]
    print('Строки в таблицах совпадают.')

    # Для сверки несовпавших значений (поменять на номер)
    dl = data_lite[1]
    dp = data_post[1]
    count = 0

    for i in range(len(data_lite[1])):
        if dl[i] != dp[i]:
            print('Следующие строчки не совпадают', dl[i], dp[i])
            count += 1
            
    print('Всего несовпавших значений', count)


сheck_quantity()
сheck_integrity()


