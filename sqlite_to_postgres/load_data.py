from components.config import db_path, dsn
from components.classes_module import (
    Film_work,
    Genre,
    Genre_film_work,
    Person,
    Person_film_work,
)
import logging

logging.basicConfig(
    level = logging.INFO,
    filename = "py_log.log",
    filemode = "w",
    format = "%(asctime)s %(levelname)s %(message)s",
)

import psycopg2
import sqlite3
from contextlib import contextmanager
from dataclasses import astuple
from dataclasses import fields


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


# Связь таблицы с классом
tb_cls = {
    "film_work": Film_work,
    "genre": Genre,
    "genre_film_work": Genre_film_work,
    "person": Person,
    "person_film_work": Person_film_work,
}


# Общая функция для формирования листов данных SQLite
def read_data_sqlite(table_name: str, count: int):
    with conn_sqlite(db_path) as conn:
        try:
            name_t = table_name
            cls = tb_cls[table_name]
            column_names = [field.name for field in fields(cls)]
            column_names_str = ",".join(column_names)
            len_table = len(column_names)
            column_names_mod = column_names_str.replace(
                """title""", """REPLACE(REPLACE(title, "'%",""), "\'", "'") AS title"""
            )
            column_names_mod = column_names_mod.replace(
                """description""",
                """REPLACE(REPLACE(description, "'%",""), "\'", "'") AS description""",
            )

            curs = conn.cursor()
            query = """
                    SELECT {}
                    FROM {}
                    """.format(
                column_names_mod, table_name
            )
            curs.execute(query)
            while True:
                data = curs.fetchmany(count)
                if data:
                    list_data = []

                    for table_name in data:
                        list_data.append(cls(**table_name))

                    write_data_postgres(list_data, len_table, name_t, column_names_str)
                    logging.info(
                        "Writing to the table " + name_t + ". Quantity: " + str(count)
                    )
                    conn.commit()
                else:
                    break

        except sqlite3.OperationalError:
            logging.warning("The entered table was not found!")
        except sqlite3.ProgrammingError:
            logging.debug("There is no connection to the database.")


##############################################
#### Записываем в Postgres
def write_data_postgres(list_data, len_table, name_t, column_names_str):
    with conn_postgres(**dsn) as conn, conn.cursor() as cursor:
        try:
            col_count = ", ".join(["%s"] * len_table)

            bind_values = ",".join(
                cursor.mogrify(f"({col_count})", astuple(line)).decode("utf-8")
                for line in list_data
            )

            column_names_mod = column_names_str.replace("created_at", "created")
            column_names_mod = column_names_mod.replace("updated_at", "modified")

            query = """
                INSERT INTO content.{} ({})
                VALUES {} ON CONFLICT (id)
                DO NOTHING;
                UPDATE content.{} SET created = NOW();
                """.format(
                name_t, column_names_mod, bind_values, name_t
            )

            cursor.execute(query)
            conn.commit()

        except psycopg2.errors.UndefinedColumn:
            logging.exception("Recording error. Syntax error, see the content." + name_t)
        except psycopg2.errors.SyntaxError:
            logging.exception("Recording error. Syntax error, see the content." + name_t)
        except psycopg2.errors.NotNullViolation:
            logging.exception(
                "Recording error.\
                    The discrepancy between the entities of the tables, see content."
                + name_t
            )
        except psycopg2.errors.ForeignKeyViolation:
            logging.debug("ERROR KEY - " + name_t)


read_data_sqlite("genre", 20)
read_data_sqlite("person", 1000)
read_data_sqlite("film_work", 1000)
read_data_sqlite("person_film_work", 1000)
read_data_sqlite("genre_film_work", 1000)
