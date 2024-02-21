import psycopg2
import sqlite3
from contextlib import contextmanager
from dataclasses import astuple
from split_settings.tools import include


include(
    "components/config.py",
)

include(
    "components/classes_module.py",
)


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
tb_cls = {"film_work": Film_work, "genre": Genre, 
 "genre_film_work": Genre_film_work,
 "person": Person, "person_film_work": Person_film_work}


# Общая функция для формирования листов данных SQLite
def read_data_sqlite(table_name: str):
    with conn_sqlite(db_path) as conn:

        try:

            cls = tb_cls[table_name]
            column_names = [field.name for field in fields(cls)] 
            column_names_str = ",".join(column_names)
            column_names_mod = column_names_str.replace(
                '''title''', '''REPLACE(REPLACE(title, "'%",""), "\'", "'") AS title'''
                )
            column_names_mod = column_names_mod.replace(
                '''description''', '''REPLACE(REPLACE(description, "'%",""), "\'", "'") AS description'''
                )

            curs = conn.cursor()    
            query = ("""
                    SELECT {}
                    FROM {}""".format(column_names_mod, table_name)
            )
            curs.execute(query)
            print(query)

            data = curs.fetchmany(20)
            list_data= []

            for line in data:
                list_data.append(dict(line))

            return list_data

        except sqlite3.OperationalError:
           print("Введенная таблица не найдена! (см. film_work)")
        except sqlite3.ProgrammingError:
           print("Нет подключения к БД (см. film_work)")

    
    
def create_data_list():

    # Сначала формируем независимые от других таблиц

    data_genre = read_data_sqlite('genre')

    genre_list = []
    genre_ids = []

    for genre in data_genre:
        genre_list.append(Genre(**genre))
        genre_ids.append(genre["id"])

    data_person = read_data_sqlite('person')
    person_list = []
    person_ids = []

    for person in data_person:
        person_list.append(Person(**person))
        person_ids.append(person["id"])

    data_film_work = read_data_sqlite('film_work')
    #print(data_film_work)
    film_work_list = []
    film_works_ids = []

    for film_work in data_film_work:
        film_work_list.append(Film_work(**film_work))
        film_works_ids.append(film_work["id"])

    # Теперь связующие таблицы, проверяя связь между записями
    data_genre_film_work = read_data_sqlite('genre_film_work')
    genre_film_work_list = []
    stop_list_gfw = []
    
    for genre_film_work in data_genre_film_work:

        if (genre_film_work["film_work_id"] in film_works_ids) & (
            genre_film_work["genre_id"] in genre_ids
        ):
            genre_film_work_list.append(Genre_film_work(**genre_film_work))
        else:
            stop_list_gfw.append(genre_film_work)

    if len(stop_list_gfw) > 0:
        print("Нарушают связь между записями (genre_film_work):", stop_list_gfw)

    data_person_film_work = read_data_sqlite('person_film_work')
    person_film_work_list = []
    stop_list_pfw = []
    
    # Из той двадцатки по которой проверяет, не может выполнить проверку
     # cделать тут мэин и вызывать функции, читать, менять, проверять и записывать.
    # айдишники комплектовать отдельной фукцией с сортировкой и только id
    # где будут все значения
    # добавить возможность задавать количество пачке

    for person_film_work in data_person_film_work:

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



# БАхнуть мэин и из него всё запускать
##############################################
#### Записываем в Postgres
#def write_data_postgres(table_name: str):
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
            f"VALUES {bind_values_gfw} ON CONFLICT (id) DO UPDATE "
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
    print(persons_films)
    bind_values_pfw = ",".join(
        cursor.mogrify(f"(%s, %s, %s, %s, %s)", astuple(person_film)).decode("utf-8")
        for person_film in persons_films
    )
    print(bind_values_pfw)
    #try:
    query_pfw = (
        f"INSERT INTO content.person_film_work(id, film_work_id, person_id,"
        f" role, created) "
        f"VALUES {bind_values_pfw} ON CONFLICT (id) DO UPDATE "
        f"SET film_work_id = EXCLUDED.film_work_id, person_id = EXCLUDED.person_id," 
        f"role = EXCLUDED.role;"
        f"UPDATE content.person_film_work SET created = NOW()"
    )
    cursor.execute(query_pfw)
    print("Запись/обновление таблицы person_film_work выполнено")
    #except psycopg2.errors.UndefinedColumn:
    #    print("Ошибка записи. Ошибка синтаксиса, см. запись content.person_film_work")
    #except psycopg2.errors.NotNulSyntaxErrorlViolatio:
    #    print("Ошибка записи. Ошибка синтаксиса, см. запись content.person_film_work")
    #except psycopg2.errors.NotNullViolatio:
    #    print(
    #        "Ошибка записи. Несовпадение сущностей таблиц, см. запись content.person_film_work"
    #    )