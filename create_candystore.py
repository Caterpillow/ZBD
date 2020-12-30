import psycopg2
import random

import config as conf


def get_presents():
    return [conf.present_prefix + str(nr) for nr in range(conf.diff_kinds_max)]


def pop_random(lst):
    idx = random.randrange(0, len(lst))
    return lst.pop(idx)


def prepare_candystore(presents):
    conn = psycopg2.connect(user='vaen')
    cursor = conn.cursor()

    for p in presents:
        cursor.execute("""INSERT INTO slodycz_w_magazynie (nazwa, ilosc_pozostalych) VALUES ('{}', {})""" \
                       .format(p, random.randint(1, conf.one_kind_mx)))

    conn.commit()
    conn.close()


def prepare_similar_candy(presents):
    conn = psycopg2.connect(user='vaen')
    cursor = conn.cursor()

    pairs = []
    for i in range(conf.similar_candy):
        if len(presents) < 2:
            break

        rand1 = 0
        rand2 = 0

        while rand1 == rand2:
            rand1 = pop_random(presents)
            rand2 = pop_random(presents)

        pair = rand1, rand2
        pairs.append(pair)

    #add to similar candy db
    for a, b in pairs:
        q = """INSERT INTO podobny_slodycz (slodycz_1, slodycz_2, podobienstwo) VALUES ('{}', '{}', {})"""\
            .format(a, b, random.uniform(0, 1))
        cursor.execute(q)

    conn.commit()
    conn.close()


def clear_db():
    conn = psycopg2.connect(user='vaen')
    cursor = conn.cursor()

    cursor.execute("""DROP TABLE IF EXISTS slodycz_w_paczce""")
    cursor.execute("""DROP TABLE IF EXISTS podobny_slodycz""")
    cursor.execute("""DROP TABLE IF EXISTS paczka""")
    cursor.execute("""DROP TABLE IF EXISTS slodycz_w_magazynie""")

    cursor.execute("""CREATE TABLE slodycz_w_paczce (id_paczki int, slodycz varchar, ilosc int)""")
    cursor.execute("""CREATE TABLE paczka (id serial, kraj varchar, imie varchar)""")
    cursor.execute("""CREATE TABLE slodycz_w_magazynie 
                    (nazwa varchar, ilosc_pozostalych int CHECK(ilosc_pozostalych >= 0))""")
    cursor.execute("""CREATE TABLE podobny_slodycz (slodycz_1 varchar, slodycz_2 varchar, 
                    podobienstwo DOUBLE PRECISION)""")

    conn.commit()
    conn.close()


def main():
    clear_db()

    presents = get_presents()
    prepare_candystore(presents)
    prepare_similar_candy(presents)


main()
