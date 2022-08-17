import os
import re
import sqlite3
from sqlite3 import Error
import pandas as pd


def create_db(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn):
    sql = """CREATE TABLE IF NOT EXISTS convoy (
                vehicle_id integer PRIMARY KEY,
                engine_capacity integer NOT NULL,
                fuel_consumption integer NOT NULL, 
                maximum_load integer NOT NULL
            ); """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def add_row(conn, row):
    sql = ''' INSERT INTO convoy(vehicle_id,engine_capacity,fuel_consumption,maximum_load)
                VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, row)
    conn.commit()

def add_data(df, conn, fn):
    cnt = 0
    for i, row in df.iterrows():
        # print(row.values)
        add_row(conn, row)
        cnt += 1
    print(f'{cnt} record{"s were" if cnt > 1 else " was"} inserted into {fn}')

def select_all(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM convoy")
    rows = cur.fetchall()
    for row in rows:
        print(row)

def read_data(path):
    fn = path.split(os.sep)[-1]
    f = fn.split('.')

    if f[-1] == 'xlsx':
        df = pd.read_excel(path, sheet_name='Vehicles', dtype=str)
        fn = f[-2] + '.csv'
        df.to_csv(fn, index=False)
        n = df.shape[0]
        print(f'{n} line{"s were" if n > 1 else " was"} added to {fn}')
    else:
        df = pd.read_csv(path, dtype=str)

    return df

def edit_data(df, path):
    f = path.split(os.sep)[-1].split('.')
    if path.find('[CHECKED]') == -1:
        cnt = 0
        for r in range(df.shape[0]):
            for c in range(df.shape[1]):
                cell = df.iloc[r, c]
                new = re.sub(r"[\D]", "", cell)
                if new != cell:
                    cnt += 1
                    df.iloc[r, c] = new

        fn = f'{f[-2]}[CHECKED].csv'
        df.to_csv(fn, index=False)
        print(f'{cnt} cell{"s were" if cnt > 1 else " was"} corrected in {f[-2]}[CHECKED].csv')

    return df

def get_path():
    path = input('Input file name\n')
    # path = '../test/data_big_chk[CHECKED].csv'
    path = os.path.normpath(path)
    return path


p = get_path()
df = read_data(p)
df = edit_data(df, p)

f = p.split(os.sep)[-1].split('.')
db_name = f[-2] + '.s3db'
db_name = re.sub('\[CHECKED\]', '', db_name)
conn = create_db(db_name)
create_table(conn)
add_data(df, conn, db_name)

# select_all(conn)

if conn:
    conn.close()
