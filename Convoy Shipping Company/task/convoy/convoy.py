import json
import os
import re
import sqlite3
from sqlite3 import Error
import pandas as pd


def connect_db(db_file):
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
                maximum_load integer NOT NULL,
                score integer NOT NULL
            ); """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def add_row(conn, row):
    sql = ''' INSERT INTO convoy(vehicle_id,engine_capacity,fuel_consumption,maximum_load,score)
                VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, row)
    conn.commit()

def add_data(df, conn, fn):
    cnt = 0
    s = []
    for i, row in df.iterrows():
        r = row.tolist()
        score = calc_score(r)
        s.append(score)
        r += [score]
        add_row(conn, r)
        cnt += 1
    print(f'{cnt} record{"s were" if cnt > 1 else " was"} inserted into {fn}')
    df['score'] = s
    return df

def calc_score(row):
    r = list(map(int, row))
    p1 = 4.5 * r[2] / r[1]
    p2 = 450 * r[2] / 100
    p3 = r[3]
    s = 2 if p1 < 1 else 1 if p1 < 2 else 0
    s += 2 if p2 <= 230 else 1
    s += 2 if p3 >= 20 else 0
    # print(s, p1, p2, p3)
    return s

def select_all(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM convoy")
    rows = cur.fetchall()
    for row in rows:
        print(row)

def read_data(conn, path):
    fn = path.split(os.sep)[-1]
    f = fn.split('.')

    if f[-1] == 's3db':
        df = pd.read_sql_query("SELECT * FROM convoy", conn)
    elif f[-1] == 'xlsx':
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
        print(f'{cnt} cell{"s were" if cnt != 1 else " was"} corrected in {fn}')

    return df

def write_data(df, path, ext):
    fn = path.split(os.sep)[-1].split('.')[-2] + '.' + ext
    fn = re.sub('\[CHECKED\]', '', fn)
    if ext == 'json':
        df1 = df[df['score'] > 3]
        del df1['score']
        out = df1.to_json(orient='records')
        out = f'{{"convoy": {out}}}'
    else:
        df1 = df[df['score'] <= 3]
        del df1['score']
        out = df_to_xml(df1)
    with open(fn, 'w') as f:
        f.write(out)
    cnt = df1.shape[0]
    print(f'{cnt} vehicle{"s were" if cnt != 1 else " was"} saved into {fn}')

def df_to_xml(df):
    xml = ['<convoy>']
    for i, row in df.iterrows():
        xml.append('<vehicle>')
        for fld in row.index:
            xml.append(f'<{fld}>{row[fld]}</{fld}>')
        xml.append('</vehicle>')
    xml.append('</convoy>')
    return ''.join(xml)

def get_path():
    path = input('Input file name\n')
    # path = '../test/data_big_chk[CHECKED].csv'
    path = os.path.normpath(path)
    return path


p = get_path()
f = p.split(os.sep)[-1].split('.')
ext = f[-1]
name = f[-2]

db_name = name + '.s3db'
db_name = re.sub('\[CHECKED\]', '', db_name)
conn = connect_db(db_name)

df = read_data(conn, p)
if ext != 's3db':
    df = edit_data(df, p)
    create_table(conn)
    df = add_data(df, conn, db_name)

write_data(df, p, 'json')
write_data(df, p, 'xml')

if conn:
    conn.close()
