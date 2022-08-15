import os
import re

import pandas as pd

fn = input('Input file name\n')
# fn = '../test/data_big_xlsx.xlsx'
path = os.path.normpath(fn)
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
