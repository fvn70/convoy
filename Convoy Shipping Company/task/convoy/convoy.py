import pandas as pd
fn = input('Input file name\n')
df = pd.read_excel(fn, sheet_name='Vehicles', dtype=str)
fn = fn.split('.')[0] + '.csv'
df.to_csv(fn, index=False)
n = df.shape[0]
print(f'{n} line{"s were" if n > 1 else " was"} added to {fn}')
