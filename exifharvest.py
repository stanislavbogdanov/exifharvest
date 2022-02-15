# exifharvest 0.1
# 16.02.2022
# Stanislav Bogdanov

from re import sub
import pyexiv2
import os
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype
import re

def eval_expr(x):
    if is_numeric_dtype(x):
        return x
    try:
        return eval(x)
    except (SyntaxError, TypeError):
        return np.nan


df = pd.DataFrame(columns=[
    'Filename',
    'IsJPEG',
    'Exif.Photo.DateTimeOriginal',
    'Exif.Image.Orientation',
    'Exif.Photo.FocalLength',
    'Exif.Photo.ExposureTime',
    'Exif.Photo.FNumber',
    'Exif.Photo.ISOSpeedRatings',
    'Exif.Photo.ExposureBiasValue',
    'Exif.Photo.MeteringMode',
    'Exif.Photo.Flash',
    'Exif.Photo.ExposureProgram',
    'Exif.Image.Make',
    'Exif.Image.Model'
])
for cur_dir, dirs, files in os.walk("E:\\_Foto"):
    # Только директории с именем типа ____2015
    if not re.search(r'\\_+\d{4}', cur_dir): continue

    print(cur_dir)
    for f in files:
        f_ext = f.split('.')[-1]
        if f_ext.lower() in ['jpg', 'pef', 'dng']: 
            full_name = os.path.join(cur_dir, f)
            try:
                with pyexiv2.Image(full_name, encoding='mbcs') as img:
                    data = img.read_exif()
                    if len(data) > 0:
                        data['Filename'] = f
                        data['IsJPEG'] = '1' if f_ext == 'jpg' else '0'
                        df = df.append(data, ignore_index=True)
            except:
                print('Wrong EXIF in', full_name)
                continue

if df.empty: 
    print('Пустой датасет')
    exit(0)


# Убрать пробелы
df = df.apply(lambda x: x.str.strip(), axis=0)

# Очистить от дублей (по времени съемки), RAW имеет приоритет
df['Exif.Photo.DateTimeOriginal'] = pd.to_datetime(df['Exif.Photo.DateTimeOriginal'], format='%Y:%m:%d %H:%M:%S', errors='coerce')
df = df.dropna(subset=['Exif.Photo.DateTimeOriginal'])
df = df.sort_values(['Exif.Photo.DateTimeOriginal', 'IsJPEG'])
df = df.drop_duplicates(subset=['Exif.Photo.DateTimeOriginal'], keep='first')

# Убрать пробелы
# df = df.apply(lambda x: x.str.strip(), axis=0)

# Оставить нужные столбцы и переименовать их
df = df[[
    'Exif.Photo.DateTimeOriginal',
    'Filename',
    'Exif.Image.Orientation',
    'Exif.Photo.FocalLength',
    'Exif.Photo.ExposureTime',
    'Exif.Photo.FNumber',
    'Exif.Photo.ISOSpeedRatings',
    'Exif.Photo.ExposureBiasValue',
    'Exif.Photo.MeteringMode',
    'Exif.Photo.Flash',
    'Exif.Photo.ExposureProgram',
    'Exif.Image.Make',
    'Exif.Image.Model'
]]
df.columns = df.columns.to_series().astype(str).str.replace(r'Exif\.(Photo|Image)\.', '', regex=True)

df.insert(3, 'Horizontal', df['Orientation'].isin(list('1234')).astype(int))
df['FocalLength'] = df['FocalLength'].apply(eval_expr)
df.insert(6, 'ExposureTimeDN', (1.0 / df['ExposureTime'].apply(eval_expr)).round(0))
df['FNumber'] = df['FNumber'].apply(eval_expr)
df.insert(10, 'ExposureBiasValueN', df['ExposureBiasValue'].apply(eval_expr))

df['ISOSpeedRatings'] = df['ISOSpeedRatings'].apply(eval_expr)
df['MeteringMode'] = df['MeteringMode'].apply(eval_expr)
df['Flash'] = df['Flash'].apply(eval_expr)
df['ExposureProgram'] = df['ExposureProgram'].apply(eval_expr)

# print(df)
# print()
# print(df.info())

df.to_csv('fotobase.csv', sep=';', index=False)
print('Всего файлов:', df.shape[0])
