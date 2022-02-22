# exifharvest 
# by Stanislav Bogdanov
#
# Release 0.2
# 22.02.2022 

# Description:
#   Collect EXIF for all images in selected folder and save dataset into CSV file. 
#
# Usage:
#   exifharvest.py [-drs] [walkdir] [reportfile]
#
# Keys:
#   -d    Deduplicate by shooting datetime.
#   -r    Preserve RAW formats, if deduplicate (-d key) is specified.
#   -s    Use short and simple list of EXIF fields.
#   -w    Overwrite the report file.
#
# Args:
#   walkdir       Any existing folder with images or current directory by default.
#   reportfile    Name for output report file in CSV format. It defaults to basename of walkdir.

import os
import sys
import pyexiv2
from tqdm import tqdm
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

# List of supported image formats
supp_ext_list = ['jpeg', 'jpg', 'exv', 'cr2', 'crw', 'mrw', 'tiff', 'tif', 'webp', 'dng', 'nef', 'pef', 'arw', 'rw2', 'sr2', 'srw', 'orf', 'png', 'pgf', 'raf', 'eps', 'xmp', 'gif', 'psd', 'tga', 'bmp', 'jp2']

# List of RAW supported image formats
raw_ext_list = ['cr2', 'crw', 'mrw', 'dng', 'nef', 'pef', 'arw', 'rw2', 'sr2', 'srw', 'orf', 'raf']

# Mute pyexiv2 log output
pyexiv2.set_log_level(4)

# Short list of EXIF fields
short_field_list = [
    'Exif.Photo.DateTimeOriginal',
    'Exif.Photo.FocalLength',
    'Exif.Photo.ExposureTime',
    'Exif.Photo.FNumber',
    'Exif.Photo.ISOSpeedRatings',
    'Exif.Photo.ExposureBiasValue',
    'Exif.Photo.MeteringMode',
    'Exif.Photo.Flash',
    'Exif.Photo.ExposureProgram',
    'Exif.Image.Orientation',
    'Exif.Image.Make',
    'Exif.Image.Model'
]

def eval_expr(x):
    """Evaluate symbolic expression"""
    if is_numeric_dtype(x):
        return x
    try:
        return eval(x)
    except (SyntaxError, TypeError):
        return np.nan


def walkdir(folder):
    """Walk through images files in a directory"""
    for dirpath, dirs, files in os.walk(folder):
        for filename in files:
            f_ext = os.path.splitext(filename)[1][1:].lower()
            if f_ext in supp_ext_list:
                yield os.path.abspath(os.path.join(dirpath, filename))

# Default values
gargs = {'d':False, 'r':False, 's':False, 'w':False}
walk_dir = os.path.abspath('.')     # walkdir

# Command args
walk_dir_specified = False
report_file_specified = False
filecounter = 0
for p in range(len(sys.argv)):
    if p == 0: continue
    if sys.argv[p][0] == '-':
        if len(sys.argv[p]) == 1: continue
        for k in sys.argv[p][1:]:
            if k == 'd': 
                gargs['d'] = True
            elif k == 'r': 
                gargs['r'] = True
            elif k == 's': 
                gargs['s'] = True
            elif k == 'w': 
                gargs['w'] = True
            else:
                print('Unknown key', '"' + k + '" will be skipped.')
    else:
        if p == (len(sys.argv) - 1):
            if not walk_dir_specified:
                if os.path.isdir(sys.argv[p]):
                    walk_dir = os.path.abspath(sys.argv[p])
                    walk_dir_specified = True
                    print('Walkdir:', walk_dir)
                else:
                    if sys.argv[p][-4:].lower() == '.csv':
                        report_file = os.path.abspath(sys.argv[p])
                    else:
                        report_file = os.path.abspath(sys.argv[p] + '.csv')
                    report_file_specified = True
                     
            elif not report_file_specified:
                if os.path.splitext(sys.argv[p])[1].lower() == '.csv':
                    report_file = os.path.abspath(sys.argv[p])
                else:
                    report_file = os.path.abspath(sys.argv[p] + '.csv')
                report_file_specified = True

            else:
                print("Can't identify argument", sys.argv[p])
                exit(0)

        elif p == (len(sys.argv) - 2):
            if not walk_dir_specified and os.path.isdir(sys.argv[p]):
                walk_dir = os.path.abspath(sys.argv[p])
                walk_dir_specified = True
                print('Walkdir:', walk_dir)
            else:
                print('Walkdir "' + sys.argv[p] + '"', "doesn't exist")
                exit(0)
    
        else:
            print("Can't identify argument", sys.argv[p])
            exit(0)

if not walk_dir_specified:
    print('Walkdir:', walk_dir)

# Count the files
filecounter = 0
for filepath in walkdir(walk_dir):
    filecounter += 1

if filecounter == 0:
    print('No images to harvest!')
    exit(0)
else:
    print(f'Found {filecounter} file(s).')

# Set and check report file
if not report_file_specified:
    report_file = os.path.abspath(os.path.basename(walk_dir) + '.csv') 

print('Report file:', report_file)
if os.path.isfile(report_file) and not gargs['w']:
    print("Report file exists. Delete it first, or use key -w to allow overwriting.")
    exit(0)

err_count = 0
exif_list = []
for full_name in tqdm(walkdir(walk_dir), total=filecounter, unit="files"):
    try:
        with pyexiv2.Image(full_name, encoding='mbcs') as img:
            data = img.read_exif()
            if len(data) > 0:
                exif = pd.Series(data=data)
                filefolder, filename = os.path.split(full_name)

                if gargs['s']:
                    # Restict EXIF fields
                    exif = exif.reindex(index=short_field_list)

                if exif.isnull().all(): 
                    err_count += 1
                    continue
                exif = exif.reindex(index=['Folder', 'Filename', 'Filetype'] + exif.index.to_list())
                exif['Folder'] = filefolder
                exif['Filename'] = filename
                exif['Filetype'] = os.path.splitext(filename)[1][1:].lower()
                exif_list.append(exif.to_frame().T)
    except:
        err_count += 1
        # continue

if len(exif_list) == 0: 
    print('Empty dataset!')
    exit(0)

df = pd.concat(exif_list, axis=0, ignore_index=True)

# Trim the strings
df = df.apply(lambda x: x.str.strip(), axis=0)

# Datetime parsing
df['Exif.Photo.DateTimeOriginal'] = pd.to_datetime(df['Exif.Photo.DateTimeOriginal'], format='%Y:%m:%d %H:%M:%S', errors='coerce')

# Drop duplicates 
if gargs['d']:
    before_ddup_len = df.shape[0]
    d1 = df[df['Exif.Photo.DateTimeOriginal'].isnull()].copy()
    d2 = df[~df['Exif.Photo.DateTimeOriginal'].isnull()].copy()
    if gargs['r']:
        d2['raw'] = d2['Filetype'].isin(raw_ext_list).fillna(False).astype(int)
        d2 = d2.sort_values(['Exif.Photo.DateTimeOriginal', 'raw'])
        d2 = d2.drop(columns='raw')
    else:
        d2 = d2.sort_values('Exif.Photo.DateTimeOriginal')
    d2 = d2.drop_duplicates(subset=['Exif.Photo.DateTimeOriginal'], keep='last')
    df = pd.concat([d1, d2], axis=0)
    print('Drop duplicates:', before_ddup_len - df.shape[0])

# Data cleaning
df['Horizontal'] = df['Exif.Image.Orientation'].isin(list('1234')).astype(int)
df['Exif.Photo.FocalLength'] = df['Exif.Photo.FocalLength'].apply(eval_expr)
df['ExposureTimeN'] = df['Exif.Photo.ExposureTime'].apply(eval_expr)
df['Exif.Photo.FNumber'] = df['Exif.Photo.FNumber'].apply(eval_expr)
df['ExposureBiasValueN'] = df['Exif.Photo.ExposureBiasValue'].apply(eval_expr)
df['Exif.Photo.ISOSpeedRatings'] = df['Exif.Photo.ISOSpeedRatings'].apply(eval_expr)
df['Exif.Photo.MeteringMode'] = df['Exif.Photo.MeteringMode'].apply(eval_expr)
df['Exif.Photo.Flash'] = df['Exif.Photo.Flash'].apply(eval_expr)
df['Exif.Photo.ExposureProgram'] = df['Exif.Photo.ExposureProgram'].apply(eval_expr)

# Simple names for short filds list
if gargs['s']:
    df.columns = df.columns.to_series().astype(str).str.replace(r'Exif\.(Photo|Image)\.', '', regex=True)

df.to_csv(report_file, sep='\t', index=False)

print('Harvested images:', df.shape[0])
print('Errors:', err_count)
