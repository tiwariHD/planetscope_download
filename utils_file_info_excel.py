import os
import numpy as np
import rasterio
import pandas as pd
import json
import shutil
from time import time

def get_file_properties(file_path):
    file_name = os.path.basename(file_path)
    json_data = json.load(open(file_path.split('3B')[0] + 'metadata.json', 'r'))['properties']
    src = rasterio.open(file_path)
    return {
        'File Name': file_name,
        'Width': src.width,
        'Height': src.height,
        'Count': src.count,
        'Epsg': src.crs.to_epsg(),
        'clear_conf_perc': json_data.get('clear_confidence_percent', -1),
        'cloud_cover': json_data['cloud_cover'],
        'heavy_haze_percent': json_data.get('heavy_haze_percent', -1),
        'quality_category': json_data['quality_category'],
        'visible_confidence_percent': json_data.get('visible_confidence_percent', -1),
        'Fullest': 0

    }

def save_file_properties(images_path, out_filename='images_info.xlsx', ext='harmonized_clip_reproject.tif'):
    print('Running for: ', images_path)
    filenames = sorted(os.listdir(images_path))
    print('num_files: ', len(filenames))
    max_dims = (0, 0)
    max_width = 0
    max_height = 0
    max_file = ''
    max_sum = 0
    file_properties = []
    for file in filenames:
        if file.endswith(ext):
            fp = get_file_properties(os.path.join(images_path, file))
            sum = fp['Width'] + fp['Height']
            if sum > max_sum:
                max_sum = sum
                max_dims = (fp['Width'], fp['Height'])
                max_file = fp['File Name']
            if fp['Width'] > max_width:
                max_width = fp['Width']
            if fp['Height'] > max_height:
                max_height = fp['Height']
            file_properties.append(fp)

    print('Max File: ', max_file)
    print('Max dims: ', max_dims)
    print('Max sum: ', max_sum)
    print('Max Width: ', max_width)
    print('Max Height: ', max_height)
    print('------------')
    print('len: ', len(file_properties))
    excl_fp = []
    selected_fp = []
    for fpi in file_properties:
        dims = (fpi['Width'], fpi['Height'])
        if dims == max_dims:
            # print(fpi['File Name'])
            fpi['Fullest'] = 1
            excl_fp.append(fpi)
            # get some selected files
            if (
                fpi['clear_conf_perc'] == 100 and fpi['cloud_cover'] == 0 and fpi['heavy_haze_percent'] == 0 and
                fpi['quality_category'] == 'standard' and fpi['visible_confidence_percent'] == 100
            ):
                selected_fp.append(fpi)

    if file_properties:
        df = pd.DataFrame(file_properties)
        df.to_excel(out_filename, index=False)
    if excl_fp:
        df_ecl = pd.DataFrame(excl_fp)
        df_ecl.to_excel(os.path.join(os.path.dirname(out_filename), os.path.basename(out_filename).split('.')[0] + '_fullest.xlsx'), index=False)
    if selected_fp:
        selected_dir = os.path.join(os.path.dirname(images_path), 'tmp', 'selected')
        if not os.path.exists(selected_dir):
            os.makedirs(selected_dir)
        print('copying: {} selected files to: {}'.format(len(selected_fp), selected_dir))
        for fpi in selected_fp:
            shutil.copy2(os.path.join(images_path, fpi['File Name']), os.path.join(selected_dir, fpi['File Name']))


if __name__ == '__main__':
    start = time()
    save_file_properties('data1/files', 'data1_info.xlsx')
    save_file_properties('data2/files', 'data2_info.xlsx')
    print('Run Process Time: {}'.format(time() - start))
