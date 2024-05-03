import os
import shutil
import numpy as np
from osgeo import gdal, gdalconst
import rasterio
import pandas as pd
from time import time
from multiprocessing import Pool

MAX_CLOUD_COVER = 0.1
MULTI_PROCESS_NUM = 24

def expand_image(small_image_path, output_image_path, big_image_path):
    src_small = rasterio.open(small_image_path)
    src_big = rasterio.open(big_image_path)
    data_small = src_small.read()

    data_big_shape = (src_big.count, src_big.height, src_big.width)
    data_big = np.zeros(data_big_shape)

    # using gdal for better transformation
    smaller_ds = gdal.Open(small_image_path, gdalconst.GA_ReadOnly)
    smaller_geo_transform = smaller_ds.GetGeoTransform()
    bigger_ds = gdal.Open(big_image_path, gdalconst.GA_ReadOnly)
    bigger_geo_transform = bigger_ds.GetGeoTransform()
    offset_x = int(round((smaller_geo_transform[0] - bigger_geo_transform[0]) / bigger_geo_transform[1]))
    offset_y = int(round((smaller_geo_transform[3] - bigger_geo_transform[3]) / bigger_geo_transform[5]))

    for band_idx in range(data_small.shape[0]):
        data_big[band_idx, offset_y:offset_y + src_small.height, offset_x:offset_x + src_small.width] = data_small[band_idx]

    with rasterio.open(output_image_path, 'w', **src_big.profile) as dst:
        dst.write(data_big)

    smaller_ds = None
    bigger_ds = None

def apply_udm2_mask(image_path, udm2_path, masked_dir):
    src = rasterio.open(image_path)
    image_data = src.read()
    mask = rasterio.open(udm2_path)
    mask_data = mask.read()

    combined_mask = mask_data[3, :, :] + mask_data[4, :, :] + mask_data[5, :, :]
    # if no mask val present
    if not np.any(combined_mask != 0):
        return ''

    binary_mask = np.where(combined_mask > 0, 0, 1)
    masked_input_image = image_data * binary_mask
    masked_file_path = os.path.join(masked_dir, os.path.basename(image_path))
    with rasterio.open(masked_file_path, 'w', **src.profile) as dst:
        dst.write(masked_input_image)
    return masked_file_path


def process_single(filename, input_dir, output_dir, masked_dir, expand_reference_path, udm2_ext, is_full):
    print('Running for: ', filename)
    in_file_path = os.path.join(input_dir, filename)
    out_file_path = os.path.join(output_dir, filename)
    udm2_file_path = os.path.join(input_dir, filename.split('3B')[0] + udm2_ext)
    masked_file_path = apply_udm2_mask(in_file_path, udm2_file_path, masked_dir)
    if is_full == 0:
        #call expand
        if masked_file_path:
            expand_image(masked_file_path, out_file_path, expand_reference_path)
        else:
            expand_image(in_file_path, out_file_path, expand_reference_path)
    else:
        #write to disk
        if masked_file_path:
            shutil.copy2(masked_file_path, out_file_path)
        else:
            shutil.copy2(in_file_path, out_file_path)
    is_masked = 1 if masked_file_path else 0
    is_expanded = 1 if is_full == 0 else 0
    return (is_masked, is_expanded)

def main_dir(
        input_dir, temp_dir, output_dir, excel_file, expand_reference_path,
        ext='harmonized_clip_reproject.tif', udm2_ext='3B_udm2_clip_reproject.tif',
        max_cloud_cover=MAX_CLOUD_COVER, num_processes=MULTI_PROCESS_NUM
):
    print('Running for dir: ', input_dir)
    masked_dir = os.path.join(temp_dir, 'masked_files')
    if (not os.path.isdir(masked_dir)):
        os.makedirs(masked_dir)
    if (not os.path.isdir(output_dir)):
        os.makedirs(output_dir)
    file_info = pd.read_excel(excel_file)
    print('Loaded {}, num_records: {}'.format(excel_file, len(file_info)))
    filenames = [
        fl for fl in sorted(os.listdir(input_dir))
        if (not os.path.exists(os.path.join(output_dir, fl))) and (fl.endswith(ext)) and
        (file_info.at[file_info[file_info['File Name'] == fl].index[0], 'cloud_cover'] < max_cloud_cover)
    ]
    file_info_filtered = file_info[file_info['cloud_cover'] < max_cloud_cover]
    print('No of files to process: ', len(filenames))
    with Pool(num_processes) as pool:
        result = pool.starmap(
            process_single,
            [(
                filename, input_dir, output_dir, masked_dir,
                expand_reference_path, udm2_ext,
                file_info.at[file_info[file_info['File Name'] == filename].index[0], 'Fullest'] 
            ) for filename in filenames]
        )
    print('all files processed.., len: ', len(result))
    if os.path.exists(masked_dir):
        print('Deleting: ', masked_dir)
        shutil.rmtree(masked_dir)

    print('Dataframes; len of original: {}, len of filtered: {}'.format(len(file_info), len(file_info_filtered)))
    file_info_filtered = file_info_filtered.assign(Masked=0, Expanded=0)
    for i, filename in enumerate(filenames):
        is_masked, is_expanded = result[i]
        file_excel_index = file_info_filtered[file_info_filtered['File Name'] == filename].index[0]
        file_info_filtered.at[file_excel_index, 'Masked'] = is_masked
        file_info_filtered.at[file_excel_index, 'Expanded'] = is_expanded
    print('num files masked and expanded: ', ((file_info_filtered['Masked'] == 1) & (file_info_filtered['Expanded'] == 1)).sum())
    print('num files masked and not expanded: ', ((file_info_filtered['Masked'] == 1) & (file_info_filtered['Expanded'] == 0)).sum())
    print('num files not masked and expanded: ', ((file_info_filtered['Masked'] == 0) & (file_info_filtered['Expanded'] == 1)).sum())
    print('num files not masked and not expanded: ', ((file_info_filtered['Masked'] == 0) & (file_info_filtered['Expanded'] == 0)).sum())
    file_info_filtered.to_excel(
        os.path.join(os.path.dirname(excel_file), os.path.basename(excel_file).split('.')[0] + '_expanded.xlsx'),
        index=False
    )
    file_info_filtered[
        (
            (file_info_filtered['Masked'] == 0) & (file_info_filtered['Expanded'] == 0) &
            (file_info_filtered['Fullest'] == 1) & (file_info_filtered['clear_conf_perc'] == 100) &
            (file_info_filtered['quality_category'] == 'standard')
        )
    ].to_excel(
        os.path.join(os.path.dirname(excel_file), os.path.basename(excel_file).split('.')[0] + '_reference_candidates.xlsx'
    ), index=False)


if __name__ == '__main__':
    start = time()
    dir_info = [
        {
            'input_dir': 'data/downloads',
            'temp_dir': 'data/tmp',
            'output_dir': 'data/masked_files',
            'excel_file': 'data/downloaded_files_info.xlsx',
            'expand_reference_path': '' # replace with a full extent file
        }
    ]
    main_dir(**dir_info[0])
    print('Run Process Time: {}'.format(time() - start))
