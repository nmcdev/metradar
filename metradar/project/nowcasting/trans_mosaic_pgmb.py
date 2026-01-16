'''
测试将雷达拼图输出成pgmb格式

'''

# %%
import matplotlib
matplotlib.use('Agg')  # 关键：禁用交互式后端
from metradar.io.pgmb_io import pgmb_write
import os
from metradar.io.read_new_mosaic_func import decode_mosaic
import xarray as xr
import numpy as np
import gzip


def zip_file(path, new_path):
    with open(new_path, 'wb') as wf:
        with open(path, 'rb') as rf:
            data = rf.read()
            # 压缩数据
            data_comp = gzip.compress(data,compresslevel=5)
        wf.write(data_comp)
 
 
filepath = '/mnt/e/metradar_test/pysteps_data/mosaic_bin/2025/07/02/RADA_L3_MST_CREF_QC/'
for filename in os.listdir(filepath):
    
    data = decode_mosaic(filepath,filename)
    # filename = 'ACHN.CREF000.20230421.180009.nc'
    obstimestr = filename.split('_')[9]+filename.split('_')[10][0:4].split('.')[0]
    outname = obstimestr + '_fmi.radar.composite.lowest_FIN_SUOMI1.pgm'
    outpath = '/mnt/e/metradar_test/pysteps_data/radar/fmi/%s'%filename.split('_')[9]
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    file_name = outpath + os.sep + outname

    data = data.sel(lat=slice(33, 37), lon=slice(109, 117))

    width = data.CREF.shape[1]
    height = data.CREF.shape[0]
    maxval = 255
    gray = data.CREF*2+66

    aa = np.isnan(gray.data)
    gray.data[aa]=255
    cref = np.flipud(gray.data.astype(np.uint8))
    params={}
    params['obstimestr'] = obstimestr
    params['left_lon'] = 113
    params['right_lon'] = 118
    params['bottom_lat'] = 37
    params['upper_lat'] = 41

    pgmb_write ( file_name, params, width, height, maxval, cref )

    # gzip file

    # zip_file(file_name, file_name + '.gz')


    print('write pgmb file %s successfully!'%(file_name))
    # break