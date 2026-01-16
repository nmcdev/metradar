# _*_ coding: utf-8 _*_

'''

从单个基数据文件计算雨强

'''


import pyart
import os
import numpy as np

from metradar.io.decode_fmt_pyart import read_cnrad_fmt
import datetime

def get_rainrate(inputfilepath,
                 inputfilename,
                 outpath,
                 outname,
                 GRID_XNUM = 701,
                 GRID_YNUM = 701,
                 GRID_RESO = 1000
                 ):

    #降水产品格点设置
    # GRID_XNUM = 701 #X方向格点数
    # GRID_YNUM = 701 #Y方向格点数
    # GRID_RESO = 1000 #网格分辨率，米

    GRID_SHAPE = (1,GRID_XNUM, GRID_YNUM)
    GRID_LIMITS = ((GRID_RESO, GRID_RESO), (-1000*(GRID_XNUM-1)/2, 1000*(GRID_XNUM-1)/2), (-1000*(GRID_XNUM-1)/2, 1000*(GRID_XNUM-1)/2))
    
    # radar = standard_data_to_pyart(f)
    try:
        radar = read_cnrad_fmt(inputfilepath + os.sep + inputfilename)
    except:
        print(inputfilepath + os.sep + inputfilename + ' read error!')

    # 对于部分雷达，必须添加下面两行，要不然会报错
    radar.nrays = radar.fields['reflectivity']['data'].shape[0]
    radar.ngates = radar.fields['reflectivity']['data'].shape[1]

    # rad_lon = radar.longitude['data']
    # rad_lat = radar.latitude['data']
    # rad_alti = radar.altitude['data']
    # rad_name = radar.metadata['instrument_name']
    
    rain = pyart.retrieve.est_rain_rate_z(radar)
    radar.add_field('radar_estimated_rain_rate',rain)
    # rfd = radar.fields['radar_estimated_rain_rate']['data']
    # qpe = rfd[radar.get_slice(0)]

    grid = pyart.map.grid_from_radars(
    (radar,),
    grid_shape=GRID_SHAPE,
    grid_limits=GRID_LIMITS,
    fields=['radar_estimated_rain_rate'])
    grid.fields['radar_estimated_rain_rate']['_FillValue'] = 0

    #输出雨强文件
    
    # outname = filename.replace('.bz2','_rr.nc')
    pyart.io.write_grid(outpath + os.sep + outname + '.lock',grid)
    os.rename(outpath + os.sep + outname + '.lock',outpath + os.sep + outname)
    print('%s done!'%outname)
    return True


if __name__ == "__main__":

    inputfilepath = '/data3/zwj/qpe_test/data/radarbase/Z9453'
    outpath = '/data3/zwj/qpe_test/data/rain_rate/Z9453'
    inputfilename = 'Z_RADR_I_Z9453_20210509105446_O_DOR_CC_CAP_FMT.bin.bz2'
    
    outname = inputfilename.replace('.bz2','_rr.nc')
    get_rainrate(inputfilepath = inputfilepath,
                 inputfilename = inputfilename,
                 outpath = '/data3/zwj/qpe_test/data/rain_rate/Z9453',
                 outname = outname)