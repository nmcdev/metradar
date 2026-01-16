
# _*_ coding: utf-8 _*_

# Copyright (c) 2026 NMC Developers.
# Distributed under the terms of the GPL V3 License.

'''
该脚本是用于批量绘制综合体，并可以针对不同case进行不同的绘图设置，具体参数保存在类似radardrawlist_20120612.csv文件中

朱文剑
'''

import pandas as pd
import warnings
warnings.filterwarnings('ignore')

from metradar.graph.draw_radar_comp_func import DRAW_RADAR_OTHER,ini_params
from multiprocessing import cpu_count, Pool,freeze_support


def draw_all(params):
    # sourcepath,outpath,filename,start_lat,end_lat,start_lon,end_lon
    # 河南北部区域
    sourcepath = params['sourcepath']
    outpath = params['outpath']
    radfilename = params['radfilename']
    slat = params['slat']
    nlat = params['nlat']
    wlon = params['wlon']
    elon = params['elon']

    timestr = radfilename[5:13] + '.' +  radfilename[14:20]

    
    # 用户需根据实际情况设置路径
    fontname='../common/fonts/msyhbd.ttc'

    # 添加中文地名
    # 用户需根据实际情况设置路径
    filename = '../common/中文地理信息原始文件/吉林省所有站点信息.xls'
  
    data_guojia = pd.read_excel(filename, sheet_name = '国家站55',header=0,skiprows=0,index_col=None,)
    for jj in range(data_guojia.shape[0]):
        curidx = data_guojia['站名'][jj].find('国家')
        data_guojia['站名'][jj] = data_guojia['站名'][jj].replace( data_guojia['站名'][jj][curidx:],'')
        data_guojia['站名'][jj] = data_guojia['站名'][jj].replace( data_guojia['组织机构'][jj],'')

    data_guojiatianqi = pd.read_excel(filename, sheet_name = '国家天气站333',header=0,skiprows=0,index_col=None,)
                        
    for jj in range(data_guojiatianqi.shape[0]):
        data_guojiatianqi['站名'][jj] = data_guojiatianqi['站名'][jj].replace('国家气象观测站','')
        data_guojiatianqi['站名'][jj] = data_guojiatianqi['站名'][jj].replace( data_guojiatianqi['组织机构'][jj],'')
        if data_guojiatianqi['站名'][jj].find('尔罗斯')>=0:
            data_guojiatianqi['站名'][jj] = data_guojiatianqi['站名'][jj].replace('尔罗斯','')

    data_quyuzhan = pd.read_excel(filename, sheet_name = '区域站1048',header=0,skiprows=0,index_col=None,)
    for jj in range(data_quyuzhan.shape[0]):
        data_quyuzhan['站名'][jj] = data_quyuzhan['站名'][jj].replace('气象观测站','')
        
        data_quyuzhan['站名'][jj] = data_quyuzhan['站名'][jj].replace(data_quyuzhan['组织机构'][jj],'')
        if data_quyuzhan['站名'][jj].find('尔罗斯')>=0:
            data_quyuzhan['站名'][jj] = data_quyuzhan['站名'][jj].replace('尔罗斯','')
        


    lat=[]
    lon=[]
    staname=[]
    for ng in range(len(data_guojia['经度'])):
        lon.append(data_guojia['经度'][ng])
        lat.append(data_guojia['纬度'][ng])
        staname.append(data_guojia['站名'][ng])
    
    for ng in range(len(data_guojiatianqi['经度'])):
        lon.append(data_guojiatianqi['经度'][ng])
        lat.append(data_guojiatianqi['纬度'][ng])
        staname.append(data_guojiatianqi['站名'][ng])
    
    # for ng in range(len(data_quyuzhan['经度'])):
    #     lon.append(data_quyuzhan['经度'][ng])
    #     lat.append(data_quyuzhan['纬度'][ng])
    #     staname.append(data_quyuzhan['站名'][ng])



    # radfilepath='/Users/wenjianzhu/Downloads/ZZHN'
    radfilepath = sourcepath
    params['radarfile_path'] = radfilepath
    params['radarfile_name'] = radfilename
    params['pic_path'] = outpath
    params['timestr'] = timestr
    params['slat'] = slat
    params['nlat'] = nlat
    params['wlon'] = wlon
    params['elon'] = elon
    params['fontfile'] = fontname
    params['gis_lats'] = lat
    params['gis_lons'] = lon
    params['gis_name'] = staname
    params['breplace'] = True
    params['bdraw_crs'] = True
    params['ref_colorfile'] = '../common/gr2_colors/default_BR_PUP2.pal'
    params['vel_colorfile'] = '../common/gr2_colors/default_BV_PUP2.pal'
    params['figsize_width'] = 4
    params['fontsize_gis'] = 5
    params['fontsize_colorbar'] = 5
    params['fontsize_title'] = 6
    params['mapcolor'] = [0/255,0/255,0/255]
    params['dpi'] = 800
    params['pic_format'] = 'jpg'
    params['bdraw_title_ppi'] = False

    # params={'radarfile_path':radfilepath,
    #         'radarfile_name':radfilename,
    #         'mosaicfile_path':'',
    #         'mosaicfile_name':'',
    #         'pic_path':outpath,
    #         'timestr':timestr,
    #         'aws_min_file_path':'',
    #         'aws_min_file_name':'',
    #         'aws_hour_file_path':'',
    #         'aws_hour_file_name':'',
    #         'gis_name':staname,
    #         'gis_lats':lat,
    #         'gis_lons':lon,
    #         'slat':slat,
    #         'nlat':nlat,
    #         'wlon':wlon,
    #         'elon':elon,
    #         'ref_colorfile':'../common/gr2_colors/default_BR_PUP2.pal',
    #         'vel_colorfile':'../common/gr2_colors/default_BV_PUP2.pal',
    #         'fontfile':fontname,
    #         'dpi':800,
    #         'pic_format':'png',
    #         'figsize_width':4,
    #         'fontsize_gis':5,
    #         'fontsize_colorbar':5,
    #         'fontsize_title':6,
    #         'mapcolor':[0/255,0/255,0/255],
    #         'breplace':True, #如果图片文件已存在，是否重新绘制
    #         'bdraw_crs':False
    # }

    _draw_radar_other = DRAW_RADAR_OTHER(params)

    _draw_radar_other.read_vol_data()

    # _draw_radar_other.draw_ref_alone(subdir='回波强度',tilt=0,thred=-5)
    _draw_radar_other.draw_ref_alone(subdir='回波强度',tilt=1,thred=-5) 


    # _draw_radar_other.draw_vel_alone(subdir='径向速度',tilt=0)
    # _draw_radar_other.draw_vel_alone(subdir='径向速度',tilt=1)
    # _draw_radar_other.draw_vel_alone(subdir='径向速度',tilt=2)

    # _draw_radar_other.draw_vel_pre()
    _draw_radar_other.draw_vel_wind_barb()
    # _draw_radar_other.draw_vel_wind_quiver()
    # _draw_radar_other.draw_ref_pre()
    # _draw_radar_other.draw_ref_pre_wind_barb()

    # _draw_radar_other.get_cref_from_radar([_draw_radar_other.g_rad_lat,_draw_radar_other.g_rad_lon])
    # _draw_radar_other.get_cref_from_mosaicfile()
    # _draw_radar_other.get_cref_from_radar([35.6,114.0])
    # _draw_radar_other.draw_cref_pre()
    # _draw_radar_other.draw_cref_wind_barb()
    # _draw_radar_other.draw_cref_wind_quiver()
    # _draw_radar_other.draw_cref_pre_wind_barb()


# %%
import os
if __name__ == '__main__':
    pass
    freeze_support()
    # Pool不支持跨CPU的虚拟服务器，会出现页面不足的错误提示

    # 下面是针对不同的case进行绘图
    paramfilepath = '/Users/wenjianzhu/Downloads/雷达数据-xxx/绘图参数/回波强度'
    # paramfilepath = '/Users/wenjianzhu/Downloads/雷达数据-xxx/绘图参数/径向速度'
    # drawinfo = pd.read_csv(paramfilepath + os.sep + 'radardrawlist_20120612.csv',encoding='gb18030')
    # drawinfo = pd.read_csv(paramfilepath + os.sep + 'radardrawlist_20120701.csv',encoding='gb18030')
    # drawinfo = pd.read_csv(paramfilepath + os.sep + 'radardrawlist_20150608.csv',encoding='gb18030')
    # drawinfo = pd.read_csv(paramfilepath + os.sep + 'radardrawlist_20170905.csv',encoding='gb18030')
    drawinfo = pd.read_csv(paramfilepath + os.sep + 'radardrawlist_20210909.csv',encoding='gb18030')
    # drawinfo = pd.read_csv(paramfilepath + os.sep + 'radardrawlist_20190602.csv',encoding='gb18030')

    params = []
    nums=drawinfo.shape[0]
    # nums=1
    for nn in range(nums):
        pass
        curparam=ini_params()
        curparam['sourcepath'] = drawinfo['sourcepath'].iloc[nn]
        curparam['outpath'] = drawinfo['outpath'].iloc[nn]
        curparam['radfilename'] = drawinfo['filename'].iloc[nn]
        curparam['slat'] = drawinfo['start_lat'].iloc[nn]
        curparam['nlat'] = drawinfo['end_lat'].iloc[nn]
        curparam['wlon'] = drawinfo['start_lon'].iloc[nn]
        curparam['elon'] = drawinfo['end_lon'].iloc[nn]
        # aws_min_delta_t_file_path
        params.append(curparam)

    # MAXP = int(cpu_count()*0.5)
    MAXP=1#nums
    pools = Pool(MAXP)

    pools.map(draw_all, params)
    pools.close()
    pools.join()



    