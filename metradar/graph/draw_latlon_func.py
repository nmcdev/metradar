# _*_ coding: utf-8 _*_
'''
绘制雷达拼图
'''


# %%
import os
import matplotlib.pyplot as plt 
import cartopy.crs as ccrs
from metradar.util.parse_pal import parse,parse_pro
import xarray as xr
import numpy as np
from matplotlib.transforms import offset_copy
import matplotlib.patheffects as path_effects
from matplotlib.font_manager import FontProperties
import json
import matplotlib as mpl
import matplotlib
matplotlib.use('agg')

from cartopy.io.shapereader import Reader
import pandas as pd
from multiprocessing import cpu_count, Pool,freeze_support
from metradar.config import CONFIG

# 资源文件路径
RESOURCES_PATH = CONFIG.get('SETTING','RESOURCES_PATH')

FONT_FILE = RESOURCES_PATH + '/fonts/YaHeiConsolasHybrid_1.12.ttf'
COLOR_PATH=RESOURCES_PATH + '/gr2_colors/'
MAP_PATH = RESOURCES_PATH + '/国省市县审图号(GS (2019) 3082号'
STATION_FILE = RESOURCES_PATH + '/stations/cma_city_station_info.dat'

g_font_max = FontProperties(fname= FONT_FILE, size=8)
g_font_mid = FontProperties(fname= FONT_FILE, size=6)
g_font_min = FontProperties(fname= FONT_FILE, size=4)

mpl.rcParams['font.size'] = 7
mpl.rcParams['font.weight'] = 'normal'
# mpl.rcParams['font.family'] = 'Times New Roman'


def draw_gisinfo(ax,font=None):

    city_file =STATION_FILE
    
    if not os.path.exists(city_file):
        print(city_file +' not exist!')
        return False

    cities = pd.read_csv(city_file,  delimiter=r"\s+") # cma_city_station_info

    cityname = np.unique(cities['city_name'])
    names=[]
    lat=[]
    lon=[]

    curdata = cities
    for name in cityname:
        flag2 =  curdata['city_name'] == name
        tmp_lat = np.mean(curdata[flag2]['lat'])
        tmp_lon = np.mean(curdata[flag2]['lon'])

        names.append(name)
        lon.append(tmp_lon)
        lat.append(tmp_lat)

    cities = pd.DataFrame(data={'name':names, 'lat':lat, 'lon':lon})
    # draw station information
    font = g_font_min
    geodetic_transform = ccrs.Geodetic()._as_mpl_transform(ax)
    for _, row in cities.iterrows():
        text_transform = offset_copy(geodetic_transform, units='dots', x=-5)
        ax.plot(row['lon'], row['lat'], marker='o', color=[0.2,0.2,0.2], linestyle='None',
                markersize=2, alpha=0.8, transform=ccrs.PlateCarree())
        ax.text(row['lon']-0.05, row['lat'], row['name'], clip_on=True, # city_name
                verticalalignment='center', horizontalalignment='right',
                transform=text_transform, fontproperties=font, color=[0.2,0.2,0.2],
                path_effects=[path_effects.Stroke(linewidth=1, foreground=[1,1,1]),
                path_effects.Normal()])
                
    
def add_china_map_2cartopy(ax, facecolor='none',transform=None,name='province',
                           edgecolor='k', lw=0.8, **kwargs):
    """
    Draw china boundary on cartopy map.

    :param ax: matplotlib axes instance.
    :param name: map name.
    :param facecolor: fill color, default is none.
    :param edgecolor: edge color.
    :param lw: line width.
    :return: None
    """

    # map name
    names = {'nation': "BOUL_G", 'province': "BOUL_S",
            'city': "BOUL_D", 'county': "BOUL_X",}

    if not name in {'nation','province','city','county'}:
        print(name + ' should be nation or province or city or county !')
        return False


    # add 省界
    if transform is None:
        transform = ccrs.PlateCarree()

    if name == 'county':
        # 添加县边界
        shpfile = MAP_PATH + os.sep + names[name] + ".shp"
        ax.add_geometries(
            Reader(shpfile).geometries(), transform,
            path_effects=[path_effects.Stroke(linewidth=lw, foreground=[1,1,1]),path_effects.Normal()],
            facecolor=facecolor, edgecolor=edgecolor, lw=lw-0.5, **kwargs)

    if name == 'city':
        # 添加市边界
        shpfile = MAP_PATH + os.sep + names[name] + ".shp"
        ax.add_geometries(
            Reader(shpfile).geometries(), transform,
            path_effects=[path_effects.Stroke(linewidth=lw+0.2, foreground=[1,1,1]),path_effects.Normal()],
            facecolor=facecolor, edgecolor=edgecolor, lw=lw-0.2, **kwargs)
        
    # 添加省边界
    if name == 'province':
        shpfile = MAP_PATH + os.sep + names[name] + ".shp"
        ax.add_geometries(
            Reader(shpfile).geometries(), transform,
            path_effects=[path_effects.Stroke(linewidth=lw+0.5, foreground=[1,1,1]),path_effects.Normal()],
            facecolor=facecolor, edgecolor=edgecolor, lw=lw+0.2, **kwargs)

def cm_precip():
    """
    Standardized colormaps from National Weather Service
    https://github.com/blaylockbk/pyBKB_v2/blob/master/BB_cmap/NWS_standard_cmap.py

    Range of values:
        metric: 0 to 762 millimeters
        english: 0 to 30 inches
    """
    # The amount of precipitation in inches
    # a = [0,.01,.1,.25,.5,1,1.5,2,3,4,6,8,10,15,20,30]
    a = [0,0.01,0.5,2.5,5,10,15,20,30,40,60,80,100,150,200,250]

    # Normalize the bin between 0 and 1 (uneven bins are important here)
    norm = [(float(i)-min(a))/(max(a)-min(a)) for i in a]

    # Color tuple for every bin
    C = np.array(# [255,255,255],
        [ [199,233,192], [161,217,155], [116,196,118], [49,163,83], 
        [0,109,44], [255,250,138], [255,204,79], [254,141,60], [252,78,42], 
        [214,26,28], [173,0,38], [112,0,38], [59,0,48], [76,0,115], [255,219,255]])

    cmap, norm = mpl.colors.from_levels_and_colors(np.array(a), np.array(C)/255.0, extend='neither')
    return cmap, norm


def draw_latlon(data,lat,lon,slat=None,nlat=None,wlon=None,elon=None,outpath=None,outname=None,tstr=None,
                prefix_title='雷达组合反射率拼图 ',units='dBZ',add_title=False,badd_gis=True,badd_logo=True,
                subtitle='',titlecolor='r',thred=None,cb_ratio=0.618,fig_size=[6,4],dpi=400,colorfile=None):
    '''
    该函数在绘制经纬度网格数据的核心函数
    draw_latlon 的 Docstring
    
    :param data: DataArray 类型，二维数组
    :param lat: 数据的纬度列表，一维数组
    :param lon: 数据的经度列表，一维数组
    :param slat: 绘图南边界
    :param nlat: 绘图北边界
    :param wlon: 绘图西边界
    :param elon: 绘图东边界
    :param outpath: 图片保存路径
    :param outname: 图片保存名称
    :param tstr: 时间字符串，YYYYMMDDHHmmss格式
    :param prefix_title: 标题前缀
    :param units: 单位
    :param add_title: 是否添加标题
    :param badd_gis: 是否添加地理信息
    :param badd_logo: 是否在左下角添加说明: Powered by metradar
    :param subtitle: 标题后缀
    :param titlecolor: 标题颜色
    :param thred: 显示阈值，小于该阈值不显示
    :param cb_ratio: 色标长度占Y轴长度的比例
    :param fig_size: 图片画布尺寸，默认为[4,3]
    :param dpi: 保存图片的分辨率
    :param colorfile: 自定义配色文件路径，如果不提供则使用默认配色方案,色标文件必须是.pal格式，可以使用resources目录下的配色文件
    '''
    if units == 'dBZ':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/BR_WDTB_Bright.pal')#BR_WDTB_Bright.pal  default_BR_PUP2.pal BR_AVL_BroadcastNegatives.pal
    elif units == 'mm':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = cm_precip()
    elif units == '%':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/default_CC_ROSE.pal')
    elif units == 'dB':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/default_ZDR_ROSE.pal')
    elif units == 'deg/km':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/default_KDP_ROSE.pal')
    elif units == 'cat':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/default_HCAS.pal')
    elif units == 'm/s':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/default_BV_PUP2.pal')
    elif units == 'kg/m2':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            var_cmap,norm = parse(COLOR_PATH + '/default_VIL.pal')
    elif units == 'km':
        if colorfile is not None:
            var_cmap,norm = parse(colorfile)
        else:
            outdic = parse_pro(COLOR_PATH + '/default_ET_PUP.pal')
            var_cmap,norm = outdic['cmap'],outdic['norm']
    else:
        pass
        
    # newdata.data.shape[1],newdata.data.shape[0]
    fig= plt.figure(figsize=fig_size,dpi=dpi,clear=True)
    map_panel_axes = [0.1, 0.1, .8, .8]

    projs = ccrs.PlateCarree()
    
    ax = plt.axes(map_panel_axes, projection=projs)
    # 如果slat,nlat,wlon,elon没有提供，则使用数据的范围
    if slat is None:
        slat = float(lat.min())
    if nlat is None:
        nlat = float(lat.max())
    if wlon is None:
        wlon = float(lon.min())
    if elon is None:
        elon = float(lon.max())
    extent = [wlon, elon, slat, nlat]
    ax.set_extent(extent, crs=projs) 


    if not thred is None:
        newdata = np.ma.masked_array(data, data < thred)
    else:
        newdata = data
    grid_lat,grid_lon=np.meshgrid(lat,lon)
    pcm = ax.pcolormesh(grid_lon,grid_lat,newdata.T,cmap=var_cmap,norm=norm,transform=projs)


    # add_china_map_2cartopy(ax,name='county')
    # add_china_map_2cartopy(ax,name='city')
    add_china_map_2cartopy(ax,name='province',lw=0.5)

    if badd_gis:
        draw_gisinfo(ax)


    # 添加地图网格
    # ax.minorticks_on()
    # ax.tick_params(direction='out', length=6, width=2, colors='r',grid_color='r', grid_alpha=0.5)
    if elon - wlon > 20:
        lon_step = 5
    elif elon - wlon > 5:
        lon_step = 1
    elif elon - wlon > 1:
        lon_step = 0.5
    else:
        lon_step = 0.2

    if nlat - slat > 20:
        lat_step = 5
    elif nlat - slat > 5:
        lat_step = 1
    elif nlat - slat > 1:
        lat_step = 0.5
    else:
        lat_step = 0.2

    lb=ax.gridlines(draw_labels=True,x_inline=False, y_inline=False,xlocs=np.arange(int(wlon)-1,int(elon)+1,lon_step), ylocs=np.arange(int(slat)-1,int(nlat)+1,lat_step),
                    linewidth=0.1, color='k', alpha=0.8, linestyle='--')
    
    # 在左下角添加一行字： Powered by metradar
    if badd_logo:
        textstr = 'Powered by metradar'
        ax.text(0.01, 0.01, textstr, transform=ax.transAxes, fontsize=5, verticalalignment='bottom',
                horizontalalignment='left', color='k', alpha=0.6, fontproperties=g_font_min)
    # point_lat = 30.72
    # point_lon = 108.57
    # ax.plot(point_lon,point_lat,marker='o',color='r',markersize=4,transform=ccrs.PlateCarree())
    lb.top_labels = None
    lb.right_labels = None
    lb.rotate_labels = False
 
    titlestr1 = prefix_title + tstr[0:8] + 'T' + tstr[8:14] + ' UTC ' + subtitle

    if add_title:
        ax.set_title(titlestr1,fontproperties=g_font_mid,loc='left',verticalalignment='top',color=titlecolor)
    # add colorbar
    ax_pos = ax.get_position().bounds
    cbar_axes = [ax_pos[0]+ ax_pos[2]+0.005,(ax_pos[1]+ax_pos[1]+ax_pos[3])/2-ax_pos[3]*cb_ratio/2,0.02,ax_pos[3]*cb_ratio]
    cb_ax = fig.add_axes(cbar_axes)
    if units == 'cat':
        plt.colorbar(mappable=pcm,cax=cb_ax,extend='both',ticks=np.arange(0.5,11.5,1)) # extend='both',ticks=np.arange(-5,80,5)
    elif units == 'dBZ':
        plt.colorbar(mappable=pcm,cax=cb_ax,extend='both',ticks=np.arange(0,80,5))
    else:
        plt.colorbar(mappable=pcm,cax=cb_ax,extend='both')
    cb_ax.text(1.8, 1.035, units, transform=cb_ax.transAxes,va="center", ha="center", font=g_font_mid ) # 0.5,1.02

    if units == 'cat':
        cb_ax.set_yticklabels(['空','地物','晴空','干雪','湿雪','冰晶','霰','大雨滴','小到中雨','大雨','冰雹',],font=g_font_min)

    
    if not os.path.exists(outpath):
        os.makedirs(outpath,exist_ok=True)
    if os.path.exists(outpath):
        if outname.endswith('.eps'):
            plt.savefig(outpath + os.sep  + outname, transparent=False, dpi=dpi,papertype='a4')
        else:
            fig.savefig(outpath + os.sep  + outname, bbox_inches="tight",transparent=False, dpi=dpi,)
        
        print(outpath + os.sep + outname + ' saved!')
    # 关闭图形
    plt.close()
    

def draw_single(param):
    filename = param['filename']
    filepath = param['filepath']
    outpath = param['outpath']
    outname = param['outname']
    tstr = param['timestr']
    slat = param['slat']
    nlat = param['nlat']
    wlon = param['wlon']
    elon = param['elon']
    dpi = param['dpi']
    thred = param['thred']
    

    # outname = filename.replace('.nc','.png')
    # tstr = filename.split('.')[2] + filename.split('.')[3]
    data = xr.open_dataset(filepath + os.sep + filename)
    try:
        newdata = data.cref.sel(latitude=slice(slat,nlat),longitude=slice(wlon,elon))
        draw_latlon(newdata,newdata.latitude.data,newdata.longitude.data,slat,nlat,wlon,elon,outpath,outname,tstr,dpi=dpi,thred=thred)
    except:
        newdata = data.cref.sel(lat=slice(slat,nlat),lon=slice(wlon,elon))
        draw_latlon(newdata,newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,outpath,outname,tstr,dpi=dpi,thred=thred)
    
    
    

if __name__ == "__main__":      
    freeze_support()
    filepath = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/testdata/mosaic_bin'
    filename = 'ACHN_CREF_20210918_155000.BIN'
    outpath = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/output/pic'
    
    if not os.path.exists(outpath):
        os.makedirs(outpath)
     # 单张图绘制
    curparam=dict()
    curparam['filepath'] = filepath
    curparam['filename'] = filename
    curparam['outpath'] = outpath
    curparam['outname'] = filename + '.png'
    curparam['timestr'] = filename.split('_')[2] + filename.split('_')[3].split('.')[0]
    curparam['slat'] = 15
    curparam['nlat'] = 55
    curparam['wlon'] = 72
    curparam['elon'] =140
    curparam['dpi'] = 300
    curparam['thred'] = 10

    draw_single(curparam)

    
    
    # 多进程批量绘制
    # params=[]
    # files = os.listdir(filepath)
    # files = sorted(files)
    # for filename in files:
    #     if filename.startswith('.') or filename.startswith('..'):
    #         continue
    #     # if filename.split('.')[3][2:4] != '00' and filename.split('.')[3][2:4] != '12' and filename.split('.')[3][2:4] != '30' and filename.split('.')[3][2:4] != '48':
    #     #     continue
    #     curparam=dict()
    #     curparam['filepath'] = filepath
    #     curparam['filename'] = filename
    #     curparam['outname'] = filename + '.png'
    #     curparam['timestr'] = filename.split('_')[2] + filename.split('_')[3].split('.')[0]
    #     curparam['outpath'] = outpath
    #     curparam['slat'] = 28
    #     curparam['nlat'] = 32.5
    #     curparam['wlon'] = 105.5
    #     curparam['elon'] =110.5
    #     curparam['dpi'] = 300
    #     curparam['thred'] = 15

    #     params.append(curparam)

    # MAXP = int(cpu_count()*0.8)
    # pools = Pool(MAXP)

    # pools.map(draw_single, params)
    # pools.close()
    # pools.join()

