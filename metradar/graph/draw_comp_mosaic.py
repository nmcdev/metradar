
# _*_ coding: utf-8 _*_

'''
快速绘制三维雷达组网拼图

'''

# %%
import pyart
import os
import numpy as np
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from cartopy.io.shapereader import Reader
import matplotlib.patheffects as path_effects
from matplotlib.transforms import offset_copy
from matplotlib.font_manager import FontProperties
import pandas as pd
from pyart.graph import common
from datetime import datetime,timedelta
from metradar.util.parse_pal import parse_pro,parse
import xarray as xr

from metradar.config import CONFIG

# 资源文件路径
RESOURCES_PATH = CONFIG.get('SETTING','RESOURCES_PATH')
FONT_FILE = RESOURCES_PATH + '/fonts/YaHeiConsolasHybrid_1.12.ttf'
COLOR_FILE=RESOURCES_PATH + '/gr2_colors/default_BR_PUP2.pal'
MAP_PATH = RESOURCES_PATH + '/国省市县审图号(GS (2019) 3082号/'
STATION_FILE = RESOURCES_PATH + '/stations/cma_city_station_info.dat'


def add_china_map_2cartopy(ax, name='province', facecolor='none',transform=None,
                           edgecolor='c', lw=2, **kwargs):
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
        
def draw_gisinfo(ax,slat,nlat,wlon,elon):

    # 添加中文地名

    city_file = STATION_FILE
  
    cities = pd.read_csv(city_file,  delimiter=r"\s+") # cma_city_station_info
   
    gis_lats=[]
    gis_lons=[]
    gis_name=[]
    for ng in range(len(cities['lon'])):
        gis_lons.append(cities['lon'][ng])
        gis_lats.append(cities['lat'][ng])
        gis_name.append(cities['city_name'][ng])
    

   
    font2=FontProperties(fname=FONT_FILE, size=8)
    geodetic_transform = ccrs.Geodetic()._as_mpl_transform(ax)
    text_transform = offset_copy(geodetic_transform, units='dots', x=-5)
    aa = (np.array(gis_lats)<nlat) & (np.array(gis_lats) > slat)
    bb = (np.array(gis_lons)<elon) & (np.array(gis_lons) > wlon)
    cc = aa & bb
    # cc = cc.values
    for nn in range(len(gis_name)):
        if not cc[nn]:
            continue
        curlat = gis_lats[nn]
        curlon = gis_lons[nn]

        ax.text(curlon, curlat, gis_name[nn], clip_on=True,
                        verticalalignment='center', horizontalalignment='right',
                        transform=text_transform, fontproperties=font2, color='white',
                        path_effects=[path_effects.Stroke(linewidth=1, foreground='black'),path_effects.Normal()])

# 快速绘制垂直剖面图
def quick_draw_mosaic_cross(grid:pyart.core.Grid,params):
    
    # Setting projection, figure size, and panel sizes.
    projection = ccrs.PlateCarree()

    fig = plt.figure(figsize=[16, 7])

    map_panel_axes = [0.1, 0.1, .4, .80]
    x_cut_panel_axes = [0.55, 0.15, .4, .25]
    y_cut_panel_axes = [0.55, 0.55, .4, .25]

    
    # lat = 32.89
    # lon = 118.30
    lat = params['cross_lat']
    lon = params['cross_lon']
    xlim_west = params['xlim_west']
    xlim_east = params['xlim_east']
    ylim_south = params['ylim_south']
    ylim_north = params['ylim_north']

    xcross_left = params['xcross_left']
    xcross_right = params['xcross_right']
    ycross_down = params['ycross_down']
    ycross_up = params['ycross_up']
    zcross_bottom = params['zcross_bottom']
    zcross_top = params['zcross_top']
    dis_level = params['level']

    min_ref = params['min_ref']

    am = grid.fields['reflectivity']['data'].mask
    da = grid.fields['reflectivity']['data'].data
    am[da < min_ref] = True

    display = pyart.graph.GridMapDisplay(grid)
    # Set parameters.
    level = 3
    vmin = 0
    vmax = 75

    cmapname = 'NWSRef'

    # Panel 1: PPI plot of the second tilt.
    ax1 = fig.add_axes(map_panel_axes, projection=projection)
    display.plot_grid('reflectivity', dis_level, vmin=vmin, vmax=vmax,
                    ax=ax1,
                    projection=projection,
                    cmap=cmapname)
    display.plot_crosshairs(lon=lon, lat=lat)
    ax1.set_ylim([ylim_south, ylim_north])
    ax1.set_xlim([xlim_west, xlim_east])
    add_china_map_2cartopy(ax1, name='province', facecolor='none',edgecolor=None, lw=1)
    draw_gisinfo(ax1,slat=ylim_south,nlat=ylim_north,wlon=xlim_west,elon=xlim_east)
    # Panel 2: longitude slice
    ax2 = fig.add_axes(x_cut_panel_axes)
    display.plot_longitude_slice('reflectivity', lon=lon, lat=lat,
                                ax=ax2,
                                vmin=vmin, vmax=vmax,
                                cmap=cmapname)

    ax2.set_ylim([zcross_bottom, zcross_top])
    ax2.set_xlim([xcross_left, xcross_right])

    # Panel 3: latitude slice
    ax3 = fig.add_axes(y_cut_panel_axes)
    display.plot_latitude_slice('reflectivity', lon=lon, lat=lat,
                                ax=ax3,
                                vmin=vmin, vmax=vmax,
                                cmap=cmapname)
    ax3.set_ylim([zcross_bottom, zcross_top])
    ax3.set_xlim([ycross_down, ycross_up])
    plt.close()


def draw_composite_operational(gridfile,outpath='.',level=4,vmin=0,vmax=75,colorfile=None):

    if not os.path.exists(gridfile):
        print(gridfile + ' not exists!')
        return False
    try:
        grid = pyart.io.read_grid(gridfile)
    except:
        print(gridfile + ' read error!')
        return False
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    (filepath,filename) = os.path.split(gridfile)
    tstr = filename[7:19]
    tt = datetime.strptime(tstr,'%Y%m%d%H%M') + timedelta(hours=8)

    tstr_date = '%04d年%02d月%02d日'%(tt.year,tt.month,tt.day)
    tstr_time = '%02d时%02d分（北京时）'%(tt.hour,tt.minute)
    font_large=FontProperties(fname=FONT_FILE, size=14)
    font_mid=FontProperties(fname=FONT_FILE, size=10)

    min_ref=10
    am = grid.fields['reflectivity']['data'].mask
    da = grid.fields['reflectivity']['data'].data
    am[da < min_ref] = True
    xrange = grid.x['data'].max()
    yrange = grid.y['data'].max()
    origin_lat = grid.origin_latitude['data'][0]
    origin_lon = grid.origin_longitude['data'][0]
    [startlon,startlat]=pyart.core.cartesian_to_geographic_aeqd(-1*xrange,-1*yrange,origin_lon,origin_lat)
    [endlon,endlat]=pyart.core.cartesian_to_geographic_aeqd(1*xrange,1*yrange,origin_lon,origin_lat)

    display = pyart.graph.GridMapDisplay(grid)


    ax_pos_main   = [0.05, 0.05, .65, .65]
    ax_pos_top    = [0.05, 0.71, .65, .2] 
    ax_pos_right  = [0.71, 0.05, .2, .65]
    ax_pos_cbar   = [0.92, 0.05, .03, .65]


    fig = plt.figure(figsize=(8,8))
    projection = ccrs.PlateCarree()
    ax_main = fig.add_axes(ax_pos_main, projection=projection)
    
    fig.text(0.5,0.97, '三维雷达组网拼图-多视角组合图', va="center", ha="center", font=font_large ) 
    
    # add product info
    infostr = '要 素：' + '反射率因子'
    fig.text(0.71,0.89, infostr, va="center", ha="left", font=font_mid ) 
    infostr = '日 期：' + tstr_date
    fig.text(0.71,0.86, infostr, va="center", ha="left", font=font_mid ) 
    infostr = '时 间：' + tstr_time
    fig.text(0.71,0.83, infostr, va="center", ha="left", font=font_mid ) 
    infostr = '高 度：%d米(左下图)'%grid.z['data'][level]
    fig.text(0.71,0.80, infostr, va="center", ha="left", font=font_mid ) 

    ax_main.axis('tight')
    
    xlim_west = startlon[0]
    xlim_east = endlon[0]
    ylim_south = startlat[0]
    ylim_north = endlat[0]
    
    
    field = 'reflectivity'
    if colorfile is None:
        cmapname = 'NWSRef'
        cmap = common.parse_cmap(cmapname, field)
    else:
        cmap,norm=parse(colorfile)
        # outdic= parse(colorfile)
        # cmap=outdic['cmap']
        # norm=outdic['norm']

    # display.plot_grid(field, level, vmin=vmin, vmax=vmax,
    #                 ax=ax_main,colorbar_flag=False,title_flag=False,
    #                 projection=projection,axislabels_flag=False,title=None,
    #                 cmap=cmap,norm=norm)
    ds = grid.to_xarray()
    ds[field][0, level].plot.pcolormesh(
            x="lon",
            y="lat",
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            add_colorbar=False,
        )
    add_china_map_2cartopy(ax_main, name='province', facecolor='none',edgecolor=None, lw=1)
    draw_gisinfo(ax_main,slat=ylim_south,nlat=ylim_north,wlon=xlim_west,elon=xlim_east)
    
    infostr = '基于 metradar 制作'

    fig.text(0.05,0.03, infostr, va="center", ha="left", font=font_mid ) 

    ax_main.set_ylim([ylim_south, ylim_north])
    ax_main.set_xlim([xlim_west, xlim_east])
    ax_main.set_xticks([])
    ax_main.set_yticks([])
    ax_main.set_xlabel('')
    ax_main.set_ylabel('')
    plt.title('')

    ax_top = fig.add_axes(ax_pos_top)
    # ax_top.axis('equal')
    x_1d = grid.x['data'] / 1000
    y_1d = grid.y['data'] / 1000
    z_1d = grid.z['data'] / 1000
    # data = self.grid.fields[field]['data'][:, y_index, :]
    # data = np.ma.masked_outside(data, vmin, vmax)

    ref = grid.fields[field]['data']
    data1 = np.max(ref,1)
    xd, zd = np.meshgrid(x_1d, z_1d)
    pm_top = ax_top.pcolormesh(
            xd, zd, data1, vmin=vmin, vmax=vmax,cmap=cmap )
    ax_top.set_ylim([0, 20])
    plt.yticks([0,5,10,15,20])
    plt.xticks([])
    plt.title('经度-高度图（公里）（沿经向取最大值）',fontproperties=font_mid)
    plt.grid(color='k',axis='y')
    
    ax_right = fig.add_axes(ax_pos_right)
    data2 = np.max(ref,2)
    zd, xd = np.meshgrid(z_1d, x_1d)
    pm_right = ax_right.pcolormesh(
            zd, xd, data2.T, vmin=vmin, vmax=vmax,cmap=cmap )
    ax_right.set_xlim([0, 20])
    plt.xticks([0,5,10,15,20])
    plt.yticks([])
    plt.title('纬度-高度图（公里）\n（沿纬向取最大值）',fontproperties=font_mid)
    plt.grid(color='k',axis='x')

    # add colorbar
    ax_cbar = fig.add_axes(ax_pos_cbar)
    cb=plt.colorbar(pm_right,cax=ax_cbar,orientation='vertical')#方向
    plt.title('dBZ')
    plt.savefig(outpath + os.sep + filename.replace('.nc','.png'),dpi=600)
    plt.close()
    # plt.show()
    # create axis
    print('save figure to ' + outpath + os.sep + filename.replace('.nc','.png'))
    pass

if __name__ == "__main__":

    
    path = '/mnt/e/metradar_test/vpr/mosaic/20230731_daxing'
    outpath = '/mnt/e/metradar_test/vpr/mosaic/20230731_daxing/mosac_pic'

    print(os.listdir(path))
    for file in os.listdir(path):
        if not file.endswith('.nc'):
            continue
        draw_composite_operational(path + os.sep + file,outpath,colorfile=None)
        pass


