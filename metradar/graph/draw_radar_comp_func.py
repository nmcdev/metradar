

# _*_ coding: utf-8 _*_

# Copyright (c) 2026 NMC Developers.
# Distributed under the terms of the GPL V3 License.
'''
画雷达综合图主程序

'''


from metradar.io.decode_fmt_pyart import read_cnrad_fmt
import pyart
import os
import numpy as np
import numpy.ma as MA
from metradar.io.decode_pup_rose import READ_ROSE
import matplotlib.pyplot as plt
import cartopy.crs as ccrs

from netCDF4 import num2date
import xarray 
from cartopy.io.shapereader import Reader
from matplotlib.font_manager import FontProperties
import pandas as pd 
from metpy.calc import wind_components
from metpy.units import units
from metradar.util.parse_pal import parse_pro
from matplotlib.colorbar import ColorbarBase
import matplotlib.patheffects as path_effects
from matplotlib import patheffects
import matplotlib.transforms as transforms 
from matplotlib.transforms import offset_copy
from metradar.core.oa_dig_func import Object_Analyst
import warnings
warnings.filterwarnings('ignore')
import matplotlib as mpl

from metradar.core.get_cross_section import get_cross_radar
import matplotlib
# matplotlib.use('Agg')

from matplotlib import ticker
import xarray as xr
from metradar.config import CONFIG

# 资源文件路径
RESOURCES_PATH = CONFIG.get('SETTING','RESOURCES_PATH')
MAP_PATH = RESOURCES_PATH + '/maps'


# 初始化参数字典，用于存储绘图所需的所有参数
def ini_params():
    param={'radarfile_path':None, # 雷达文件路径
            'radarfile_name':None, # 雷达文件名 
            'rose_cr_path':None, # ROSE组合反射率因子文件路径
            'rose_cr_name':None, # ROSE组合反射率因子文件名
            'mosaicfile_path':None,# 反射率拼图文件路径
            'mosaicfile_name':None, # 反射率拼图文件名
            'pic_path':None, # 图片保存路径
            'timestr':None, # 图片时间
            'aws_min_file_path':None, # aws分钟数据文件路径
            'aws_min_file_name':None, # aws分钟数据文件名
            'aws_hour_file_path':None, # aws小时数据文件路径
            'aws_hour_file_name':None,# aws小时数据文件名
            'rose_cr_path':None, # ROSE组合反射率因子文件路径
            'rose_cr_name':None, # ROSE组合反射率因子文件名
            'aws_min_delta_t_file_path':None, # aws小时变温数据文件路径
            'aws_min_delta_t_file_name':None,# aws小时变温数据文件名
            'aws_min_delta_p_file_path':None, # aws小时变压数据文件路径
            'aws_min_delta_p_file_name':None,# aws小时变压数据文件名
            'wind_bar_width_inner':None,#wind bar 里线宽
            'wind_bar_width_outter':None,#wind bar 外线宽
            'gis_name':None, # 站点名称
            'gis_lats':None, # 站点纬度
            'gis_lons':None, # 站点经度
            'slat':None, # 绘图区域南纬
            'nlat':None, # 绘图区域北纬
            'wlon':None, # 绘图区域西经
            'elon':None, # 绘图区域东经
            'ref_colorfile':None, # 回波强度色标文件
            'vel_colorfile':None, # 径向速度色标文件
            'cc_colorfile':None, # 相关系数色标文件
            'zdr_colorfile':None, # 差分反射率色标文件
            'kdp_colorfile':None, # 差分相移率色标文件
            'fontfile':None, # 字体文件
            'dpi':300, # 图片分辨率
            'pic_format':'png', # 图片格式
            'figsize_width':4, # 图片宽度
            'figsize_height':4, # 图片高度
            'fontsize_gis':4, # GIS字体大小
            'fontsize_colorbar':3, # 色标字体大小
            'fontsize_title':4,# 标题字体大小
            'fontsize_tick':6, # 坐标轴刻度字体大小
            'bdraw_title_ppi':True, # 是否给PPI图绘制标题
            'bdraw_title_crs':True, # 是否给垂直剖面图绘制标题
            'mapcolor':[170/255,170/255,170/255], # 行政边界颜色
            'linewidth_map': 0.5,# 行政边界线宽
            'contour_color':[0/255,0/255,0/255], # 等值线颜色
            'linewidth_cntr': 0.8,# 等值线线宽
            'breplace':True, #如果图片文件已存在，是否重新绘制
            'bdraw_crs':False, # 是否绘制垂直剖面图
            'thred_pre1h': 30, # 1小时降水阈值
            'thred_ref':0, # 回波强度阈值
            'radarname': 'XX',# 雷达站名
            
        }
    return param


# FuncFormatter can be used as a decorator
@ticker.FuncFormatter
def major_formatter_lon(x, pos):
    return f'{x:.1f}°E'

@ticker.FuncFormatter
def major_formatter_lat(x, pos):
    return f'{x:.1f}°N'

class DRAW_RADAR_OTHER:
    
    def __init__(self,params):
        pass
        self.radarfile_path=params['radarfile_path']
        self.radarfile_name=params['radarfile_name']
        self.rose_cr_path=params['rose_cr_path']
        self.rose_cr_name=params['rose_cr_name']
        self.mosaicfile_path=params['mosaicfile_path']
        self.mosaicfile_name=params['mosaicfile_name']
        self.aws_min_file_path=params['aws_min_file_path']
        self.aws_min_file_name=params['aws_min_file_name']

        self.aws_min_delta_t_file_path=params['aws_min_delta_t_file_path']
        self.aws_min_delta_t_file_name=params['aws_min_delta_t_file_name']
        self.aws_min_delta_p_file_path=params['aws_min_delta_p_file_path']
        self.aws_min_delta_p_file_name=params['aws_min_delta_p_file_name']

        self.aws_hour_file_path=params['aws_hour_file_path']
        self.aws_hour_file_name=params['aws_hour_file_name']
        self.gis_name=params['gis_name']
        self.gis_lats=params['gis_lats']
        self.gis_lons=params['gis_lons']
        self.slat=params['slat']
        self.nlat=params['nlat']
        self.wlon=params['wlon']
        self.elon=params['elon']
        self.wind_bar_width_inner=params['wind_bar_width_inner']
        self.wind_bar_width_outter=params['wind_bar_width_outter']
        self.ref_colorfile=params['ref_colorfile']
        self.vel_colorfile=params['vel_colorfile']
        self.cc_colorfile=params['cc_colorfile']
        self.zdr_colorfile=params['zdr_colorfile']
        self.fontfile=params['fontfile']
        self.picpath=params['pic_path']
        self.timestr=params['timestr']
        self.breplace=params['breplace']
        self.mapcolor=params['mapcolor']
        self.linewidth_map=params['linewidth_map']
        self.contour_color=params['contour_color']
        self.linewidth_cntr=params['linewidth_cntr']
        self.dpi = params['dpi']
        self.paintsize_x = params['figsize_width']
        self.paintsize_y = params['figsize_height']

        self.gatefilter_ref = None
        self.pic_format = params['pic_format']
        self.fontsize_gis=params['fontsize_gis']
        self.fontsize_colorbar=params['fontsize_colorbar']
        self.fontsize_title=params['fontsize_title']
        self.bdraw_title_ppi=params['bdraw_title_ppi']
        self.bdraw_title_crs=params['bdraw_title_crs']
        self.crs_paintsize_x = params['figsize_width']
        self.bdraw_crs=params['bdraw_crs']
        self.fontsize_tick=params['fontsize_tick']
        self.thred_pre1h=params['thred_pre1h']
        self.thred_ref =params['thred_ref']
        self.radarname = params['radarname']
        self.linewidth_cntr = params['linewidth_cntr']
        if self.bdraw_crs == True:
            self.crs_start_azi=params['crs_start_azi']
            self.crs_end_azi=params['crs_end_azi']
            self.crs_start_range=params['crs_start_range']
            self.crs_end_range=params['crs_end_range']
            self.top_height = params['top_height']
            
        else:
            self.crs_start_azi=None
            self.crs_end_azi=None
            self.crs_start_range=None
            self.crs_end_range=None
            self.top_height = None

        try:
            if not os.path.exists(self.picpath): os.makedirs(self.picpath)
        except:
            pass

        self.ratio = (self.nlat - self.slat)/(self.elon-self.wlon)

        
        # 设置坐标轴字体大小

        mpl.rcParams['font.size'] = self.fontsize_tick
        mpl.rcParams['font.weight'] = 'bold'
        # mpl.rcParams['font.family'] = 'Times New Roman'
        mpl.rcParams['xtick.direction'] = 'in'  # 'out'  'inout'
        mpl.rcParams['ytick.direction'] = 'in'  # 'out'  'inout'


        self.cref=None
        self.voltime = None
        
        self.disratio =  self.paintsize_x / 2
        # self.disratio=1

        self.font_gis=FontProperties(fname=self.fontfile, size=self.fontsize_gis)
        self.font_colorbar=FontProperties(fname=self.fontfile, size=self.fontsize_colorbar)
        self.font_indicate=FontProperties(fname=self.fontfile, size=self.fontsize_colorbar*2)
        self.font_title=FontProperties(fname=self.fontfile, size=self.fontsize_title)
        self.stroke_line_width=1 * self.disratio
        self.cref=None
        self.voltime = None
        self.crs_data=None
        self.crs_fig=None

        # self.axes_pos = [0.01, 0.06, 0.98, 0.88]
        self.axes_pos = [0.05, 0.07, 0.9, 0.9]
        self.cbaxes_pos = [0.05, 0.01, 0.9, 0.05]
        # self.cbaxes_pos = [0.8,0.15,0.02,0.6]

        self.crs_axes_pos = [0.15, 0.15, 0.70, 0.8]
        self.crs_cbaxes_pos = [0.86, 0.15, 0.03, 0.8]
        self.radar = None

    def read_vol_data(self,):
        # filepath = 'data/'
        # filename = 'ZZHN.20210720.180000.ar2v'
        if self.radarfile_name is None:
            print('no valid radfile!')
            return False
        if not os.path.exists(self.radarfile_path + os.sep + self.radarfile_name):
            print(self.radarfile_path + os.sep + self.radarfile_name + ' not exists!')
            return False
        self.radar = read_cnrad_fmt(self.radarfile_path + os.sep + self.radarfile_name)
        # self.radar.fields.keys()
        self.display = pyart.graph.RadarMapDisplay(self.radar)
        self.gatefilter_ref = pyart.correct.GateFilter(self.radar)
        self.gatefilter_ref.exclude_below('reflectivity', self.thred_ref, exclude_masked=True, op='or', inclusive=False)
    
        
        self.g_rad_lon = self.radar.longitude['data'][0]
        self.g_rad_lat = self.radar.latitude['data'][0]

        times_t = self.radar.time['data'][0]
        units_t = self.radar.time['units']
        calendar = self.radar.time['calendar']
        voltime= num2date(times_t, units_t, calendar, only_use_cftime_datetimes=False,
                        only_use_python_datetimes=True)
        datetime_format='%Y%m%d-%H:%M'
        self.voltime = voltime.strftime(datetime_format)
        return True

    def get_crs_from_radar(self,):
        pass
        param_crs=dict()
        param_crs['crs_start_azi'] = self.crs_start_azi
        param_crs['crs_start_range'] = self.crs_start_range
        param_crs['crs_end_azi'] = self.crs_end_azi
        param_crs['crs_end_range'] = self.crs_end_range
        param_crs['top_height'] = self.top_height
        # st = time.time()
        if self.radar is None:
            return False
   
        self.crs_data = get_cross_radar(self.radar,param_crs)
        pass

    def draw_crs(self,subdir='垂直剖面',varname=None,brefresh=False,thred=None,):
        # check 
    
        if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.jpg') and not brefresh:
            return False
        if self.crs_data is None:
            print('warning: crs_data not pre loaded!')
            return False

        if varname is None:
            print('warning: varname is None!')
            return False

        if not thred is None:
            flag = self.crs_data[varname] < thred
            self.crs_data[varname] = MA.masked_array(self.crs_data[varname], mask=flag)

        colorfile = self.ref_colorfile
        if varname.find('ref') >=0:
            colorfile = self.ref_colorfile
        elif varname.find('vel') >=0:
            colorfile = self.vel_colorfile
        else:
            print('warning: varname = %s not config the colorfile in the draw_crs function!'%varname)
            return False

        colordata = parse_pro(colorfile)

        self.crs_ratio = 0.6
        self.crs_paintsize_y = self.crs_paintsize_x * self.crs_ratio
        self.crs_fig = plt.figure(figsize=(self.crs_paintsize_x,self.crs_paintsize_y))
        map_panel_axes = self.crs_axes_pos
        self.crs_ax = self.crs_fig.add_axes(map_panel_axes)
        plt.pcolormesh(self.crs_data[varname],cmap=colordata['cmap'],norm=colordata['norm'])
        xticks = self.crs_ax.get_xticks()
        if xticks[-1] < self.crs_data[varname].shape[1]:
            xticks.append(self.crs_data[varname].shape[1])
        else:
            xticks[-1] = self.crs_data[varname].shape[1]

        xticklabels = []
        for ix in range(len(xticks)):
           xticklabels.append('%.1f'%(xticks[ix]*self.crs_data['xreso']))
        xticklabels[0]='A'
        xticklabels[-1]='B'
        
        self.crs_ax.set_xticks(xticks)
        self.crs_ax.set_xticklabels(xticklabels)

        yticks = self.crs_ax.get_yticks()
        if yticks[-1] < self.crs_data[varname].shape[0]:
            yticks.append(self.crs_data[varname].shape[0])
        yticklabels = []
        for iy in range(len(yticks)):
           yticklabels.append('%.1f'%(yticks[iy]*self.crs_data['yreso']))
        
        self.crs_ax.set_yticklabels(yticklabels)
        self.crs_ax.set_xlabel('水平距离(KM)',font=self.font_colorbar)
        self.crs_ax.set_ylabel('垂直高度(KM)',font=self.font_colorbar)
        plt.grid()
        self.draw_title_crs(varname=varname)
        self.draw_crs_colorbar(colorfile=colorfile)
        self.crs_finish_save(subdir=subdir,add_str=varname,)

    def draw_title_crs(self,varname='reflectivity'):
        if not self.bdraw_title_crs:
            return None
        x,y,z=pyart.core.antenna_to_cartesian(self.crs_start_range, self.crs_start_azi, elevations=0)
        start_lon,start_lat=pyart.core.cartesian_to_geographic_aeqd(x,y,self.g_rad_lon,self.g_rad_lat)
        x,y,z=pyart.core.antenna_to_cartesian(self.crs_end_range, self.crs_end_azi, elevations=0)
        end_lon,end_lat=pyart.core.cartesian_to_geographic_aeqd(x,y,self.g_rad_lon,self.g_rad_lat)
        self.crs_startlat = start_lat[0]
        self.crs_startlon = start_lon[0]
        self.crs_endlat = end_lat[0]
        self.crs_endlon = end_lon[0]
        titlestr = 'A(%.2f,%.2f)->B(%.2f,%.2f) '%(self.crs_startlon,self.crs_startlat,self.crs_endlon,self.crs_endlat)
        tstr=''
        if self.voltime is None:
            tstr = self.timestr
        else:
            tstr = self.voltime
        plt.title('%s'%(titlestr),fontproperties=self.font_title,loc='right',verticalalignment='top')

    def draw_crs_colorbar(self,fig=None,colorfile=None,tickstep=5,orientation = 'vertical',cb_ratio=0.618):
        # 画色标
        ax_pos = self.crs_ax.get_position().bounds
        cbar_axes = [ax_pos[0]+ ax_pos[2]+0.005,(ax_pos[1]+ax_pos[1]+ax_pos[3])/2-ax_pos[3]*cb_ratio/2,0.02,ax_pos[3]*cb_ratio]
        cb_ax = self.crs_fig.add_axes(cbar_axes)
        colordata = parse_pro(colorfile)
        
        cbar = ColorbarBase(cb_ax, orientation=orientation, norm=colordata['norm'], cmap=colordata['cmap'],extend='both')
        cbar.set_ticks([])
        # cb_ax.axis('tight')

        trans = transforms.blended_transform_factory(cb_ax.transData, cb_ax.transAxes)
        tick_poss=np.arange(colordata['norm'].vmin, colordata['norm'].vmax, tickstep)
        xrange = colordata['norm'].vmax - colordata['norm'].vmin

        for tick_pos in tick_poss:
            cb_ax.hlines(tick_pos, colordata['norm'].vmin, colordata['norm'].vmin + 0.3*xrange, color="black",linewidth=0.5)
            cb_ax.hlines(tick_pos, colordata['norm'].vmin + 0.7*xrange, colordata['norm'].vmax, color="black",linewidth=0.5)
  
  

            cb_ax.text(1.04,tick_pos, str(int(tick_pos)),va="center", ha="left",fontsize=5) # font=self.font_colorbar

        cb_ax.text(1.8, 1.035, colordata['units'], transform=cb_ax.transAxes,va="center", ha="center", fontsize=5 ) # font=self.font_colorbar 
                  
        

    def add_crs_line(self,ax=None):
        pass
        if not self.bdraw_crs:
            # print('warning: bdraw_crs is False!')
            return False

        if ax is None:
            print('warning: ax is None in function add_crs_line!')
            return False

        x,y,z=pyart.core.antenna_to_cartesian(self.crs_start_range, self.crs_start_azi, elevations=0)
        start_lon,start_lat=pyart.core.cartesian_to_geographic_aeqd(x,y,self.g_rad_lon,self.g_rad_lat)
        x,y,z=pyart.core.antenna_to_cartesian(self.crs_end_range, self.crs_end_azi, elevations=0)
        end_lon,end_lat=pyart.core.cartesian_to_geographic_aeqd(x,y,self.g_rad_lon,self.g_rad_lat)
        self.crs_startlat = start_lat[0]
        self.crs_startlon = start_lon[0]
        self.crs_endlat = end_lat[0]
        self.crs_endlon = end_lon[0]

        simble_color=[0,0,0]
        ax.plot([start_lon[0],end_lon[0]],[start_lat[0],end_lat[0]],color='k',linewidth=0.75)
        ax.plot([start_lon[0],end_lon[0]],[start_lat[0],end_lat[0]],color=simble_color,linewidth=0.5)
        ax.text(start_lon[0],start_lat[0],'A',color=simble_color,font=self.font_colorbar,
                path_effects=[path_effects.Stroke(linewidth=0.25, foreground='black'),path_effects.Normal()])
        ax.text(end_lon[0],end_lat[0],'B',color=simble_color,font=self.font_colorbar,
                path_effects=[path_effects.Stroke(linewidth=0.25, foreground='black'),path_effects.Normal()])

    # 从拼图文件中读取组合反射率
    def get_cref_from_mosaicfile(self,):
        
        data = None
        if not os.path.exists(self.mosaicfile_path + os.sep + self.mosaicfile_name):
            print(self.mosaicfile_path + os.sep + self.mosaicfile_name + ' does not exists!')
            return False
        try:
            data = xarray.open_dataset(self.mosaicfile_path + os.sep + self.mosaicfile_name)
        except:
            print(self.mosaicfile_path + os.sep + self.mosaicfile_name + ' read error!')
            self.cref=None
            return False
        cref = data.cref.data
        self.cref = (cref.astype(np.float32) - data.cref.offset)/data.cref.scale
        
        self.cref = self.cref.transpose()
        self.cref = MA.masked_array(self.cref, mask=self.cref==-32)
        self.grid_lat,self.grid_lon=np.meshgrid(data.lat.data,data.lon.data)

        return True

    # 从ROSE产品中读取组合反射率因子数据
    def get_cref_from_rose(self,):
        
        rose_reader = READ_ROSE()
        data = None
        if not os.path.exists(self.rose_cr_path + os.sep + self.rose_cr_name):
            print(self.rose_cr_path + os.sep + self.rose_cr_name + ' does not exists!')
            return False
        try:
            data = rose_reader.read_cr(self.rose_cr_path, self.rose_cr_name)
        except:
            print(self.rose_cr_path + os.sep + self.rose_cr_name + ' read error!')
            self.cref=None
            return False
        cref = data.data
        
        self.cref = cref.transpose()
        self.cref = MA.masked_array(self.cref, mask=self.cref==-32)
        self.grid_lat,self.grid_lon=np.meshgrid(data.lat.data,data.lon.data)

        return True
    
    # 从单站雷达基数据中获取组合反射率因子，先进行三维插值，再求CR
    def get_cref_from_radar(self,xlimits=[-50,50], ylimits=[-50,50], zlimits=[1,15], xreso=1000, yreso=1000, zreso=500, grid_origin=None):
        print('enter get_cref_from_radar function!')
        
        self.xlims=xlimits # km
        self.ylims=ylimits # km
        self.zlims=zlimits # km
        self.xreso=xreso #m
        self.yreso=yreso #m
        self.zreso=zreso #m
        self.g_xlim = (self.xlims[0]*1000,self.xlims[1]*1000)
        self.g_ylim = (self.ylims[0]*1000,self.ylims[1]*1000)
        self.g_zlim = (self.zlims[0]*1000,self.zlims[1]*1000)
        self.g_numx = int(len(range(self.xlims[0],self.xlims[1]))*1000/self.xreso+1) # x方向格点数
        self.g_numy = int(len(range(self.ylims[0],self.ylims[1]))*1000/self.yreso+1) # y方向格点数
        self.g_numz = int(len(range(self.zlims[0],self.zlims[1]))*1000/self.zreso+1) # z方向格点数
        if grid_origin is None:
            grid_origin = (self.radar.latitude['data'][0], self.radar.longitude['data'][0])
        print('grid shape:',(self.g_numz, self.g_numy, self.g_numx))
        print('grid limits:',(self.g_zlim, self.g_ylim, self.g_xlim))
        print('grid origin:',grid_origin)
        self.grid=pyart.map.grid_from_radars(
            (self.radar,),
            grid_origin=grid_origin,
            weighting_function = 'BARNES2',
            grid_shape=(self.g_numz, self.g_numy, self.g_numx),
            grid_limits=(self.g_zlim, self.g_ylim, self.g_xlim),
            fields=['reflectivity'])

        xrdata = self.grid.to_xarray()

        self.cref = np.nanmax(xrdata.reflectivity.data[0],axis=0) 
        self.cref = self.cref.transpose()
        self.grid_lat = xrdata.lat.data
        self.grid_lon = xrdata.lon.data
        

    def draw_cref(self,):
        cmapdic= parse_pro(self.ref_colorfile)
        plt.pcolormesh(self.grid_lon,self.grid_lat,self.cref,cmap=cmapdic['cmap'],norm=cmapdic['norm'])  
    
    def draw_divergence(self,zorder=2):
        # cmapdic= parse_pro(self.ref_colorfile)
        # plt.pcolormesh(self.grid_lon,self.grid_lat,self.cref,cmap=cmapdic['cmap'],norm=cmapdic['norm'])  
        # 叠加涡度场
        # filepath_minute='temp/backup_aws_minute'
        # filename_aws='surface_aws_20210720_1800.csv'
        if self.aws_min_file_name is None:
            print('warning: aws_min_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_file_path + os.sep + self.aws_min_file_name):
            print(self.aws_min_file_path + os.sep + self.aws_min_file_name + ' does not exists!')
            return False
        awsdata = pd.read_csv(self.aws_min_file_path + os.sep + self.aws_min_file_name,encoding='GBK')

        aws_lats=awsdata['Lat'].values
        aws_lons=awsdata['Lon'].values
        aws_2minspd=awsdata['WIN_S_Avg_2mi'].values
        aws_2mindir=awsdata['WIN_D_Avg_2mi'].values
        # u,v=wind_components(aws_2minspd,aws_2mindir)

        validflag=[]
        for nn in range(len(aws_lats)):
            if aws_2minspd[nn] > 999000 or aws_2mindir[nn] > 999000 or aws_2minspd[nn] < 1:
                continue
            else:
                validflag.append(nn)
        aws_2minspd = aws_2minspd[validflag]*units('m/s')
        aws_2mindir = aws_2mindir[validflag]*units.degree
        oa_class = Object_Analyst()
        oa_class.set_reso(0.01)
        vtx_div = oa_class.calc_vor_div(aws_2minspd,aws_2mindir,aws_lons,aws_lats)

        levels = list(range(-80, 80, 2))

        cntr = self.ax1.contour(vtx_div.lon.values,vtx_div.lat.values,vtx_div.div_10m.values,
                                levels=levels,colors=self.contour_color,zorder=zorder,linewidths=self.linewidth_cntr)
        
        plt.setp(cntr.collections, path_effects=[
            patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
        clbls = self.ax1.clabel(cntr,fmt="%2.1f",use_clabeltext=True,fontsize=self.fontsize_colorbar)

        plt.setp(cntr.labelTexts, path_effects=[
            patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])

    # 画温度场客观分析等值线
    def draw_temperature(self,zorder=2):
        if self.aws_min_file_name is None:
            print('warning: aws_min_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_file_path + os.sep + self.aws_min_file_name):
            print(self.aws_min_file_path + os.sep + self.aws_min_file_name + ' does not exists!')
            return False
        awsdata = pd.read_csv(self.aws_min_file_path + os.sep + self.aws_min_file_name,encoding='GBK')

        aws_lats=awsdata['Lat'].values
        aws_lons=awsdata['Lon'].values
        aws_tem = awsdata['TEM'] # 温度
        aws_dpt = awsdata['DPT'] # 露点

        validflag=[]
        for nn in range(len(aws_lats)):
            if aws_tem[nn] > 999000 :
                continue
            else:
                validflag.append(nn)
        aws_tem = aws_tem[validflag]
        aws_lons = aws_lons[validflag]
        aws_lats = aws_lats[validflag]

        oa_class = Object_Analyst()
        oa_class.set_reso(0.01)
        params={}
        params['in_lon'] = aws_lons
        params['in_lat'] = aws_lats
        params['in_data'] = aws_tem
        params['out_varname'] = 't2m'
        params['out_long_name'] = 'surface temperature objective analyse'
        params['out_short_name'] = 'oa_t'
        params['out_units'] = 'degC'
        
        t2m = oa_class.do_oa_base(params)

        # levels = list(range(-40, 50, 0.5))
        levels = np.arange(-40,50,0.5)


        cntr = self.ax1.contour(t2m.lon.values,t2m.lat.values,t2m.t2m.values,
                                levels=levels,colors='k',zorder=zorder)
        
        plt.setp(cntr.collections, path_effects=[
            patheffects.withStroke(linewidth=2, foreground="w")])
        clbls = self.ax1.clabel(cntr,fmt="%2.1f",use_clabeltext=True,fontsize=self.fontsize_colorbar)

        plt.setp(cntr.labelTexts, path_effects=[
            patheffects.withStroke(linewidth=2, foreground="w")])


    # 画露点温度场客观分析等值线
    def draw_drewpoint(self,zorder=2):
        if self.aws_min_file_name is None:
            print('warning: aws_min_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_file_path + os.sep + self.aws_min_file_name):
            print(self.aws_min_file_path + os.sep + self.aws_min_file_name + ' does not exists!')
            return False
        awsdata = pd.read_csv(self.aws_min_file_path + os.sep + self.aws_min_file_name,encoding='GBK')

        aws_lats=awsdata['Lat'].values
        aws_lons=awsdata['Lon'].values
        aws_tem = awsdata['TEM'] # 温度
        aws_dpt = awsdata['DPT'] # 露点

        validflag=[]
        for nn in range(len(aws_lats)):
            if aws_dpt[nn] > 999000 :
                continue
            else:
                validflag.append(nn)
        aws_dpt = aws_dpt[validflag]
        aws_lons = aws_lons[validflag]
        aws_lats = aws_lats[validflag]

        oa_class = Object_Analyst()
        oa_class.set_reso(0.01)
        params={}
        params['in_lon'] = aws_lons
        params['in_lat'] = aws_lats
        params['in_data'] = aws_dpt
        params['out_varname'] = 'dpt2m'
        params['out_long_name'] = 'surface drew temperature objective analyse'
        params['out_short_name'] = 'oa_dpt'
        params['out_units'] = 'degC'
        
        dpt2m = oa_class.do_oa_base(params)

        levels = np.arange(-40,50,0.5)
       
        cntr = self.ax1.contour(dpt2m.lon.values,dpt2m.lat.values,dpt2m.dpt2m.values,
                                levels=levels,colors='k',zorder=zorder)
        
        plt.setp(cntr.collections, path_effects=[
            patheffects.withStroke(linewidth=2, foreground="w")])
        clbls = self.ax1.clabel(cntr,fmt="%2.1f",use_clabeltext=True,fontsize=self.fontsize_colorbar)

        plt.setp(cntr.labelTexts, path_effects=[
            patheffects.withStroke(linewidth=2, foreground="w")])

    def draw_vortex(self,zorder=2):
        if self.aws_min_file_name is None:
            print('warning: aws_min_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_file_path + os.sep + self.aws_min_file_name):
            print(self.aws_min_file_path + os.sep + self.aws_min_file_name + ' does not exists!')
            return False
        awsdata = pd.read_csv(self.aws_min_file_path + os.sep + self.aws_min_file_name,encoding='GBK')

        aws_lats=awsdata['Lat'].values
        aws_lons=awsdata['Lon'].values
        aws_2minspd=awsdata['WIN_S_Avg_2mi'].values
        aws_2mindir=awsdata['WIN_D_Avg_2mi'].values

        validflag=[]
        for nn in range(len(aws_lats)):
            if aws_2minspd[nn] > 999000 or aws_2mindir[nn] > 999000 or aws_2minspd[nn] < 1:
                continue
            else:
                validflag.append(nn)
        aws_2minspd = aws_2minspd[validflag]*units('m/s')
        aws_2mindir = aws_2mindir[validflag]*units.degree
        oa_class = Object_Analyst()
        oa_class.set_reso(0.01)
        vtx_div = oa_class.calc_vor_div(aws_2minspd,aws_2mindir,aws_lons,aws_lats)
        kk=0
        levels = list(range(-80, 80, 2))
      
        cntr = self.ax1.contour(vtx_div.lon.values,vtx_div.lat.values,vtx_div.vtx_10m.values,
                                levels=levels,colors='k',zorder=zorder)
        for artist in cntr.get_children():
            if isinstance(artist, matplotlib.collections.PathCollection):
                artist.set_path_effects([patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
                clbls = self.ax1.clabel(artist,fmt="%2.1f",use_clabeltext=True,fontsize=self.fontsize_colorbar)
                
        plt.setp(cntr.labelTexts, path_effects=[
            patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
        
    def draw_ppi(self,fieldname='reflectivity',tilt=0,colorfile=None,**kwargs):
        cmapdic = parse_pro(colorfile)
        if not isinstance(fieldname,str):
            print('fieldname should be string')
            return False
        if not isinstance(tilt,int):
            print('tilt should be int and >=0') 
            return False
        
        
        self.display.plot_ppi_map(fieldname, tilt,cmap=cmapdic['cmap'],gatefilter=self.gatefilter_ref,
                            min_lon=self.wlon, max_lon=self.elon, norm=cmapdic['norm'],ax=self.ax1,
                            min_lat=self.slat, max_lat=self.nlat,title_flag=False,
                            fig=self.fig, lat_0=self.g_rad_lat,lon_0=self.g_rad_lon,colorbar_flag=False,
                            **kwargs)

    def draw_wind_barb(self,zorder=1):
        # 画风场
        # filepath_minute='temp/backup_aws_minute'
        # filename_aws='surface_aws_20210720_1800.csv'
        if self.aws_min_file_name is None:
            print('warning: aws_min_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_file_path + os.sep + self.aws_min_file_name):
            print(self.aws_min_file_path + os.sep + self.aws_min_file_name + ' does not exists!')
            return False
        
        awsdata = pd.read_csv(self.aws_min_file_path + os.sep + self.aws_min_file_name,encoding='GBK')

        aws_lats=awsdata['Lat'].values
        aws_lons=awsdata['Lon'].values
        aws_2minspd=awsdata['WIN_S_Avg_2mi'].values
        aws_2mindir=awsdata['WIN_D_Avg_2mi'].values
        u,v=wind_components(aws_2minspd*units('m/s'),aws_2mindir*units.deg)
        for nn in range(len(aws_lats)):
            if aws_2minspd[nn] > 999000 or aws_2mindir[nn] > 999000 or aws_2minspd[nn] < 1:
                continue
            u,v=wind_components(float(2)*units('m/s'),int(aws_2mindir[nn])*units.deg)
            plt.barbs(aws_lons[nn],aws_lats[nn],u.magnitude*2.5,v.magnitude*2.5,length=5,barbcolor='black',linewidth=self.wind_bar_width_outter,zorder=zorder) 
            plt.barbs(aws_lons[nn],aws_lats[nn],u.magnitude*2.5,v.magnitude*2.5,length=5,barbcolor='white',linewidth=self.wind_bar_width_inner,zorder=zorder) 

    
    def draw_wind_quiver(self,zorder=1):
        
        if self.aws_min_file_name is None:
            print('warning: aws_min_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_file_path + os.sep + self.aws_min_file_name):
            print(self.aws_min_file_path + os.sep + self.aws_min_file_name + ' does not exists!')
            return False
        
        awsdata = pd.read_csv(self.aws_min_file_path + os.sep + self.aws_min_file_name,encoding='GBK')

        aws_lats=awsdata['Lat'].values
        aws_lons=awsdata['Lon'].values
        aws_2minspd=awsdata['WIN_S_Avg_2mi'].values
        aws_2mindir=awsdata['WIN_D_Avg_2mi'].values
        u,v=wind_components(aws_2minspd*units('m/s'),aws_2mindir*units.deg)
        for nn in range(len(aws_lats)):
            if aws_2minspd[nn] > 999000 or aws_2mindir[nn] > 999000 or aws_2minspd[nn] < 1:
                continue
            u,v=wind_components(float(2)*units('m/s'),int(aws_2mindir[nn])*units.deg)
           
            self.ax1.quiver(aws_lons[nn],aws_lats[nn],u.magnitude,v.magnitude,width=0.003,minshaft=1,scale=60,facecolor=[0,0,0],
                       path_effects=[path_effects.Stroke(linewidth=0.5, foreground='white'),path_effects.Normal()],label='地面2分钟平均风',
                       zorder=zorder)#


    def draw_pre_1h(self,):
        # 叠加1小时降水场
        # filepath_hour = 'temp/backup_aws_hourly'
        # filename_hour = 'surface_aws_hourly_20210720_1000.csv'

        # 方式2：从文件读取数据
        awsdata_hourly = pd.read_csv(self.aws_hour_file_path + os.sep + self.aws_hour_file_name,encoding='GBK')
        awsdata_hourly = awsdata_hourly.sort_values(by='PRE_1h')
        pre_1h = awsdata_hourly['PRE_1h'].values
        pre_lat = awsdata_hourly['Lat'].values
        pre_lon = awsdata_hourly['Lon'].values

        geodetic_transform = ccrs.Geodetic()._as_mpl_transform(self.ax1)
        text_transform = offset_copy(geodetic_transform, units='dots', x=-5)
        for nn in range(len(pre_1h)):
            if pre_1h[nn] > 1000 or pre_1h[nn] < self.thred_pre1h:
                continue
            precolor=[1,1,1]
            # print(pre_1h[nn])
            if pre_1h[nn] >= 50 and pre_1h[nn] < 80:
                precolor=[0,1,1]
            elif pre_1h[nn] >= 80 and pre_1h[nn] < 100:
                precolor=[1,1,0]
            elif pre_1h[nn] >= 100:
                precolor=[1,0,1]
            self.ax1.text(pre_lon[nn],pre_lat[nn],'%d'%(int(pre_1h[nn])), clip_on=True,
                            verticalalignment='center', horizontalalignment='right',
                            transform=text_transform, fontproperties=self.font_colorbar, color=precolor,
                            path_effects=[path_effects.Stroke(linewidth=1, foreground='black'),path_effects.Normal()])

    def draw_colorbar(self,colorfile,tickstep=5,orientation='vertical',cb_ratio=0.618):
        # 画色标
        
        # 水平色标
        if orientation == 'horizontal':
            # cbar_axes = [0.01, 0.01, 0.95, 0.04]
            cbar_axes = [self.map_panel_axes[0],self.map_panel_axes[1]+0.01+self.map_panel_axes[3],self.map_panel_axes[2]*0.618,0.03]
            
            ax2 = self.fig.add_axes(cbar_axes)
            ax2.set_axis_off()
            cmapdic = parse_pro(colorfile)
            cbar = ColorbarBase(ax2, orientation="horizontal", norm=cmapdic['norm'], cmap=cmapdic['cmap'],extend='both')
            cbar.set_ticks([])
            trans = transforms.blended_transform_factory(ax2.transData, ax2.transAxes)
            tick_poss=np.arange(cmapdic['norm'].vmin, cmapdic['norm'].vmax, tickstep)
            for tick_pos in tick_poss:
                ax2.vlines(tick_pos, 0, 0.2, transform=trans, color="black")
                ax2.vlines(tick_pos, 0.85, 1, transform=trans, color="black")

                ax2.text(tick_pos, 1.4, str(int(tick_pos)),va="center", ha="center", transform=trans, fontsize = self.fontsize_colorbar)
    
            ax2.text(1.02, 0.5, cmapdic['units'], transform=ax2.transAxes,va="center", ha="center",fontsize = self.fontsize_colorbar)
            

        # 垂直色标
        if orientation == 'vertical':
            ax_pos = self.ax1.get_position().bounds
            cbar_axes = [ax_pos[0]+ ax_pos[2]+0.01,(ax_pos[1]+ax_pos[1]+ax_pos[3])/2-ax_pos[3]*cb_ratio/2,0.02,ax_pos[3]*cb_ratio]
            cb_ax = self.fig.add_axes(cbar_axes)
            colordata = parse_pro(colorfile)
            
            cbar = ColorbarBase(cb_ax, orientation=orientation, norm=colordata['norm'], cmap=colordata['cmap'],extend='both')
            cbar.set_ticks([])
            trans = transforms.blended_transform_factory(cb_ax.transData, cb_ax.transAxes)
            tick_poss=np.arange(colordata['norm'].vmin, colordata['norm'].vmax, tickstep)
            xrange = colordata['norm'].vmax - colordata['norm'].vmin

            for tick_pos in tick_poss:
                cb_ax.hlines(tick_pos, colordata['norm'].vmin, colordata['norm'].vmin + 0.3*xrange, color="black",linewidth=0.5)
                cb_ax.hlines(tick_pos, colordata['norm'].vmin + 0.7*xrange, colordata['norm'].vmax, color="black",linewidth=0.5)
    

                cb_ax.text(1.04,tick_pos, str(int(tick_pos)),va="center", ha="left",fontsize=5) # font=self.font_colorbar

            cb_ax.text(1.85, 1.035, colordata['units'], transform=cb_ax.transAxes,va="center", ha="center", fontsize=5 ) # ,font=self.font_colorbar
                       
        
    
    def draw_gisinfo(self,):
        geodetic_transform = ccrs.Geodetic()._as_mpl_transform(self.ax1)
        text_transform = offset_copy(geodetic_transform, units='dots', x=-5)
        aa = (np.array(self.gis_lats)<self.nlat) & (np.array(self.gis_lats) > self.slat)
        bb = (np.array(self.gis_lons)<self.elon) & (np.array(self.gis_lons) > self.wlon)
        cc = aa & bb

        alltxt=[]
        for nn in range(len(self.gis_name)):
            if not cc[nn]:
                continue
            curlat = self.gis_lats[nn]
            curlon = self.gis_lons[nn]
            self.ax1.plot(curlon, curlat, marker='o', color=[0.2,0.2,0.2], linestyle='None',
                markersize=1, alpha=0.8, transform=ccrs.PlateCarree())
            txt_lon = curlon
            txt_lat = curlat
            texts = self.ax1.text(txt_lon, txt_lat, self.gis_name[nn], clip_on=True,
                            verticalalignment='center', horizontalalignment='right',
                            transform=text_transform, fontproperties=self.font_gis, color='white',
                            path_effects=[path_effects.Stroke(linewidth=1, foreground='black'),path_effects.Normal()])


    def add_china_map_2cartopy(self, ax, name='province',edgecolor='k', lw=0.8,facecolor='none',transform=None, **kwargs):
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
        names = {'nation': "bou1_4p", 'province': "bou2_4p",
                'county': "BOUNT_poly", 'river': "hyd1_4l",
                'river_high': "hyd2_4l"}

        # get shape filename
        shpfile = MAP_PATH + os.sep + names[name] + ".shp"

        # add map
        ax.add_geometries(
            Reader(shpfile).geometries(), ccrs.PlateCarree(),
            path_effects=[path_effects.Stroke(linewidth=lw+0.3, foreground='white'),path_effects.Normal()],
            facecolor=facecolor, edgecolor=edgecolor, lw=lw, **kwargs)

    def draw_basemap(self,):
        # Setting projection and ploting the second tilt
        projection = ccrs.PlateCarree()
        
        self.fig = plt.figure(figsize=(self.paintsize_x,self.paintsize_y))

        self.map_panel_axes = [0.12, 0.1, .76, .8]
        self.ax1 = self.fig.add_axes(self.map_panel_axes, projection=projection)
        pass

    def add_chinamap(self,):
        self.add_china_map_2cartopy(self.ax1, name='county',edgecolor=self.mapcolor, lw=self.linewidth_map)


    def draw_title(self,titlestr='XX雷达0.5度仰角径向速度'):
        if not self.bdraw_title_ppi:
            return None
        
        tstr=''
        if self.voltime is None:
            tstr = self.timestr
        else:
            tstr = self.voltime

        plt.title('%s(%s BJT)'%(titlestr,tstr),fontproperties=self.font_title,loc='right',verticalalignment='top')

    def crs_finish_save(self,bshow=False,subdir='',add_str='',):

        
        try:
            if not os.path.exists(self.picpath + os.sep + subdir): 
                os.makedirs(self.picpath + os.sep + subdir)
        except:
            pass
        if self.pic_format=='ps':
            plt.savefig(self.picpath + os.sep + subdir + os.sep + self.timestr + '.' + add_str + '.' + self.pic_format,dpi=self.dpi)
        else:
            plt.savefig(self.picpath + os.sep + subdir + os.sep + self.timestr + '.' + add_str + '.' + self.pic_format,dpi=self.dpi,bbox_inches='tight')

        print(self.picpath + os.sep + subdir + os.sep + self.timestr + '.' + add_str + '.' + self.pic_format)
        if bshow:
            plt.show()

        plt.close('all')

    def finish_save(self,bshow=False,subdir='',):
        self.ax1.axis('tight')
        self.ax1.set_xlim([self.wlon,self.elon])
        self.ax1.set_ylim([self.slat,self.nlat])

        if self.elon - self.wlon > 5:
            lon_step = 1
        elif self.elon - self.wlon > 1:
            lon_step = 0.5
        else:
            lon_step = 0.2

        if self.nlat - self.slat > 5:
            lat_step = 1
        elif self.nlat - self.slat > 1:
            lat_step = 0.5
        else:
            lat_step = 0.2
        self.ax1.xaxis.set_major_formatter(major_formatter_lon)
        self.ax1.yaxis.set_major_formatter(major_formatter_lat)
        self.ax1.set_xticks(np.arange(self.wlon,self.elon,lon_step))
        self.ax1.set_yticks(np.arange(self.slat,self.nlat,lat_step))

        self.ax1.minorticks_on()

        # self.ax1.legend(loc='upper right')
        if not os.path.exists(self.picpath + os.sep + subdir): os.makedirs(self.picpath + os.sep + subdir)
        if self.pic_format=='ps':
            plt.savefig(self.picpath + os.sep + subdir + os.sep + self.timestr + '.' + self.pic_format,dpi=self.dpi)
        else:
            plt.savefig(self.picpath + os.sep + subdir + os.sep + self.timestr + '.' + self.pic_format,dpi=self.dpi,bbox_inches='tight')
        print(self.picpath + os.sep + subdir + os.sep + self.timestr + '.' + self.pic_format)
        if bshow:
            plt.show()

        plt.close('all')

    def draw_vel_pre(self,subdir='径向速度+当前小时降水',tilt=1):
        # check 
        if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='velocity',tilt=tilt,colorfile=self.vel_colorfile)
        self.draw_pre_1h()
        self.add_chinamap()
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.vel_colorfile)
        self.finish_save(subdir=subdir)

    def draw_ref_alone(self,subdir='基本反射率',tilt=0,thred=-5,):
        # check 
        if self.radar is None:
            return False
        self.gatefilter_ref = pyart.correct.GateFilter(self.radar)
        self.gatefilter_ref.exclude_below('reflectivity', thred, exclude_masked=True, op='or', inclusive=False)

        ele=np.mean(self.radar.get_elevation(tilt))
        subdir='%.1f_%s'%(ele,subdir)
        if not self.breplace and os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
            print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.add_chinamap()
        self.draw_title(subdir)
        self.draw_colorbar(colorfile=self.ref_colorfile,orientation='vertical')
        self.add_crs_line(ax=self.ax1)
        
        self.finish_save(subdir=subdir,)

    # 画相关系数，偏振量
    def draw_cc_alone(self,subdir='相关系数',tilt=0,thred=-5,):
        # check 
        if self.radar is None:
            return False
        self.gatefilter_ref = pyart.correct.GateFilter(self.radar)
        self.gatefilter_ref.exclude_below('cross_correlation_ratio', thred, exclude_masked=True, op='or', inclusive=False)

        ele=np.mean(self.radar.get_elevation(tilt))
        subdir='%.1f_%s'%(ele,subdir)
        if not self.breplace and os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
            print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='cross_correlation_ratio',tilt=tilt,colorfile=self.ref_colorfile)
        self.add_chinamap()
        self.draw_title(subdir)
        self.draw_colorbar(colorfile=self.cc_colorfile,orientation='vertical')
        self.add_crs_line(ax=self.ax1)
        
        self.finish_save(subdir=subdir,)


    # 画差分反射率，偏振量
    def draw_zdr_alone(self,subdir='差分反射率',tilt=0,thred=-5,):
        # check 
        if self.radar is None:
            return False
        self.gatefilter_ref = pyart.correct.GateFilter(self.radar)
        self.gatefilter_ref.exclude_below('cross_correlation_ratio', thred, exclude_masked=True, op='or', inclusive=False)

        ele=np.mean(self.radar.get_elevation(tilt))
        subdir='%.1f_%s'%(ele,subdir)
        if not self.breplace and os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
            print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='cross_correlation_ratio',tilt=tilt,colorfile=self.ref_colorfile)
        self.add_chinamap()
        self.draw_title(subdir)
        self.draw_colorbar(colorfile=self.zdr_colorfile,orientation='vertical')
        self.add_crs_line(ax=self.ax1)
        
        self.finish_save(subdir=subdir,)

    def draw_vel_alone(self,subdir='径向速度',tilt=0,format='png'):
        # check 
        if self.radar is None:
            return False
        ele=np.mean(self.radar.get_elevation(tilt))
        subdir='%.1f_%s'%(ele,subdir)
        
        if not self.breplace and os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.' + format):
            print(self.picpath + os.sep + subdir + self.timestr + '.' + format + ' already exists!')
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='velocity',tilt=tilt,colorfile=self.vel_colorfile)
        self.add_chinamap()
        self.draw_title(subdir)
        self.add_crs_line(ax=self.ax1)
        self.draw_colorbar(colorfile=self.vel_colorfile)
        self.finish_save(subdir=subdir,)

    def draw_vel_wind_barb(self,subdir='径向速度+地面自动站风场',tilt=1):
        # check 
        if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
            print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='velocity',tilt=tilt,colorfile=self.vel_colorfile)
        self.add_chinamap()
        self.draw_wind_barb()
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.vel_colorfile)
        self.finish_save(subdir=subdir)

    def draw_vel_wind_quiver(self,subdir='径向速度+地面流场',tilt=1):
        # check 
        if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
            print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
            return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='velocity',tilt=tilt,colorfile=self.vel_colorfile)
        self.add_chinamap()
        self.draw_wind_quiver()
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.vel_colorfile)
        self.finish_save(subdir=subdir)

    def draw_ref_pre(self,subdir='基本反射率+当前小时降水',tilt=0):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.draw_pre_1h()
        self.add_chinamap()
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)

    def draw_cref_pre(self,subdir='组合反射率+当前小时降水'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_cref()
        self.draw_pre_1h()
        self.add_chinamap()
        self.draw_title('组合反射率因子+当前小时降水量')
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)

    def draw_cref_wind_barb(self,subdir='组合反射率+地面自动站风场'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_cref()
        self.add_chinamap()
        self.draw_wind_barb()
        self.draw_title(subdir)
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)

    def draw_cref_wind_quiver(self,subdir='组合反射率+地面流场'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_cref()
        self.add_chinamap()
        self.draw_wind_quiver()
        self.draw_title(subdir)
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)

    def draw_cref_pre_wind_barb(self,subdir='组合反射率+地面自动站风场+当前小时降水'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False

        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_cref()
        self.draw_wind_barb()
        self.draw_pre_1h()
        self.add_chinamap()
        self.draw_title(subdir)
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)


    def draw_ref_wind_barb_vortex(self,subdir='基本反射率+地面自动站风场+涡度场(等值线)',tilt=0):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.draw_vortex(zorder=2)
        self.add_chinamap()
        self.draw_wind_barb(zorder=10)
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)
    
    def draw_ref_wind_divergence(self,subdir='基本反射率+地面自动站风场+散度(等值线)',tilt=0,type='quiver'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.draw_divergence(zorder=2)
        self.add_chinamap()
        if type == 'quiver':
            self.draw_wind_quiver(zorder=10)
        elif type == 'barb':
            self.draw_wind_barb(zorder=10)
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)

    def draw_ref_wind_barb_temperature(self,subdir='基本反射率+地面自动站风场+温度场(等值线)',tilt=0):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.draw_temperature(zorder=2)
        self.add_chinamap()
        self.draw_wind_barb(zorder=10)
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f°%s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)
    
    def draw_ref_wind_barb_dewpoint(self,subdir='基本反射率+地面自动站风场+露点场(等值线)',tilt=0):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.draw_drewpoint(zorder=2)
        self.add_chinamap()
        self.draw_wind_barb(zorder=10)
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)


    def draw_ref_pre_wind(self,subdir='基本反射率+地面自动站风场+当前小时降水',type='barb',tilt=0):
        # type == barb or quiver
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        if type == 'barb':
            self.draw_wind_barb()
        elif type == 'quiver':
            self.draw_wind_quiver()
        self.draw_pre_1h()
        self.add_chinamap()
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)

    def draw_ref_wind(self,subdir='基本反射率+地面自动站风场',type='barb',tilt=0):
        # type == barb or quiver
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.png'):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.png' + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        if type == 'barb':
            self.draw_wind_barb()
        elif type == 'quiver':
            self.draw_wind_quiver()
        self.add_chinamap()
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)


    # 叠加1小时变温
    def draw_ref_wind_delta_t(self,subdir='基本反射率+地面自动站风场+1小时变温(等值线)',tilt=0,type='quiver'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + '.' + self.pic_format):
        #     print(self.picpath + os.sep + subdir + self.timestr + '.' + self.pic_format + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        # self.draw_divergence(zorder=2)
        self.draw_delta_t(zorder=2)
        self.add_chinamap()
        if type == 'quiver':
            self.draw_wind_quiver(zorder=10)
        elif type == 'barb':
            self.draw_wind_barb(zorder=10)
        ele=np.mean(self.radar.get_elevation(tilt))
        self.draw_title('%s %.1f° %s'%(self.radarname,ele, subdir))
        self.draw_colorbar(colorfile=self.ref_colorfile)
        self.finish_save(subdir=subdir)
    
    # 叠加1小时变压
    def draw_ref_wind_delta_p(self,subdir='基本反射率+地面自动站风场+1小时变压(等值线)',tilt=0,type='quiver'):
        # check 
        # if os.path.exists(self.picpath + os.sep + subdir + os.sep +self.timestr + + '.' + self.pic_format):
        #     print(self.picpath + os.sep + subdir + self.timestr + + '.' + self.pic_format + ' already exists!')
        #     return False
        self.draw_basemap()
        self.draw_gisinfo()
        self.draw_ppi(fieldname='reflectivity',tilt=tilt,colorfile=self.ref_colorfile)
        self.draw_delta_p(zorder=2)
        
        
    def draw_delta_t(self,zorder=2):
        
        # 叠加1小时变温
        # filepath_minute='temp/backup_aws_minute'
        # filename_aws='surface_aws_20210720_1800.csv'
        if self.aws_min_delta_t_file_name is None:
            print('warning: aws_min_delta_t_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_delta_t_file_path + os.sep + self.aws_min_delta_t_file_name):
            print(self.aws_min_delta_t_file_path + os.sep + self.aws_min_delta_t_file_name + ' does not exists!')
            return False
        
        delta_t_data = xr.open_dataset(self.aws_min_delta_t_file_path + os.sep + self.aws_min_delta_t_file_name)
        

        levels = list(np.arange(delta_t_data.attrs['sta_minvalue'], delta_t_data.attrs['sta_maxvalue'], 0.2))

        cntr = self.ax1.contour(delta_t_data.lon.values,delta_t_data.lat.values,delta_t_data.t2m_delta_1hr.values.T,
                                levels=levels,colors=self.contour_color,zorder=zorder,linewidths=self.linewidth_cntr)
        
        for artist in cntr.get_children():
            if isinstance(artist, matplotlib.collections.PathCollection):
                artist.set_path_effects([patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
                clbls = self.ax1.clabel(artist,fmt="%2.1f",use_clabeltext=True,fontsize=self.fontsize_colorbar)

        plt.setp(cntr.labelTexts, path_effects=[
            patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
    
    def draw_delta_p(self,zorder=2):
        
        # 叠加1小时变压
        # filepath_minute='temp/backup_aws_minute'
        # filename_aws='surface_aws_20210720_1800.csv'
        if self.aws_min_delta_p_file_name is None:
            print('warning: aws_min_delta_p_file_name is None!')
            return False
        if not os.path.exists(self.aws_min_delta_p_file_path + os.sep + self.aws_min_delta_p_file_name):
            print(self.aws_min_delta_p_file_path + os.sep + self.aws_min_delta_p_file_name + ' does not exists!')
            return False
        
        delta_p_data = xr.open_dataset(self.aws_min_delta_p_file_path + os.sep + self.aws_min_delta_p_file_name)
        

        levels = list(np.arange(delta_p_data.attrs['sta_minvalue'], delta_p_data.attrs['sta_maxvalue'], 0.1))

        cntr = self.ax1.contour(delta_p_data.lon.values,delta_p_data.lat.values,delta_p_data.sprs2m_delta_1hr.values.T,
                                levels=levels,colors=self.contour_color,zorder=zorder,linewidths=self.linewidth_cntr,label='1小时变压')
        
        for artist in cntr.get_children():
            if isinstance(artist, matplotlib.collections.PathCollection):
                artist.set_path_effects([patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
                clbls = self.ax1.clabel(artist,fmt="%2.1f",use_clabeltext=True,fontsize=self.fontsize_colorbar,inline=True)

        plt.setp(cntr.labelTexts, path_effects=[
            patheffects.withStroke(linewidth=self.linewidth_cntr, foreground="w")])
        return cntr
    

    
        
        

if __name__ == "__main__":
    
    import pandas as pd
    import numpy as np  
    # 准备GIS文件
    gisfile = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/resources/stations/cma_city_station_info.dat'

    cities = pd.read_csv(gisfile,  delimiter=r"\s+") # cma_city_station_info

    cityname = np.unique(cities['city_name'])
    stanames=[]
    lat=[]
    lon=[]

    curdata = cities
    for name in cityname:
        flag2 =  curdata['city_name'] == name
        tmp_lat = np.mean(curdata[flag2]['lat'])
        tmp_lon = np.mean(curdata[flag2]['lon'])

        stanames.append(name)
        lon.append(tmp_lon)
        lat.append(tmp_lat)
        
        
    # 初始化绘图参数
    params=ini_params()

    # 修改补充完善参数
    params['radarfile_path'] = '/mnt/e/metradar_test/radar_aws/radar_fmt/2021/07/20/Z9371'
    params['radarfile_name'] = 'Z_RADR_I_Z9371_20210720083000_O_DOR_SAD_CAP_FMT.bin.bz2'
    params['rose_cr_path'] = '/mnt/e/metradar_test/radar_aws/pup/Z9371/CR/37'
    params['rose_cr_name'] = 'Z9371_20210720083000Z_CR_00_37'
    params['aws_hour_file_path'] = '/mnt/e/metradar_test/radar_aws/awsdata/aws_hourly_obs'
    params['aws_hour_file_name'] = 'surface_aws_hourly_20210720_0900.csv'
    params['aws_min_file_path'] = '/mnt/e/metradar_test/radar_aws/awsdata/aws_minutes_obs'
    params['aws_min_file_name'] = 'surface_aws_minute_20210720_0824_0830.csv'
    
    params['pic_path'] = '/mnt/e/metradar_test/radar_aws/pic'
    params['slat'] = 34
    params['nlat'] = 35
    params['wlon'] = 113
    params['elon'] = 114
    params['timestr'] = params['radarfile_name'].split('_')[4]
    params['fontfile'] = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/resources/fonts/YaHeiConsolasHybrid_1.12.ttf'
    params['gis_lats'] = lat
    params['gis_lons'] = lon
    params['gis_name'] = stanames
    params['breplace'] = True
    params['bdraw_crs'] = False
    params['ref_colorfile'] = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/resources/gr2_colors/default_BR_PUP2.pal'
    params['vel_colorfile'] = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/resources/gr2_colors/default_BV_PUP2.pal'
    params['figsize_width'] = 4
    params['fontsize_gis'] = 5
    params['fontsize_colorbar'] = 5
    params['fontsize_title'] = 6
    params['mapcolor'] = [0/255,0/255,0/255]
    params['dpi'] = 300
    params['pic_format'] = 'png'
    params['bdraw_title_ppi'] = False


    # 创建绘图函数
    _draw_radar_other = DRAW_RADAR_OTHER(params)

    # _draw_radar_other.read_vol_data()

    # _draw_radar_other.draw_ref_alone(subdir='ref',tilt=0,thred=-5)
    
    # _draw_radar_other.draw_vel_alone(subdir='vel',tilt=1)
    
    # _draw_radar_other.draw_ref_pre(subdir='ref_pre',)
    
    # _draw_radar_other.draw_ref_wind_barb_vortex(subdir='ref_pre_wind_vortex',)
    
    # _draw_radar_other.get_cref_from_radar(xlimits=[-80,50],ylimits=[-80,50])
    # _draw_radar_other.draw_cref_wind_barb(subdir='cref_wind_barb',)
    # _draw_radar_other.draw_cref_pre(subdir='cref_pre',)
    
    _draw_radar_other.get_cref_from_rose()
    # _draw_radar_other.draw_cref_wind_barb(subdir='cref_wind_barb',)
    _draw_radar_other.draw_cref_pre(subdir='cref_pre',)