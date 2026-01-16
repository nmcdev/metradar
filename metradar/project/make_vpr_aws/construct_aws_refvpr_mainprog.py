# _*_ coding: utf-8 _*_

'''
构建时间序列图的框架程序
'''

from nmc_met_io.retrieve_cmadaas import cmadaas_obs_by_time_range_and_id
from nmc_met_io.retrieve_cmadaas import cmadaas_sounding_by_time
from nmc_met_io.retrieve_cmadaas import cmadaas_radar_level2_by_timerange_and_id

import pandas as pd
import geo_transforms_pyart as geotrans
import math
import numpy as np
import os
import configparser
from make_mosaic_mp_archive import MAKE_RADAR_MOSAIC
from multiprocessing import freeze_support
from datetime import datetime,timedelta
import xarray as xr
import numpy.ma as MA
import matplotlib.pyplot as plt
from metradar.util.parse_pal import parse_pro
from matplotlib.ticker import (MultipleLocator)
from matplotlib import font_manager
import matplotlib
from metradar.util.comm_func import geopotential_to_height
matplotlib.use('agg')
# 一键生成VPR数据和自动站数据，并绘制图形
#数据来源：天擎

class CONSTRUCT_VPR:

    # sub function for reading config file
    def ConfigFetchError(Exception):
        pass

    def _get_config_from_rcfile(self,rcfile):
        """
        Get configure information from config_dk_met_io.ini file.
        """
        
        rc = rcfile
        print(rc)
        if not os.path.exists(rc):
            print(rc + ' not exist')
            return None
        try:
            config = configparser.ConfigParser()
            config.read(rc,encoding='UTF-8')
        except IOError as e:
            raise self.ConfigFetchError(str(e))
        except Exception as e:
            raise self.ConfigFetchError(str(e))

        return config
    
    def __init__(self,inifilepath) -> None:
        pass
        config = self._get_config_from_rcfile(inifilepath)
        self.outpath = config['SETTINGS']['root_outpath']
        self.outpath_fmt = self.outpath + os.sep + config['SETTINGS']['radar_basedata_path']
        self.task_name = config['SETTINGS']['task_name']
       
        self.start_time = config['SETTINGS']['starttime']
        self.end_time = config['SETTINGS']['endtime']
        self.staid = config['SETTINGS']['aws_site']
        self.mosaic_range = int(config['SETTINGS']['mosaic_range'])
        
        self.pic_format = config['SETTINGS']['pic_format']
        self.pic_dpi = int(config['SETTINGS']['pic_dpi'])
        self.colorfile = config['SETTINGS']['colorfile']
        self.time_range = "[%s,%s]"%(self.start_time, self.end_time)

        # self.tlogp_time = '20230731000000'
        # 自动计算探空时间，用起始日期08时的探空
        self.tlogp_time = self.start_time[0:8] + '000000'

        self.outpath_tlogp = self.outpath + os.sep + 'tlogp'
        self.outpath_sta = self.outpath + os.sep + 'VPR'
        self.outpath_pic = self.outpath + os.sep + 'VPR'
        self.outpath_ref = self.outpath + os.sep + 'VPR'
        self.outname_sta= '%s.csv'%self.staid
        self.outname_ref = 'p3_vpr_%s_pyart.nc'%self.staid
        self.outpath_mosaic = self.outpath + os.sep + 'mosaic' + os.sep + self.task_name 
        
        if not os.path.exists(self.outpath):
            os.makedirs(self.outpath)
        if not os.path.exists(self.outpath_fmt):
            os.makedirs(self.outpath_fmt)
        if not os.path.exists(self.outpath_tlogp):
            os.makedirs(self.outpath_tlogp)
        if not os.path.exists(self.outpath_sta):
            os.makedirs(self.outpath_sta)
        if not os.path.exists(self.outpath_pic):
            os.makedirs(self.outpath_pic)
        if not os.path.exists(self.outpath_ref):
            os.makedirs(self.outpath_ref) 
        if not os.path.exists(self.outpath_mosaic):
            os.makedirs(self.outpath_mosaic)   

        self.radarfile = config['SETTINGS']['radarfile']
        self.base_mosaic_inifile = config['SETTINGS']['base_mosaic_inifile']
        self.fontfile = config['SETTINGS']['fontfile']
        self.data_code_radar_api = config['SETTINGS']['data_code_radar_api']
        self.data_code_aws_api = config['SETTINGS']['data_code_aws_api']

    
    # 第一步，从天擎下载自动站数据
    def get_aws_data(self,):
        # 读取自动站雨量数据
        # A7606, A7607, A7055, A7617 重庆
        # # 54501 斋堂, A1067 丰台千灵山 A1254 大兴庞各庄
    
        
        elements='Station_Name,Station_Id_C,Station_Id_d,lat,lon,Datetime,PRE,PRE_1h'

        data_code =  self.data_code_aws_api

        # 读取数据
        data = cmadaas_obs_by_time_range_and_id(time_range=self.time_range, data_code = data_code,elements=elements,sta_ids = self.staid)
        if data is None:
            print('required data is None, please check the site number!')
            return False

        self.sta_lat = data['lat'][0]
        self.sta_lon = data['lon'][0]
        self.sta_name = data['Station_Name'][0]

        # data.to_csv(outpath_sta + os.sep + outname_sta,index=False)
        data2 = data.set_index('Datetime')
        # data2['PRE_1h'][0]=0


        diff_rain =[]
        diff_rain.append(0)


        # PRE_1h表示当前小时的累计雨量
        # pre_5min表示当前5分钟的累计雨量
        # pre_1min表示当前1分钟的累计雨量
        # accsum表示从起始时刻到当前时刻的累计雨量
        # 时间均为UTC时间

        # diff()为求差，mode()为获取众数
        # steps 为数据的时间间隔，单位为分钟
        steps = int(data['Datetime'].diff().mode()[0].seconds/60)
        difrain_name = 'pre_%dmin'%steps
        for nn in np.arange(0,data2.shape[0]-1):
            
            if data2['PRE_1h'][nn+1] >= data2['PRE_1h'][nn]:
                diff_rain.append(round(data2['PRE_1h'][nn+1]-data2['PRE_1h'][nn],2))
            else:
                diff_rain.append(round(data2['PRE_1h'][nn+1],2))
        newpd = pd.DataFrame(diff_rain,columns=[difrain_name],index=data2.index)
        # 如果间隔是1分钟，那么还需要求出5分钟的雨量
        if steps == 1:
            newpd['pre_5min'] = newpd[difrain_name].rolling(5).sum()
            newpd['pre_5min'] = newpd['pre_5min'].round(2)
        else:
            pass
        newindex = [tt for tt in newpd.index if tt.minute%5==0]
        newpd['accsum'] = newpd[difrain_name].cumsum()
        newpd['accsum'] = newpd['accsum'].round(2)
        newpd['PRE_1h'] = data2['PRE_1h']
        newpd['PRE'] = data2['PRE']
        newpd['lat'] = data2['lat']
        newpd['lon'] = data2['lon']
        newpd['staname']=data2['Station_Name']
        newpd.to_csv(self.outpath_sta + os.sep + self.outname_sta,index=True,encoding='gbk')
        return True

    def get_tlogp_data(self,):
        # 第二步，从离自动站最近的探空站中获取探空数据，提取零度层高度和-20度层高度
        # 读取探空站信息，获取经纬度、站号等信息
        # tlogpinfo = pd.read_csv('../common/stationinfo_new/china_tlogp_stations_m3.txt',delimiter=r"\s+",skiprows=3,names=['stanum','lon','lat','alt','num2'])
        
        # tmpdata['stanum'].values[0] int
        # staid_tlogp = str(tmpdata['stanum'].values[0])
        tlogpdata = cmadaas_sounding_by_time(times=self.tlogp_time)
        allstas = np.unique(tlogpdata['Station_Id_C'])
        alllons=[]
        alllats=[]
        #获取所有站点的经纬度信息
        for ts in allstas:
            tmpd = tlogpdata[tlogpdata['Station_Id_C']==ts]
            curlat = tmpd['Lat'].values[0]
            curlon = tmpd['Lon'].values[0]
            alllons.append(curlon)
            alllats.append(curlat)

        x,y= geotrans.geographic_to_cartesian_aeqd(lon=alllons,lat=alllats,lon_0=self.sta_lon,lat_0=self.sta_lat)
        dis = [math.sqrt(pow(x[k],2) + pow(y[k],2))/1000 for k in range(len(x))]
        flag = np.array(dis) <= min(dis)+0.1
        tmpts = allstas[flag]
        tmpdata = tlogpdata[tlogpdata['Station_Id_C']==tmpts[0]]
        tmpdata = tmpdata.sort_values(by='PRS_HWC',ascending=False)
        tmpdata.dropna(subset=['PRS_HWC'],inplace=True)
        tmpdata.to_csv(self.outpath_tlogp + os.sep + 'tlogp_%s.csv'%tmpts[0],index=False)
        #查找离零度层和-20度层最近的高度
        flag = np.array(abs(tmpdata['TEM'])) <= abs(tmpdata['TEM']).min()+0.01 
        z0 = tmpdata['GPH'][flag].values[0]
        flag = np.array(abs(tmpdata['TEM']+20)) <= abs(tmpdata['TEM']+20).min()+0.01
        z20 = tmpdata['GPH'][flag].values[0]

        self.z0 = geopotential_to_height(z0*9.80665)
        self.z20 = geopotential_to_height(z20*9.80665)
        return True
    
    def get_fmt_data(self,):
        # 第三步，从天擎读取雷达数据 
        # 先检查是否已经存在拼图文件，而且拼图文件中的时间和范围能够覆盖当前自动站
        # 如果已经能满足要求，则不再下载雷达数据
        startt = datetime.strptime(self.start_time,'%Y%m%d%H%M%S')
        endt   = datetime.strptime(self.end_time,'%Y%m%d%H%M%S')
        curt = startt
        allref=[]
        grd_height = None
        need_file_times=[]
        while curt <= endt:
            
            curname = 'mosaic_' + curt.strftime('%Y%m%d%H%M') + '.nc'
            if os.path.exists(self.outpath_mosaic + os.sep + curname):
                # print('file exist: %s'%(outpath_mosaic + os.sep + curname))
                
                ref = xr.open_dataset(self.outpath_mosaic + os.sep + curname)

                cent_lat = ref.origin_latitude.values[0]
                cent_lon = ref.origin_longitude.values[0]
                x,y= geotrans.geographic_to_cartesian_aeqd(lon=self.sta_lon,lat=self.sta_lat,lon_0=cent_lon,lat_0=cent_lat)
                grd_height = ref['z'].values
                reso_x = ref['x'][1]-ref['x'][0]
                reso_y = ref['y'][1]-ref['y'][0]
                xx = x[0]/reso_x
                yy = y[0]/reso_y
                xx = int(xx.round())
                yy = int(yy.round())
                radius = int((len(ref.x)-1)/2)

                if xx+radius >=0 and xx+radius < len(ref.x) and yy+radius >=0 and yy+radius < len(ref.y):
                    pass
                else:
                    # 表示当前拼图范围不能覆盖自动站，需要重新下载数据进行拼图
                    need_file_times.append(curt)
                ref.close()
            else:
                need_file_times.append(curt)
            curt += timedelta(minutes=6)
            
        

        # 如果不能满足要求，则下载雷达数据

        # 根据自动站经纬度，自动判断应该下载哪些雷达站的数据
        # 读取雷达站点信息，获取经纬度、站号等信息
        valid_range = 200
        radinfo = pd.read_csv(self.radarfile,sep='|',names=['radar_id','radar_name','gr2_name','radar_lat','radar_lon','radar_alt'],index_col=0)

        x,y = geotrans.geographic_to_cartesian_aeqd(radinfo['radar_lon'],radinfo['radar_lat'],self.sta_lon,self.sta_lat, R=6370997.0)
        # dis = math.sqrt(pow(x[0],2) + pow(y[0],2))
        dis = [math.sqrt(pow(x[k],2) + pow(y[k],2))/1000 for k in range(len(x))]
        flag = np.array(dis) <= valid_range
        # flag = np.where(dis<=valid_range)[0]
        tmpdata = radinfo.loc[flag]
        self.rdinfo = tmpdata.loc[:,['radar_name','radar_lat','radar_lon']]
    

        for site in self.rdinfo.index:
            print(site)
            # 从天擎下载该站号的雷达资料
            if len(need_file_times) == 0:
                pass
            else:
                tmp_starttime = min(need_file_times).strftime('%Y%m%d%H%M%S')
                tmp_endtime = max(need_file_times).strftime('%Y%m%d%H%M%S')
                time_range = '[ ' + tmp_starttime + ',' + tmp_endtime +  ']'
                cmadaas_radar_level2_by_timerange_and_id(radar_ids=site,time_range=time_range,outpath=self.outpath_fmt)
        
        return True

        
        
    # 第四步，制作区域三维拼图
    def make_mosaic(self,):
        # 修改配置文件
        
        config = configparser.ConfigParser()
        config.read(self.base_mosaic_inifile)
        config.set('ARCHIVE', 'input_path_archive', self.outpath_fmt) # 
        config.set('ARCHIVE', 'output_path_archive', self.outpath_mosaic) # 
        config.set('ARCHIVE', 'starttime', self.start_time[0:12]) # 
        config.set('ARCHIVE', 'endtime', self.end_time[0:12]) # 
        config.set('COMMON_SETTING', 'center_lon', '%.3f'%self.sta_lon) # 
        config.set('COMMON_SETTING', 'center_lat', '%.3f'%self.sta_lat) # 
        config.set('COMMON_SETTING', 'mosaic_range', '50') # 
        config.set('COMMON_SETTING', 'radar_sitesfile', self.radarfile) #


        str_site=''
        for site in self.rdinfo.index:
            if site.startswith('ZA') or site.startswith('ZB'): # 暂时去掉X波段的数据
                continue
            str_site += site+','
        config.set('COMMON_SETTING', 'radars', str_site) # 
        config.write(open(self.base_mosaic_inifile, 'w')) # 将修改后的配置写入文件

        _make_mosaic = MAKE_RADAR_MOSAIC(self.base_mosaic_inifile)

        if not _make_mosaic.berror:
            if _make_mosaic.run_mode == 'archive':
                _make_mosaic.do_archive()
            else:
                print('run mode is not archive!')
        return True
    

    # 第五步：从三维拼图数据中获取格点的时间序列
    def make_vpr(self,):
        
        data = pd.read_csv(self.outpath_sta+os.sep+self.outname_sta,index_col=0,encoding='gbk')
        sta_lat = data['lat'][0]
        sta_lon = data['lon'][0]
        sta_name = data['staname'][0]
        startt = datetime.strptime(self.start_time,'%Y%m%d%H%M%S')
        endt   = datetime.strptime(self.end_time,'%Y%m%d%H%M%S')
        curt = startt
        allref=[]
        grd_height = None
        file_times=[]
        while curt <= endt:
            
            curname = 'mosaic_' + curt.strftime('%Y%m%d%H%M') + '.nc'
            if not os.path.exists(self.outpath_mosaic + os.sep + curname):
                print('file not exist: %s'%(self.outpath_mosaic + os.sep + curname))
                curt += timedelta(minutes=6)
                continue
                
            ref = xr.open_dataset(self.outpath_mosaic + os.sep + curname)

            cent_lat = ref.origin_latitude.values[0]
            cent_lon = ref.origin_longitude.values[0]
            x,y= geotrans.geographic_to_cartesian_aeqd(lon=sta_lon,lat=sta_lat,lon_0=cent_lon,lat_0=cent_lat)
            grd_height = ref['z'].values
            reso_x = ref['x'][1]-ref['x'][0]
            reso_y = ref['y'][1]-ref['y'][0]
            xx = x[0]/reso_x
            yy = y[0]/reso_y
            xx = int(xx.round())
            yy = int(yy.round())
            radius = int((len(ref.x)-1)/2)
            ref2 = ref.isel(x=xx+radius,y=yy+radius)['reflectivity'].values
            # 对ref2进行线性插值
            tmpdf = pd.DataFrame(ref2[0],columns=['ref_raw'])
            tmpdf['ref_new'] = tmpdf['ref_raw'].interpolate(method='slinear')
            ref2 = tmpdf['ref_new'].values
            allref.append(ref2)
            file_times.append(curt)
            curt += timedelta(minutes=6)
            ref.close()
        allref = np.array(allref)
        # allref = allref.reshape(allref.shape[0],allref.shape[2])

        alldata = MA.masked_array(allref, mask=allref==-9999)
        xrdata = xr.Dataset({
                    'dbz':(['z', 'time'], alldata.T, {'long name':'time-height dBZ'})},
                    coords={'z':grd_height, 'time':file_times},
                    attrs={'lat':sta_lat, 'lon':sta_lon})

        xrdata.to_netcdf(self.outpath_ref + os.sep + self.outname_ref)
        print('ref data success!')
        return True
    
    # 第六步，绘制图形
    def draw_pic(self,):
        oridf = pd.read_csv(self.outpath_sta+os.sep+self.outname_sta,encoding='gbk')#,index_col=0

        oridf['Datetime'] = pd.to_datetime(oridf['Datetime'], format="%Y-%m-%d %H:%M:%S")
        oridf = oridf.set_index('Datetime')
        newindex = [tt for tt in oridf.index if tt.minute%5==0]
        df = oridf.loc[newindex]
        # df.set_index('staname',inplace=True)
        df.reset_index(inplace=True)

        #将缺失的时间进行插值处理
        helper = pd.DataFrame({'Datetime': pd.date_range(start=df['Datetime'].min(), end=df['Datetime'].max(),freq='300s')})
        newdf = pd.merge(df, helper, on='Datetime', how='outer').sort_values('Datetime')
        newdf['accsum'] = newdf['accsum'].interpolate(method='linear')

        # ref_colorfile='gr2_colors/default_BR_PUP1.pal'
        # ref_colorfile = 'gr2_colors/BR_WDTB_Bright.pal'
        outdic= parse_pro(self.colorfile)
        cmap=outdic['cmap']
        norm=outdic['norm']
        units=outdic['units'] 
        # from pyart.graph import common
        # cmapname = 'pyart_NWSRef'
        # cmap = common.parse_cmap(cmapname, 'reflectivity')

        data = xr.open_dataset(self.outpath_ref + os.sep + self.outname_ref)

        data = data.rolling(z=2, time=2, min_periods=1, center=True).mean()

        font_path = self.fontfile 
        font_manager.fontManager.addfont(font_path)
        prop = font_manager.FontProperties(fname=font_path)

        plt.rcParams["font.size"] = 12
        plt.rcParams["font.sans-serif"] = prop.get_name()
        fig = plt.figure(figsize=(10,6))
        ax1 = fig.add_axes([0.1,0.18,0.8,0.3])#位置[左,下,右,上]
        ax3 = fig.add_axes([0.1,0.64,0.8,0.3])#位置[左,下,右,上]
        ax2 = ax3.twinx()
        ax1.minorticks_on()


        cb_ax = fig.add_axes([0.25, 0.08, 0.5, 0.03])

        grid_y,grid_x=np.meshgrid(np.arange(0,data.dbz.shape[1]),data.z.values,)
        pcm = ax1.pcolormesh(grid_y,grid_x,data.dbz,cmap=cmap,norm=norm)
        # pcm = ax1.pcolormesh(data.dbz,cmap=cmap,norm=norm)


        # flag = np.array(abs(data.z.values - z0)) <= abs(data.z.values - z0).min()+0.01 
        # z0h = ax1.plot(np.arange(0,data.dbz.shape[1]),np.ones(data.dbz.shape[1])*int(np.where(flag)[0][0]),'y--',
        #                linewidth=2,label='0度层高度')
        # flag = np.array(abs(data.z.values - z20)) <= abs(data.z.values - z20).min()+0.01 
        # z20h=ax1.plot(np.arange(0,data.dbz.shape[1]),np.ones(data.dbz.shape[1])*int(np.where(flag)[0][0]),'r--',
        #               linewidth=2,label='-20度层高度')
        z0h = ax1.plot(np.arange(0,data.dbz.shape[1]),np.ones(data.dbz.shape[1])*self.z0,'k--',linewidth=2,label='0度层高度')
        z20h=ax1.plot(np.arange(0,data.dbz.shape[1]),np.ones(data.dbz.shape[1])*self.z20,'r--',linewidth=2,label='-20度层高度')
        
        # leg = ax1.legend([z0h], ["零度层高度",], loc='upper right',title_fontproperties=prop)
        ax1.legend()
        ax1.tick_params(axis='x',which='minor',length=3,width=0.8,)
        ax1.tick_params(axis='x',which='major',length=5,width=1,)
        cb = plt.colorbar(mappable=pcm,cax=cb_ax,extend='both',ticks=np.arange(-10,85,5),orientation='horizontal')
        cb.set_label('反射率因子 dBZ',fontsize=12)
        ah1=ax2.plot(newdf['accsum'].values,'r',linewidth=2,label='累计雨量')
        ah2=ax3.bar(np.arange(0,newdf.shape[0]),newdf['pre_5min'],label='5分钟雨量')
        ax2.legend(loc='upper right')
        ax3.legend(loc='upper left')
        ax3.minorticks_on()
        ax3.tick_params(axis='x',which='minor',length=3,width=0.8,)
        ax3.tick_params(axis='x',which='major',length=5,width=1,)
        ax3.xaxis.set_minor_locator(MultipleLocator(1))
        ax1.set_xlim([0,data.dbz.shape[1]+1])
        ax2.set_xlim([0,newdf.shape[0]])
        ax1.grid('y')
        ax2.grid('y')
        # plt.xticks(fontsize=12)
        # plt.yticks(fontsize=12)
        # ax1.set_xlabel('Time(BJT, day hour:min)')
        ax1.set_ylabel('距离地面的高度(米)')
        ax1.set_title('单点回波强度时间-高度图（%.2f°N,%.2f°E）'%(data.lat,data.lon),fontsize=12)
        ax2.set_ylabel('累计雨量(毫米)',color='r')
        ax3.set_ylabel('5分钟雨量(毫米)',color=[59/255,117/255,175/255])

        tmpdate = pd.to_datetime(data.time)+ timedelta(hours=8)
        dates1 = [date.strftime('%H:%M') for date in tmpdate]
        dates1[0] = tmpdate.min().strftime('%H:%M \n%Y-%m-%d')

        datestr = dates1[::5]
        tmpd = tmpdate[::5]
        datestr[-1] = tmpd.max().strftime('%H:%M \n%Y-%m-%d')

        ax1.set_xticks(np.arange(0,data.time.shape[0],5))
        ax1.set_xticklabels(datestr)
        # ax1.set_yticks(np.arange(0,data.z.shape[0],5))
        # ax1.set_yticklabels(data.z[::5].values.astype('int'))
        # ax1.set_yticks(data.z[::5])
        # ax1.set_ylim([data.z.min(),data.z.max()])
        ax1.set_ylim([0,20000])
        ax1.set_yticks(np.arange(0,20000,2500))

        tmpdate = pd.to_datetime(newdf['Datetime'])+ timedelta(hours=8)
        dates2 = [date.strftime('%H:%M') for date in tmpdate]
        dates2[0] = tmpdate.min().strftime('%H:%M \n%Y-%m-%d')
        dates2[-1] = tmpdate.max().strftime('%H:%M \n%Y-%m-%d')
        ax2.set_xticks(np.arange(0,newdf.shape[0],6))

        ax2.set_xticklabels(dates2[::6])
        ax3.set_xlabel('时间(时:分，北京时)')
        ax2.set_title('5分钟雨量（柱）、自动站累积雨量（红实线）, 站号：%s,%s(%.2f°N,%.2f°E)'%(self.staid,self.sta_name,self.sta_lat,self.sta_lon),fontsize=12)
        # plt.figtext(0.73,0.02,'国家气象中心-天气预报技术研发室制作',fontsize=10)
        plt.savefig(self.outpath_pic + os.sep + self.outname_ref.replace('.nc','.%s'%self.pic_format),dpi=self.pic_dpi)#,dpi=600
        print(self.outpath_pic + os.sep + self.outname_ref.replace('.nc','.%s'%self.pic_format) + ' saved!')
        # plt.show()
        plt.close()
        return True
    
    # 一键生成VPR数据和自动站数据，并绘制图形
    def do_all(self,):

        if not self.get_aws_data():
            return False
        if not self.get_tlogp_data():
            return False
        if not self.get_fmt_data():
            return False
        if not self.make_mosaic():
            return False
        if not self.make_vpr():
            return False
        if not self.draw_pic():
            return False

if __name__ == "__main__":


    freeze_support()

    # sitelist=['G1174','G3522']
    # for st in sitelist:
    #     _construct_vpr = CONSTRUCT_VPR(inifilepath='construct_aws_refvpr_mainprog_%s.ini'%st)
    #     _construct_vpr.do_all()

    # 根据实际情况修改配置文件路径
    _construct_vpr = CONSTRUCT_VPR(inifilepath='/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/metradar/project/make_vpr_aws/construct_aws_refvpr_mainprog.ini')
    _construct_vpr.do_all()

    
    
    

    
    
