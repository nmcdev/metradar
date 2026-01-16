# _*_ coding: utf-8 _*_

'''
自动站资料客观分析程序
朱文剑

'''

# %%
from metpy.interpolate import interpolate_to_grid
import nmc_met_io.retrieve_micaps_server as mserver
import pandas as pd
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
from collections import OrderedDict
from metpy.units import units
from metpy.calc import wind_components,vorticity,divergence,lat_lon_grid_deltas
import os
import gc
from datetime import datetime,timedelta
import matplotlib
matplotlib.use('Agg')


def remove_nan_observations(x, y, z):
    r"""Remove all x, y, and z where z is nan.

    Will not destroy original values.

    Parameters
    ----------
    x: array_like
        x coordinate
    y: array_like
        y coordinate
    z: array_like
        observation value

    Returns
    -------
    x, y, z
        List of coordinate observation pairs without
        nan valued observations.

    """
    x_ = x[~np.isnan(z)]
    y_ = y[~np.isnan(z)]
    z_ = z[~np.isnan(z)]

    return x_, y_, z_
    
def remove_repeat_coordinates(x, y, z):
    r"""Remove all x, y, and z where (x,y) is repeated and keep the first occurrence only.

    Will not destroy original values.

    Parameters
    ----------
    x: array_like
        x coordinate
    y: array_like
        y coordinate
    z: array_like
        observation value

    Returns
    -------
    x, y, z
        List of coordinate observation pairs without
        repeated coordinates.

    """
    coords = []
    variable = []

    for (x_, y_, t_) in zip(x, y, z):
        if (x_, y_) not in coords:
            coords.append((x_, y_))
            variable.append(t_)

    coords = np.array(coords)

    x_ = coords[:, 0]
    y_ = coords[:, 1]

    z_ = np.array(variable)

    return x_, y_, z_

def get_contour_verts(cn):
    contours = []
    # for each contour line
    for cc in cn.collections:
        paths = []
        # for each separate section of the contour line
        for pp in cc.get_paths():
            paths.append(pp.vertices)
        contours.append(paths)

    return contours

# 客观分析类
# 参考metpy等诊断分析库
class Object_Analyst:
    def __init__(self) -> None:
        self.debug_level = 0
        self.minlon = 100
        self.maxlon = 120
        self.minlat = 30
        self.maxlat = 45
        self.reso=0.025

    # 设置北京时还是世界时
    def set_time_type(self,ttype):
        self.time_type = ttype

    def set_reso(self,reso):
        self.reso = reso

    def set_debug(self,level):
        self.debug_level=level

    # 设置要分析的经纬度范围
    def set_boundary(self,minlon,maxlon,minlat,maxlat):
        self.minlon = minlon
        self.maxlon = maxlon
        self.minlat = minlat
        self.maxlat = maxlat
        pass
    
    # 设置当前时刻
    def set_time(self,tstr='20220511141000'):
        self.time_str=tstr

    # 设置X坐标
    def set_xcord(self,data_array):
        self.xcord = data_array
    

    # 设置Y坐标
    def set_ycord(self,data_array):
        self.ycord = data_array

    # 设置要客观分析的变量
    def set_vardata(self,data_array):
        self.vardata = data_array

    # 判断一个点是否在边界内
    def is_in_boundary(self,point_lon,point_lat):
        if point_lon < self.minlon \
                or point_lon > self.maxlon \
                    or point_lat < self.minlat \
                        or point_lat > self.maxlat:
                            return False
        else:
            return True

    # 针对从天擎下载的5分钟资料做客观分析，是从csv文件中读取的datafreame格式
    def do_oa_csv_df(self,newdata=None,vartype='温度'):

        if newdata is None:
            print('newdata is None!')
            return None
        
        
        if self.debug_level > 0:
            print(newdata.columns)
        
        validindex=[]
        for nn in range(newdata.shape[0]):
            if not self.is_in_boundary(newdata['Lon'][nn],newdata['Lat'][nn]):
                continue
            else:
                validindex.append(nn)
            

        if len(validindex) <=10:
            print('valid data less than 10, skip!')
            return None
        
        # print(validindex)        

        defaultvalue=999999
        # 注意，务必将缺测值设置为nan
        latlist = newdata['Lat'][validindex].values
        lonlist = newdata['Lon'][validindex].values
        templist = newdata['TEM'][validindex].values
        rhlist = newdata['RHU'][validindex].values
        dtlist = newdata['DPT'][validindex].values
        sprslist = newdata['PRS_Sea'][validindex].values

        self.set_boundary(np.min(lonlist),np.max(lonlist),np.min(latlist),np.max(latlist))
        if vartype.find('温度')>=0:
            flag = templist < defaultvalue  
            lonlist = lonlist[flag]
            latlist = latlist[flag]
            templist = templist[flag]

            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = templist
            params['out_varname'] = 't2m'
            params['out_long_name'] = 'surface temperature objective analyse'
            params['out_short_name'] = 'oa_t'
            params['out_units'] = 'degC'
             
            params['tipname'] = '温度'
            outdata = self.do_oa_base(params)

        elif vartype.find('露点')>=0:
            flag = dtlist < defaultvalue  
            lonlist = lonlist[flag]
            latlist = latlist[flag]
            dtlist = dtlist[flag]

            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = dtlist
            params['out_varname'] = 'td2m'
            params['out_long_name'] = 'surface dewpoint temperature objective analyse'
            params['out_short_name'] = 'oa_td'
            params['out_units'] = 'degC'
             
            params['tipname'] = '露点温度'
            outdata = self.do_oa_base(params)
            
        elif vartype.find('湿度')>=0:
            flag = rhlist < defaultvalue  
            lonlist = lonlist[flag]
            latlist = latlist[flag]
            rhlist = rhlist[flag]

            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = rhlist
            params['out_varname'] = 'rh2m'
            params['out_long_name'] = 'surface relative humidility objective analyse'
            params['out_short_name'] = 'oa_rh'
            params['out_units'] = '%'
             
            params['tipname'] = '相对湿度'
            outdata = self.do_oa_base(params)

        elif vartype.find('气压')>=0:
            flag = sprslist < defaultvalue  
            lonlist = lonlist[flag]
            latlist = latlist[flag]
            sprslist = sprslist[flag]

            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = sprslist
            params['out_varname'] = 'sprs2m'
            params['out_long_name'] = 'sea pressure objective analyse'
            params['out_short_name'] = 'oa_sprs'
            params['out_units'] = 'hPa'
             
            params['tipname'] = '海平面气压'
            outdata = self.do_oa_base(params)

        return outdata
    
    # 针对从天擎下载的5分钟资料做客观分析，是存储为csv格式的文件
    def do_oa_csv(self,filename,vartype='温度'):

        if not os.path.exists(filename):
            print(filename + ' not exists!')
            return None
        newdata = pd.read_csv(filename,encoding='gbk')
        
        if self.debug_level > 0:
            print(newdata.columns)
        
        validindex=[]
        for nn in range(newdata.shape[0]):
            if not self.is_in_boundary(newdata['Lon'][nn],newdata['Lat'][nn]):
                continue
            else:
                validindex.append(nn)
            

        if len(validindex) <=10:
            print('valid data less than 10, skip!')
            return None
        
        # print(validindex)        

        defaultvalue=999999
        tipname = ''
        # 注意，务必将缺测值设置为nan
        if vartype.find('温度')>=0:
            varname = 'TEM'
            tipname = '温度'
        elif vartype.find('湿度')>=0:
            varname = 'RHU'
            tipname = '相对湿度'
        elif vartype.find('气压')>=0:
            varname = 'PRS_Sea'
            tipname = '海平面气压'
        elif vartype.find('露点')>=0:
            varname = 'DPT'
            tipname = '露点温度'
        elif vartype.find('变温')>=0:
            for var in newdata.columns:
                if var.find('TEM_delta')>=0:
                    varname = var
                    tipname = '%s小时变温'%var.split('_')[-1][0]
                    break
        elif vartype.find('变压')>=0:           
            for var in newdata.columns:
                if var.find('PRS_Sea_delta')>=0:
                    varname = var
                    tipname = '%s小时变压'%var.split('_')[-1][0]
                    break


        latlist = newdata['Lat'][validindex].values
        lonlist = newdata['Lon'][validindex].values
        # templist = newdata['TEM'][validindex].values
        # rhlist = newdata['RHU'][validindex].values
        # dtlist = newdata['DPT'][validindex].values
        # sprslist = newdata['PRS_Sea'][validindex].values
        datalist = newdata[varname][validindex].values

        self.set_boundary(np.min(lonlist),np.max(lonlist),np.min(latlist),np.max(latlist))
        flag = datalist < defaultvalue  
        lonlist = lonlist[flag]
        latlist = latlist[flag]
        datalist = datalist[flag]
        params={}
        params['in_lon'] = lonlist
        params['in_lat'] = latlist
        params['in_data'] = datalist
        params['tipname'] = tipname
        if vartype.find('温度')>=0:
            params['out_varname'] = 't2m'
            params['out_long_name'] = 'surface temperature objective analyse'
            params['out_short_name'] = 'oa_t'
            params['out_units'] = 'degC'
            outdata = self.do_oa_base(params)
        elif vartype.find('露点')>=0:
            params['out_varname'] = 'td2m'
            params['out_long_name'] = 'surface dewpoint temperature objective analyse'
            params['out_short_name'] = 'oa_td'
            params['out_units'] = 'degC' 
            outdata = self.do_oa_base(params)
        elif vartype.find('湿度')>=0:
            params['out_varname'] = 'rh2m'
            params['out_long_name'] = 'surface relative humidility objective analyse'
            params['out_short_name'] = 'oa_rh'
            params['out_units'] = '%'
            outdata = self.do_oa_base(params)
        elif vartype.find('气压')>=0:
            params['out_varname'] = 'sprs2m'
            params['out_long_name'] = 'sea pressure objective analyse'
            params['out_short_name'] = 'oa_sprs'
            params['out_units'] = 'hPa'
            outdata = self.do_oa_base(params)
        elif vartype.find('变温')>=0:
            params['out_varname'] = 't2m_delta_' + varname.split('_')[-1]
            params['out_long_name'] = 'surface temperature delta objective analyse'
            params['out_short_name'] = 'oa_t_delta' + varname.split('_')[-1]
            params['out_units'] = 'degC'
            outdata = self.do_oa_base(params)
        elif vartype.find('变压')>=0:
            params['out_varname'] = 'sprs2m_delta_' + varname.split('_')[-1]
            params['out_long_name'] = 'sea pressure delta objective analyse'
            params['out_short_name'] = 'oa_sprs_delta' + varname.split('_')[-1]
            params['out_units'] = 'hPa'
            outdata = self.do_oa_base(params)
        return outdata

    # 进行客观分析的基础函数
    def do_oa_base(self,params):
    

        if not 'in_lon' in params.keys():
            print('key "in_lon" should be set')
            return False

        if not 'in_lat' in params.keys():
            print('key "in_lat" should be set')
            return False
        
        if not 'in_data' in params.keys():
            print('key "in_data" should be set')
            return False

        if not 'out_varname' in params.keys():
            print('key "out_varname" should be set')
            return False

        if not 'out_long_name' in params.keys():
            print('key "out_long_name" should be set')
            return False
        
        if not 'out_short_name' in params.keys():
            print('key "out_short_name" should be set')
            return False

        if not 'out_units' in params.keys():
            print('key "out_units" should be set')
            return False

        lonlist = params['in_lon']
        latlist = params['in_lat']
        data = params['in_data']
        x_masked, y_masked, data = remove_nan_observations(lonlist, latlist, data)
        x_masked, y_masked, data = remove_repeat_coordinates(x_masked, y_masked, data)

        # 4sigma
        def four_sigma(df):
            mean=df.data.mean()
            std=df.data.std()
            upper_limit=mean+4*std
            lower_limit=mean-4*std
            df['anomaly']=df.data.apply(lambda x: 1 if (x>upper_limit ) or (x<lower_limit) else 0)
            return df

        # 根据百分位进行异常值检查
        tmpdata = pd.DataFrame(data,columns=['data'])
        df1 = four_sigma(tmpdata)
        # df1[df1['anomaly']==1]

        x_masked = x_masked[df1['anomaly']==0]
        y_masked = y_masked[df1['anomaly']==0]
        data = data[df1['anomaly']==0]

        if self.debug_level > 0:
            print('read data over!')

        # self.set_reso(0.05)
        # 温度插值
        # hreso = 0.05

        gx, gy, gd = interpolate_to_grid(x_masked, y_masked, data,
                                                    interp_type='cressman',hres=self.reso,
                                                    minimum_neighbors=1,
                                                    )


        # 将客观分析结果在x和y方向调整坐标
        gd = gd.T
        gx = gx.T
        gy = gy.T

        # 构建xarray，并返回

        # define coordinates
        # time_coord = ('time', redic['time'][0])
        lon_coord = ('lon', gx[:,0], {
            'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
        lat_coord = ('lat', gy[0,:], {
            'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})


        # create xarray
        varattrs={'long_name': params['out_long_name'], 'short_name': params['out_short_name'], 'units': params['out_units']}
        
     
        result = xr.Dataset({params['out_varname']:(['lon', 'lat'], gd, varattrs),
                             },
                            coords={ 'lon':lon_coord, 'lat':lat_coord })

        # add attributes
        result.attrs['Conventions'] = "CF-1.6"
        result.attrs['time'] = self.time_str# np.datetime64(datetime.strptime(self.time_str,'%Y%m%d%H%M%S',))
        result.attrs['sta_maxvalue'] = np.max(data)
        result.attrs['sta_minvalue'] = np.min(data)
        if params['out_varname'] == 'sprs2m':
            result.attrs['sta_maxvalue'] = (np.max(data) + 2.5)//2.5*2.5
            result.attrs['sta_minvalue'] = (np.min(data) + 2.5)//2.5*2.5
        result.attrs['varname'] = params['out_varname']
        result.attrs['tipname'] = params['tipname']
        return result

    # 针对mdfs的5分钟资料做客观分析
    def do_oa_mdfs(self,filename,vartype='温度'):
        if not os.path.exists(filename):
            print(filename + ' not exists!')
            return None

        redic = mserver.get_stadata_from_mdfs(filename)
        if self.debug_level > 0:
            print(redic.columns)
        
        newdata = redic[['ID','lon','lat','温度','露点温度','相对湿度','平均风速_2分钟','平均风向_2分钟']]
        validindex=[]
        for nn in range(newdata.shape[0]):
            if not self.is_in_boundary(newdata['lon'][nn],newdata['lat'][nn]):
                continue
            else:
                validindex.append(nn)
        # print(validindex)        

        # 注意，务必将缺测值设置为nan
        latlist = newdata['lat'][validindex].values
        lonlist = newdata['lon'][validindex].values
        templist = newdata['温度'][validindex].values
        rhlist = newdata['相对湿度'][validindex].values
        dtlist = newdata['露点温度'][validindex].values
        windspd = newdata['平均风速_2分钟'][validindex].values
        winddir = newdata['平均风向_2分钟'][validindex].values

        if vartype.find('温度')>=0:
            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = templist
            params['out_varname'] = 't2m'
            params['out_long_name'] = 'surface temperature objective analyse'
            params['out_short_name'] = 'oa_t'
            params['out_units'] = 'degC'
             
            outdata = self.do_oa_base(params)
        elif vartype.find('露点')>=0:
            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = dtlist
            params['out_varname'] = 'td2m'
            params['out_long_name'] = 'surface dewpoint temperature objective analyse'
            params['out_short_name'] = 'oa_td'
            params['out_units'] = 'degC'
             
            outdata = self.do_oa_base(params)
        elif vartype.find('湿度')>=0:
            params={}
            params['in_lon'] = lonlist
            params['in_lat'] = latlist
            params['in_data'] = rhlist
            params['out_varname'] = 'rh2m'
            params['out_long_name'] = 'surface relative humidility objective analyse'
            params['out_short_name'] = 'oa_rh'
            params['out_units'] = '%'
             
            outdata = self.do_oa_base(params)

        # # wind
        # wind_speed = (windspd * units('m/s'))
        # wind_dir = winddir * units.degree
        # good_indices = np.where((~np.isnan(wind_dir)) & (~np.isnan(wind_speed)))

        # x_masked = lonlist[good_indices]
        # y_masked = latlist[good_indices]
        # wind_speed = wind_speed[good_indices]
        # wind_dir = wind_dir[good_indices]
        # u, v = wind_components(wind_speed, wind_dir)

        # # uwind
        # params['in_lon'] = x_masked
        # params['in_lat'] = y_masked
        # params['in_data'] = u.magnitude
        # params['out_varname'] = 'u10m'
        # params['out_long_name'] = '10m uwind objective analyse'
        # params['out_short_name'] = 'oa_u10m'
        # params['out_units'] = 'm/s'
        #  
        # u10m = self.do_oa_base(params)

        # # vwind
        # params['in_lon'] = x_masked
        # params['in_lat'] = y_masked
        # params['in_data'] = v.magnitude
        # params['out_varname'] = 'v10m'
        # params['out_long_name'] = '10m vwind objective analyse'
        # params['out_short_name'] = 'oa_v10m'
        # params['out_units'] = 'm/s'
        #  
        # v10m = self.do_oa_base(params)

        # digdata = self.calc_vor_div(wind_speed,wind_dir,lonlist,latlist)

        # 
        
        return outdata
    
    def calc_vor_div(self,wind_speed,wind_dir,lonlist,latlist):
        
        # 风场插值

        good_indices = np.where((~np.isnan(wind_dir)) & (~np.isnan(wind_speed)))

        x_masked = lonlist[good_indices]
        y_masked = latlist[good_indices]
        wind_speed = wind_speed[good_indices]
        wind_dir = wind_dir[good_indices]
        u, v = wind_components(wind_speed, wind_dir)
        windgridx, windgridy, uwind = interpolate_to_grid(x_masked, y_masked, np.array(u),interp_type='cressman', hres=self.reso)

        _, _, vwind = interpolate_to_grid(x_masked, y_masked, np.array(v), interp_type='cressman',hres=self.reso)
        if self.debug_level > 0:
            print('interpolate done!')

        # 计算涡度散度
        dx, dy = lat_lon_grid_deltas(longitude=windgridx, latitude=windgridy)
        vtx=vorticity(u=uwind*units.meter/units.seconds ,v=vwind*units.meter/units.seconds,dx=dx,dy=dy)
        div=divergence(u=uwind*units.meter/units.seconds ,v=vwind*units.meter/units.seconds,dx=dx,dy=dy)

        # 乘上1e5，方便显示
        vtx = vtx * 1e5
        div = div * 1e5
        # print(vtx.shape)
        # print(div.shape)
        if self.debug_level > 0:
            print('涡度和散度计算完毕！')

        # 构建xarray，并返回

        # define coordinates
        # time_coord = ('time', redic['time'][0])
        lon_coord = ('lon', windgridx[0,:], {
            'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
        lat_coord = ('lat', windgridy[:,0], {
            'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})


        # create xarray
        varattrs_vtx10m={'long_name': 'surface vortex ', 'short_name': 'oa_vtx_10m', 'units': '1e-5*1/s'}
        varattrs_div10m={'long_name': 'surface divergence ', 'short_name': 'oa_div_10m', 'units': '1e-5*1/s'}
     
        result = xr.Dataset({
                             'vtx_10m':(['lat', 'lon'], vtx.magnitude, varattrs_vtx10m),
                             'div_10m':(['lat', 'lon'], div.magnitude, varattrs_div10m),
                             },
                            coords={ 'lat':lat_coord, 'lon':lon_coord})

        # add attributes
        result.attrs['Conventions'] = "CF-1.6"

        # 对计算结果进行平滑处理
        vtx_smooth=result.vtx_10m.rolling(lon=5, lat=5, min_periods=1, center=True).mean()
        result.vtx_10m.data = vtx_smooth.values

        div_smooth=result.div_10m.rolling(lon=5, lat=5, min_periods=1, center=True).mean()
        result.div_10m.data = div_smooth.values
        
        return result


    

if __name__ == '__main__':

    filepath = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/testdata/aws/'
    # filename = 'PLOT_5MIN_20220511141000.000'
    filename = 'surface_aws_20230409_0818.csv'
    startlon = 106
    endlon = 135
    startlat = 18
    endlat = 28

    oa_class = Object_Analyst()
    oa_class.set_debug(1)
    oa_class.set_reso(0.025)
    oa_class.set_boundary(startlon,endlon,startlat,endlat)
    
    # oa_class.set_time('20220511141000')
    tstr = filename[12:20]+filename[21:25]
    oa_class.set_time(tstr)
    # result = oa_class.do_oa_mdfs(filepath + os.sep + filename,vartype='温度')
    result = oa_class.do_oa_csv(filepath + os.sep + filename,vartype='温度')
    
    print('done! ')


    if not result is None:
        # cflag = (abs(result.t2m)<20) & (abs(result.t2m)>3)
        result.t2m.plot.contourf(levels=15, add_colorbar=True)
        plt.title('t2m')
        plt.show()
  
        kk=0

    # if not result is None:
    #     cflag = (abs(result.div_10m)<20) & (abs(result.div_10m)>3)
    #     result.div_10m.where(cflag).plot.contour(levels=15, add_colorbar=True)
    #     plt.title('divergence')
    #     plt.show()
    #     kk=0


    # oa_class = Object_Analyst()
    # oa_class.set_reso(0.01)
    # params={}
    # params['in_lon'] = aws_lons
    # params['in_lat'] = aws_lats
    # params['in_data'] = aws_tem
    # params['out_varname'] = 't2m'
    # params['out_long_name'] = 'surface temperature objective analyse'
    # params['out_short_name'] = 'oa_t'
    # params['out_units'] = 'degC'
    
    # t2m = oa_class.do_oa_base(params)
    

    

    
    # %%
