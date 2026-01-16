#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
多普勒雷达三维风场反演脚本
朱文剑

'''
# %%

import pyart
import pydda
import os
import matplotlib.pyplot as plt
import matplotlib
from metradar.util.get_tlogp_from_sharppy import get_profile
import numpy as np
import configparser
from datetime import datetime
import math
import argparse
import shutil
import sys
from metradar.io.decode_fmt_pyart import read_cnrad_fmt
import xarray as xr
# import warnings
# warnings.filterwarnings("ignore")


class GET_WIND_3D:
    # sub function for reading config file
    def ConfigFetchError(Exception):
        pass

    def _get_config_from_rcfile(self,rcfile):
        """
        Get configure information from config_dk_met_io.ini file.
        """
        # print(os.getcwd())
        rc = rcfile
        print(rc)
        try:
            config = configparser.ConfigParser()
            config.read(rc,encoding='UTF-8')
        except IOError as e:
            raise self.ConfigFetchError(str(e))
        except Exception as e:
            raise self.ConfigFetchError(str(e))

        return config

    def myprint(self,message):
        f = open('process_info.txt','at+',encoding='utf-8')
        f.write(message)
        f.write('\n')
        f.close()
        print(message)

    def write_to_progress_cross(self,value):
        f = open(self.picpath + os.sep + 'progress_cross.txt','wt')
        f.write(str(value))
        f.close()
        print('current progress = %d'%value + "%")

    def write_to_progress_main(self,value):
        f = open('progress_main.txt','wt')
        f.write(str(value))
        f.close()
        print('current progress = %d'%value + "%")

    def rad(self,d):
        return d * np.pi / 180.0

    def getDistance(self,lat1, lng1, lat2, lng2):
        # 计算两点间的距离，单位为千米
        EARTH_REDIUS = 6378.137
        radLat1 = self.rad(lat1)
        radLat2 = self.rad(lat2)
        a = radLat1 - radLat2
        b = self.rad(lng1) - self.rad(lng2)
        s = 2 * math.asin(math.sqrt(math.pow(math.sin(a/2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b/2), 2)))
        s = s * EARTH_REDIUS
        return s

    def write_to_gisinfo(self,):
        try:
            fout = open(self.picpath + os.sep + 'gisinfo.txt','wt',encoding='utf-8')
            fout.write('minLat=%.3f\n'%self.lat_min[0])
            fout.write('maxLat=%.3f\n'%self.lat_max[0])
            fout.write('minLon=%.3f\n'%self.lon_min[0])
            fout.write('maxLon=%.3f\n'%self.lon_max[0])
            self.myprint('minLat=%.3f'%self.lat_min[0])
            self.myprint('maxLat=%.3f'%self.lat_max[0])
            self.myprint('minLon=%.3f'%self.lon_min[0])
            self.myprint('maxLon=%.3f'%self.lon_max[0])
            fout.close()
            return True
        except:
            self.myprint(self.picpath + os.sep + 'gisinfo.txt' + ' open error!')
            return False

    def __init__(self,inifile='config.ini'):
        pass
        self.error=False
        if os.path.exists('process_info.txt'):
            os.remove('process_info.txt')

        if not os.path.exists(inifile):
            self.myprint(inifile + ' 不存在，请检查')
        config = self._get_config_from_rcfile(inifile)
        self.xlims = np.array(config['PARAM_SETTINGS']['XLIM'].split(', ')).astype(np.int64)
        self.ylims = np.array(config['PARAM_SETTINGS']['YLIM'].split(', ')).astype(np.int64)
        self.zlims = np.array(config['PARAM_SETTINGS']['ZLIM'].split(', ')).astype(int)
        self.time_files = config['PARAM_SETTINGS']['TIME_FILES']
        if len(self.time_files) !=14:
            self.myprint('时间参数设置错误，请重新设置 TIME_FILES 参数')
            self.error=True
            return None

        self.standard_lat = float(config['PARAM_SETTINGS']['CENTER_LAT'])
        self.standard_lon = float(config['PARAM_SETTINGS']['CENTER_LON'])

        self.ana_lat = float(config['PARAM_SETTINGS']['ANA_LAT'])
        self.ana_lon = float(config['PARAM_SETTINGS']['ANA_LON'])
        # bshowpic = int(config['PARAM_SETTINGS']['SHOW_PIC'])
        self.bsavepic = int(config['PARAM_SETTINGS']['SAVE_PIC'])
        if self.bsavepic == False:
            self.myprint('参数设置中设定不保存图片！')

        self.bshowpic = False
        if not self.bshowpic:
            matplotlib.use('Agg')
        else:
            matplotlib.use('MacOSX')

        self.radars = config['PARAM_SETTINGS']['RADAR_SITES'].split(', ')
        self.radarfilepath = config['PATH_SETTINGS']['RADAR_FILE_PATH']
        self.outpath = config['PATH_SETTINGS']['OUT_PATH']
        self.picpath = config['PATH_SETTINGS']['PIC_PATH']
        self.tlogp_filepath = config['PATH_SETTINGS']['TLOG_PATH']
        self.tlogp_filename = config['PATH_SETTINGS']['TLOG_FILE']

        if not os.path.exists(self.outpath):
            os.makedirs(self.outpath)

        if not os.path.exists(self.picpath):
            os.makedirs(self.picpath)

        self.xreso = int(config['PARAM_SETTINGS']['XRESO'])
        self.yreso = int(config['PARAM_SETTINGS']['YRESO'])
        self.zreso = int(config['PARAM_SETTINGS']['ZRESO'])

        self.lon_min,self.lat_min=pyart.core.cartesian_to_geographic_aeqd(self.xlims[0]*1000,self.ylims[0]*1000,self.standard_lon,self.standard_lat)
        self.lon_max,self.lat_max=pyart.core.cartesian_to_geographic_aeqd(self.xlims[1]*1000,self.ylims[1]*1000,self.standard_lon,self.standard_lat)

        if not self.write_to_gisinfo():
            self.write_to_progress_main(100)
            self.error=True
            self.myprint('write_to_gisinfo error!')
            return None

        self.g_xlim = (self.xlims[0]*1000,self.xlims[1]*1000)
        self.g_ylim = (self.ylims[0]*1000,self.ylims[1]*1000)
        self.g_zlim = (self.zlims[0]*1000,self.zlims[1]*1000)
        self.g_numx = int(len(range(self.xlims[0],self.xlims[1]))*1000/self.xreso+1) # x方向格点数
        self.g_numy = int(len(range(self.ylims[0],self.ylims[1]))*1000/self.yreso+1) # y方向格点数
        self.g_numz = int(len(range(self.zlims[0],self.zlims[1]))*1000/self.zreso+1) # z方向格点数

    def draw_all_pic(self,bdrawhori=True,):
        resultnames = []
        files = os.listdir(self.outpath)
        valid_files = []

        self.write_to_progress_cross(0)
        for ff in files:
            if ff.find('.nc')<=0:
                continue
            else:
                valid_files.append(ff)

        if len(valid_files) == 0:
            self.myprint('未找到nc格式的三维风场数据文件，请先调用--getwind方法')
            return False

        for ff in valid_files:
            resultnames.append(self.outpath + os.sep + ff)
            self.myprint(ff)
        
 
        s_x = self.getDistance(self.ana_lat,self.ana_lon,self.ana_lat,self.standard_lon)
        s_y = self.getDistance(self.ana_lat,self.ana_lon,self.standard_lat,self.ana_lon)
        self.myprint('拼图中心经度为：%.3f'%self.standard_lon)
        self.myprint('拼图中心纬度为：%.3f'%self.standard_lat)
        self.myprint('垂直剖面经度为：%.3f'%self.ana_lon)
        self.myprint('垂直剖面纬度为：%.3f'%self.ana_lat)
        self.myprint('X方向偏移量为：%d'%s_x)
        self.myprint('Y方向偏移量为：%d'%s_y)

        lev_yz = int((self.xlims[0] - s_x)/(self.xreso/1e3))
        lev_xz = int((s_y- self.ylims[0])/(self.yreso/1e3))
        self.myprint('X方向层次为：%d'%lev_xz)
        self.myprint('Y方向层次为：%d'%lev_yz)


        Grids = [xr.open_dataset(resultname) for resultname in resultnames]

        x=Grids[0].x.values
        y=Grids[0].y.values
        z=Grids[0].z.values

        uwind=Grids[0]['u'].values
        vwind=Grids[0]['v'].values
        wwind=Grids[0]['w'].values


        picpath_horiz_bar = self.picpath + os.sep + '水平风场'
        picpath_horiz_stream = self.picpath + os.sep + '水平流场'
        picpath_xz_bar = self.picpath + os.sep + 'XZ方向垂直风场'
        picpath_xz_stream = self.picpath + os.sep + 'XZ方向垂直流场'
        picpath_yz_bar = self.picpath + os.sep + 'YZ方向垂直风场'
        picpath_yz_stream = self.picpath + os.sep + 'YZ方向垂直流场'

        picpath_vor_stretch = self.picpath + os.sep + '拉伸涡度'
        picpath_vor_tilt = self.picpath + os.sep + '倾斜涡度'
 


        if not os.path.exists(picpath_horiz_bar):
            os.makedirs(picpath_horiz_bar)
        if not os.path.exists(picpath_horiz_stream):
            os.makedirs(picpath_horiz_stream)
        if not os.path.exists(picpath_xz_bar):
            os.makedirs(picpath_xz_bar)
        if not os.path.exists(picpath_xz_stream):
            os.makedirs(picpath_xz_stream)
        if not os.path.exists(picpath_yz_bar):
            os.makedirs(picpath_yz_bar)
        if not os.path.exists(picpath_yz_stream):
            os.makedirs(picpath_yz_stream)
        if not os.path.exists(picpath_vor_stretch):
            os.makedirs(picpath_vor_stretch)
        if not os.path.exists(picpath_vor_tilt):
            os.makedirs(picpath_vor_tilt)

        if bdrawhori:
            # draw barbs
            for ll in range(10): # self.g_numz
                fig, ax = plt.subplots(figsize=(8, 6))
                pydda.vis.plot_horiz_xsection_barbs(Grids, ax, 'reflectivity', level=ll,
                                                    # w_vel_contours=[3, 5,],
                                                    cmap='NWSRef', vmin=0, vmax=70,
                                                    barb_spacing_x_km=5,
                                                    barb_spacing_y_km=5,
                                                    show_lobes=False)
                ax.set_xlim(self.xlims.tolist())
                ax.set_ylim(self.ylims.tolist())
                ax.set_title(ax.get_title().replace('PyDDA','MultiRadar'))
                picfilename = 'horiz_xsection_barbs_%02d_%s.png'%(ll+1,self.time_files)
                if self.bsavepic:
                    fig.savefig(picpath_horiz_bar + os.sep + picfilename, 
                                format='png', transparent=False, dpi=300, pad_inches = 0)
                    self.myprint(picpath_horiz_bar + os.sep + picfilename + ' saved!')
                plt.close(fig)

            # draw streamline
            for ll in range(10): # self.g_numz
   
                fig, ax = plt.subplots(figsize=(8, 6))
                pydda.vis.plot_horiz_xsection_streamlines(Grids, ax, 'reflectivity', level=ll,
                                                        cmap='NWSRef', vmin=0, vmax=70,
                                                        #   w_vel_contours=[3, 5,],
                                                        show_lobes=False,
                                                        )
                ax.set_xlim(self.xlims.tolist())
                ax.set_ylim(self.ylims.tolist())
                ax.set_title(ax.get_title().replace('PyDDA','MultiRadar'))
                picfilename = 'horiz_xsection_streamlines_%02d_%s.png'%(ll+1,self.time_files)
                if self.bsavepic:
                    fig.savefig(picpath_horiz_stream + os.sep + picfilename, 
                                format='png', transparent=False, dpi=300, pad_inches = 0)
                    self.myprint(picpath_horiz_stream + os.sep + picfilename + ' saved!')

                plt.close(fig)
                
        try:
            #=================
            fig, ax = plt.subplots(figsize=(8, 4))
            pydda.vis.plot_xz_xsection_barbs(Grids, ax, 'reflectivity', level=lev_xz,
                                            #  w_vel_contours=[3, 5,],
                                            cmap='NWSRef', vmin=0, vmax=70,
                                            barb_spacing_x_km=2.0,
                                            barb_spacing_z_km=1.0)
            ax.set_xlim(self.xlims.tolist())
            ax.set_title(ax.get_title().replace('PyDDA','MultiRadar'))
            picfilename = 'xz_xsection_barbs_%06d_%06d_%s.png'%(self.ana_lat*1000,self.ana_lon*1000,self.time_files)
            
            if self.bsavepic:
                fig.savefig(picpath_xz_bar + os.sep + picfilename, 
                            format='png', transparent=False, dpi=300, pad_inches = 0)
                self.myprint(picpath_xz_bar + os.sep + picfilename + ' saved!')
            plt.close(fig)
            self.write_to_progress_cross(25)
        except:
            self.myprint('可能是垂直剖面的经纬度超出范围了，请重新设置再尝试！')
        #=================
        try:
            fig, ax = plt.subplots(figsize=(8, 4))
            pydda.vis.plot_xz_xsection_streamlines(Grids, None, 'reflectivity', level=lev_xz,
                                                cmap='NWSRef', vmin=0, vmax=70,
                                                #  w_vel_contours=[3, 5,],
                                                )
            ax.set_xlim(self.xlims.tolist())
            ax.set_title(ax.get_title().replace('PyDDA','MultiRadar'))
            picfilename = 'xz_xsection_streamlines_%06d_%06d_%s.png'%(self.ana_lat*1000,self.ana_lon*1000,self.time_files)
            if self.bsavepic:
                fig.savefig(picpath_xz_stream + os.sep + picfilename, 
                            format='png', transparent=False, dpi=300, pad_inches = 0)
                self.myprint(picpath_xz_stream + os.sep + picfilename + ' saved!')
            plt.close(fig)
            self.write_to_progress_cross(50)
        except:
            self.myprint('可能是垂直剖面的经纬度超出范围了，请重新设置再尝试！')
        #=================
        try:
            fig, ax = plt.subplots(figsize=(8, 4))
            pydda.vis.plot_yz_xsection_barbs(Grids, None, 'reflectivity', level=lev_yz,
                                            #  w_vel_contours=[ 3, 5, ],
                                            cmap='NWSRef', vmin=0, vmax=70,
                                            barb_spacing_y_km=2,
                                            barb_spacing_z_km=1)
            ax.set_xlim(self.ylims.tolist())
            ax.set_title(ax.get_title().replace('PyDDA','MultiRadar'))
            picfilename = 'yz_xsection_barbs_%06d_%06d_%s.png'%(self.ana_lat*1000,self.ana_lon*1000,self.time_files)
            if self.bsavepic:
                fig.savefig(picpath_yz_bar + os.sep + picfilename, 
                            format='png', transparent=False, dpi=300, pad_inches = 0)
                self.myprint(picpath_yz_bar + os.sep + picfilename + ' saved!')
            plt.close(fig)
            self.write_to_progress_cross(75)
        except:
            self.myprint('可能是垂直剖面的经纬度超出范围了，请重新设置再尝试！')
        #=================
        try:
            fig, ax = plt.subplots(figsize=(8, 4))
            pydda.vis.plot_yz_xsection_streamlines(Grids, None, 'reflectivity', level=lev_yz,
                                                cmap='NWSRef', vmin=0, vmax=70,
                                                #  w_vel_contours=[3, 5, ],
                                                )
            ax.set_xlim(self.ylims.tolist())
            ax.set_title(ax.get_title().replace('PyDDA','MultiRadar'))
            picfilename = 'yz_xsection_streamlines_%06d_%06d_%s.png'%(self.ana_lat*1000,self.ana_lon*1000,self.time_files)
            if self.bsavepic:
                fig.savefig(picpath_yz_stream + os.sep + picfilename, 
                            format='png', transparent=False, dpi=300, pad_inches = 0)
                self.myprint(picpath_yz_stream + os.sep + picfilename + ' saved!')
            plt.close(fig)
            self.write_to_progress_cross(100)
        except:
            self.myprint('可能是垂直剖面的经纬度超出范围了，请重新设置再尝试！')
        return True

    def do_wind_retrievel(self):

        self.write_to_progress_main(0)
        
        filepaths = []
        filenames = []

        for rd in self.radars:
            curpath = self.radarfilepath + os.sep + rd
            filepaths.append(curpath)
            files = os.listdir(curpath)
            files = sorted(files)

            files = [ff for ff in files if ff.find('Z_RADR')>=0]
            times = [datetime.strptime(ff[15:29], '%Y%m%d%H%M%S').timestamp()  for ff in files]


            if len(times) == 0:
                self.myprint('未找到 %s 雷达站的有效数据文件，请检查！'%rd)
                self.write_to_progress_main(100)
                return False

            ct = datetime.strptime(self.time_files, '%Y%m%d%H%M%S').timestamp()
            cidx = np.where(abs(np.array(times) - ct)==min(abs(np.array(times)-ct)))[0][0]
            if min(abs(np.array(times)-ct)) > 180:
                self.myprint('查找到 %s 雷达的资料中离规定时间最近的文件的观测时间与规定时间的差超过了180秒，请知悉！'%rd)
            self.myprint('找到的 %s 雷达的资料中与规定时间最接近的文件名为 %s '%(rd,files[cidx]))
            filenames.append(files[cidx])

        self.write_to_progress_main(5)    


        radar_objs=[]
        grid_objs=[]
        self.outname_grid=[]
        self.valid_radars=len(filenames)
        for nn in range(self.valid_radars):
            outname = filenames[nn].split('.')
            self.outname_grid.append(filenames[nn].replace(outname[-1],'wind.nc'))

        for nn in range(self.valid_radars):
            pass

            radar = read_cnrad_fmt(filepaths[nn] + os.sep + filenames[nn])
            # 退速速模糊
            newvel = pyart.correct.dealias_region_based(radar, vel_field='velocity')
            radar.add_field('corrected_velocity', newvel, replace_existing=True)
            radar_objs.append(radar)

            self.myprint('read %s over!'%filenames[nn])

            grid_objs.append(pyart.map.grid_from_radars(
            (radar_objs[nn],),
            grid_origin=[self.standard_lat,self.standard_lon],
            weighting_function = 'BARNES2',
            grid_shape=(self.g_numz, self.g_numy, self.g_numx),
            grid_limits=(self.g_zlim, self.g_ylim, self.g_xlim),
            fields=['reflectivity','velocity','corrected_velocity']).to_xarray())
            self.myprint('grid over!')

        self.write_to_progress_main(20)

        # step 2=============风场反演
        
        # 给grid对象添加部分属性point_x, point_y, point_z, AZ, EL, point_altitude
        # 按z，y，x的顺序，进行meshgrid
        point_z,point_y,point_x = np.mgrid[
            self.g_zlim[0]:self.g_zlim[1]+self.zreso:self.zreso,
            self.g_ylim[0]:self.g_ylim[1]+self.yreso:self.yreso,
            self.g_xlim[0]:self.g_xlim[1]+self.xreso:self.xreso
        ]

        # 将point_x, point_y, point_z添加到grid对象中
        for nn in range(len(grid_objs)):
            grid_objs[nn]['point_x'] = (('z','y','x'), point_x)
            grid_objs[nn]['point_y'] = (('z','y','x'), point_y)
            grid_objs[nn]['point_z'] = (('z','y','x'), point_z)

        # 根据z，y，x坐标，计算对应的方位角和仰角，并存储在AZ变量和EL变量中
        for nn in range(len(grid_objs)):
            grid_objs[nn]['AZ'] = (('z','y','x'), pyart.core.cartesian_to_antenna(
                grid_objs[nn]['point_x'].data,
                grid_objs[nn]['point_y'].data,
                grid_objs[nn]['point_z'].data)[1])
            grid_objs[nn]['EL'] = (('z','y','x'), pyart.core.cartesian_to_antenna(
                grid_objs[nn]['point_x'].data,
                grid_objs[nn]['point_y'].data,
                grid_objs[nn]['point_z'].data)[2])
        # 将高度信息存储在point_altitude变量中,直接采用z坐标值
        for nn in range(len(grid_objs)):
            grid_objs[nn]['point_altitude'] = (('z','y','x'), grid_objs[nn]['point_z'].data)
            
            
        # 添加探空数据
        if not os.path.exists(self.tlogp_filepath + os.sep + self.tlogp_filename):
            self.myprint(self.tlogp_filepath + os.sep + self.tlogp_filename + ' not exists! please check!')
            self.write_to_progress_main(100)
            return False

        profile = get_profile(self.tlogp_filepath,self.tlogp_filename)
        grid_objs[0]= pydda.initialization.make_wind_field_from_profile(
                grid_objs[0], profile, vel_field='corrected_velocity')
        
        # u_init, v_init, w_init = None,None,None
        self.myprint('探空数据 %s 加载完毕！'%self.tlogp_filename)
        # 速度退模糊暂时空缺，后期补上


        #进行多雷达三维风场反演
        # try:

        print('=================================')
        # return new_grid_list, parameters
        Grids, _ = pydda.retrieval.get_dd_wind_field(
                                                    grid_objs,
                                                    Co=100.0,
                                                    Cm=1500.0,
                                                    Cx=1e-2,
                                                    Cy=1e-2,
                                                    Cz=1e-2,
                                                    vel_name="corrected_velocity",
                                                    refl_field="reflectivity",
                                                    frz=5000.0,
                                                    engine="tensorflow", # 有GPU就用tensorflow，没有就用scipy或jax
                                                    mask_outside_opt=True,
                                                    upper_bc=1,
                                                )
        # except:
        #     self.myprint('get_dd_wind_field error!')
        #     self.write_to_progress_main(100)
        #     return False
        
        # 计算拉伸涡度和倾斜涡度
        Grids = self.calc_vortex(Grids)
        
        for nn in range(len(Grids)):
            # 将reflectivity小于10的格点设置为缺省值
            Grids[nn]['reflectivity'] = Grids[nn]['reflectivity'].where(Grids[nn]['reflectivity']>=10)
            outname = filenames[nn].split('.')
            
            Grids[nn].to_netcdf(self.outpath + os.sep + filenames[nn].replace(outname[-1],'wind.nc'))
            

        # exclude masked gates from the gridding
        # gatefilter = pyart.filters.GateFilter(Grids)
        # gatefilter.exclude_masked('reflectivity')

        self.myprint('write data to nc over!')
        self.write_to_progress_main(80)
        # print('All levels = %d'%g_numz)
        
        # self.draw_all_pic()

        self.write_to_progress_main(100)
        # if bshowpic:
        #     plt.show()

        if os.path.exists('process_info.txt'):
            shutil.copyfile('process_info.txt',self.picpath + os.sep + 'process_info.txt')

    #计算拉伸涡度和倾斜涡度
    def calc_vortex(self,Grids):
        self.myprint('starting calc_vortex......')
        

        
        if isinstance(Grids, list):
            uwind=Grids[0]['u'].values
            vwind=Grids[0]['v'].values
            wwind=Grids[0]['w'].values
        else:
            uwind=Grids['u'].values
            vwind=Grids['v'].values
            wwind=Grids['w'].values
        # 检查uwind的维度是否是四维
        if uwind.ndim != 4:
            raise ValueError("Expected uwind to be a 4D array with dimensions (time, z, y, x)")
        
        vor_stretch_data = np.zeros(uwind.shape,dtype=float)
        vor_tilt_data = np.zeros(uwind.shape,dtype=float)
        for tt in range(uwind.shape[0]):
            dudz,dudy,dudx = np.gradient(uwind[tt,:,:,:])
            dvdz,dvdy,dvdx = np.gradient(vwind[tt,:,:,:])
            dwdz,dwdy,dwdx = np.gradient(wwind[tt,:,:,:])
            
            for lev in range(uwind.shape[1]):
                # picname="stretch_%03d.png"%lev
                # print(lev)
                
                div=dudx[lev,:,:] + dvdy[lev,:,:]
                vor=dvdx[lev,:,:] - dudy[lev,:,:]
                
                u=uwind[tt,lev,:,:]
                v=vwind[tt,lev,:,:]
                #dw
                
                #拉伸项
                vor_stretch_data[tt,lev,:,:]=vor*div

                #倾斜项
                vor_tilt_data[tt,lev,:,:]=dwdy[lev,:,:]*dudz[lev,:,:]-dwdx[lev,:,:]*dvdz[lev,:,:]


        if isinstance(Grids, list):
            for nn in range(len(Grids)):
                Grids[nn]['vor_stretch'] = (('time','z','y','x'), vor_stretch_data)
                Grids[nn]['vor_tilt'] = (('time','z','y','x'), vor_tilt_data)
                
                # add attrs
                
                Grids[nn]['vor_stretch'].attrs['long_name'] = 'stretch vortex'
                Grids[nn]['vor_stretch'].attrs['units'] = 'm-2/s-2'
                Grids[nn]['vor_stretch'].attrs['coordinates'] = 't,z,y,x'
                
                Grids[nn]['vor_tilt'].attrs['long_name'] = 'tilting vortex'
                Grids[nn]['vor_tilt'].attrs['units'] = 'm-2/s-2'
                Grids[nn]['vor_tilt'].attrs['coordinates'] = 't,z,y,x'
            
        else:
            Grids['vor_stretch'] = (('time','z','y','x'), vor_stretch_data)
            Grids['vor_tilt'] = (('time','z','y','x'), vor_tilt_data)
            Grids['vor_stretch'].attrs['long_name'] = 'stretch vortex'
            Grids['vor_stretch'].attrs['units'] = 'm-2/s-2'
            Grids['vor_stretch'].attrs['coordinates'] = 't,z,y,x'
            
            Grids['vor_tilt'].attrs['long_name'] = 'tilting vortex'
            Grids['vor_tilt'].attrs['units'] = 'm-2/s-2'
            Grids['vor_tilt'].attrs['coordinates'] = 't,z,y,x'
        

        return Grids

            
    

if __name__ == "__main__":
    '''
    有两个函数可以调用：
    python main_pydda.py config_3dwind.ini --getwind
    python main_pydda.py config_3dwind.ini --drawcross
    其中第一个用来计算并绘图，第二个用来单独绘制任意点的垂直剖面
    '''
    with open('current_pid.txt','wt') as f:
        f.write('%s'%str(os.getpid()))
    def mainprog(inifile):

        _getwind = GET_WIND_3D(inifile) 
        if not _getwind.error:
            _getwind.do_wind_retrievel()
            _getwind.draw_all_pic()

        else:
            print('GET_WIND_3D Init error!')
            sys.exit(-1)
    
    def drawpic_cross(inifile):
        _getwind = GET_WIND_3D(inifile)
        if not _getwind.error:
            _getwind.draw_all_pic(bdrawhori=False)
        else:
            print('GET_WIND_3D Init error!')
            sys.exit(-1)

    parser = argparse.ArgumentParser(description='Do radar 3d wind retrievel.')
    parser.add_argument('inifile', metavar='inifile', type=str, 
                    help='ini filename for the mainprog')
    
    parser.add_argument('--getwind', dest='mainprog', action='store_const', 
                        const=mainprog,
                        help='do 3d wind retrievel')
    
    parser.add_argument('--drawcross', dest='drawpic_cross', action='store_const', 
                        const=drawpic_cross,
                        help='draw XZ and YZ pic')
    BDEBUG=True

    if BDEBUG:
        print('当前为debug模式，所以直接读取主目录下的config.ini, 请注意！')
        config_path = 'metradar/retrieve/wind/config_3dwind.ini'
        if not os.path.exists(config_path):
            print('%s not exists! please check!'%config_path)
            sys.exit(-1)
        mainprog(config_path)
        
    else:
        args = parser.parse_args()
        if not args.mainprog is None:
            args.mainprog(args.inifile)

        if not args.drawpic_cross is None:
            args.drawpic_cross(args.inifile)


# %%
