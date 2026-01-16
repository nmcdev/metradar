# _*_ coding: utf-8 _*_
'''
制作区域历史雷达拼图框架程序，多进程
朱文剑

'''

# %%

import pyart
import os
import time
import pandas as pd
from datetime import datetime,timedelta
import numpy as np
from multiprocessing import Process,Pool
from metradar.core.mosaic_merge import mosaic_merge
import configparser
import warnings
import glob
warnings.filterwarnings('ignore')
import schedule
import threading
from metradar.io.decode_fmt_pyart import read_cnrad_fmt

class MAKE_RADAR_MOSAIC:

    # sub function for reading config file
    def ConfigFetchError(Exception):
        pass

    def _get_config_from_rcfile(self, rcfile):
        """
        Get configure information from xxx.ini file.
        """
        # print(os.getcwd())
        # rc = os.getcwd() + os.sep + rcfile
        rc = rcfile
        if not os.path.exists(rc):
            print('Config file: ' + rc + ' not exists!')
            return None
        try:
            config = configparser.ConfigParser()
            config.read(rc,encoding='UTF-8')
        except IOError as e:
            raise self.ConfigFetchError(str(e))
        except Exception as e:
            raise self.ConfigFetchError(str(e))

        return config
        
    def __init__(self,rcfile) -> None:
        self.berror = False
        if not os.path.exists(rcfile):
            print(rcfile + ' not exists!')
            self.berror = True
            return None
        config = self._get_config_from_rcfile(rcfile)
        self.run_mode = config['RUN_MODE']['run_mode']

        self.radar_mode = config['COMMON_SETTING']['radar_mode']
        self.radar_sitesfile = config['COMMON_SETTING']['radar_sitesfile']

        if not os.path.exists(self.radar_sitesfile):
            print(self.radar_sitesfile + ' not exists!')
            self.berror = True
            return None

        self.realtime_peroids = int(config['REAL_TIME']['realtime_peroids'])

        self.center_lon = float(config['COMMON_SETTING']['center_lon'])
        self.center_lat = float(config['COMMON_SETTING']['center_lat'])
        self.mosaic_range = float(config['COMMON_SETTING']['mosaic_range'])
        self.hori_reso = float(config['COMMON_SETTING']['hori_reso'])
        self.verti_reso = float(config['COMMON_SETTING']['verti_reso'])
        self.breplace = int(config['COMMON_SETTING']['breplace'])
        self.bshow_debuginfo = int(config['COMMON_SETTING']['bshow_debuginfo'])

        self.multi_levels = config['COMMON_SETTING']['multi_levels'].split(',')
        self.multi_levels = [float(tt.strip(' ')) for tt in self.multi_levels]
        self.mosaic_vars = config['COMMON_SETTING']['mosaic_vars'].split(',')
        self.mosaic_vars = [tt.strip(' ') for tt in self.mosaic_vars]

        if self.radar_mode == 'manul':
            self.radars = config['COMMON_SETTING']['radars'].split(',')
            self.radars = [tt.strip(' ') for tt in self.radars]
        else:
            pass
            # 根据经纬度和半径，自动计算目标范围内的雷达站名
            # 将km半径大致除以100，换算成°
            minlon = self.center_lon - self.mosaic_range / 100.0
            maxlon = self.center_lon + self.mosaic_range / 100.0
            minlat = self.center_lat - self.mosaic_range / 100.0
            maxlat = self.center_lat + self.mosaic_range / 100.0

            # load radar site file

            # 根据数字站号查找对应的中文名
            
            frad = open(self.radar_sitesfile,'rt',encoding='UTF-8')
            
            infos = frad.readlines()
            radsite_num = {}
            radname_gr2 = {}
            staname_chn={}
            radarlat={}
            radarlon={}
            radheight={}
            
            for ss in infos:
                tmps = ss.rstrip('\n').split('|')

                curlon = float(tmps[4])
                curlat = float(tmps[3])
                if curlon > minlon and curlon < maxlon and curlat > minlat and curlat < maxlat:
                    radsite_num[tmps[0]] = tmps[2]
                    radname_gr2[tmps[2]] = tmps[0]
                    staname_chn[tmps[2]] = tmps[1]
                    radarlat[tmps[2]]=float(tmps[3])
                    radarlon[tmps[2]]=float(tmps[4])
                    radheight[tmps[2]]=float(tmps[5])
            self.radars = list(radsite_num.keys())

        self.input_path_realtime = config['REAL_TIME']['input_path_realtime']
        self.output_path_realtime = config['REAL_TIME']['output_path_realtime']
        self.input_path_archive = config['ARCHIVE']['input_path_archive']
        self.output_path_archive = config['ARCHIVE']['output_path_archive']

        self.starttime = config['ARCHIVE']['starttime']
        self.endtime = config['ARCHIVE']['endtime']
        self.tstep = int(config['ARCHIVE']['tstep'])

        # check starttime and enttime
        try:
            st = datetime.strptime(self.starttime,'%Y%m%d%H%M')
        except:
            self.berror = True
            print('starttime set error!')
            return None
        
        try:
            et = datetime.strptime(self.endtime,'%Y%m%d%H%M')
        except:
            self.berror = True
            print('endtime set error!')
            return None

        

        # check time step
        if self.tstep < 0 or self.tstep > 60:
            print('tstep set error, should between 0 and 60')
            self.berror = True
            return None
        
        if (et - st).total_seconds() < self.tstep * 60:
            print('seconds between endtime and starttime is less than tstep, error!')
            self.berror = True
            return None


        pass


    def make_mosaic(self, params):
    
        '''
        params={
            # 原始数据路径
            'rootpath':rootpath,
            # 输出数据路径
            'outpath': outpath,
            # 参与拼图的雷达站点
            'radars': ['NJJS','HAJS','BBAH','MASR','HFAH','CZJS','TZJS'], 
            'origin_latitude':center_lat, # 拼图的中心纬度
            'origin_longitude':center_lon, # 拼图的中心经度
            'mosaic_range':mosaic_range, # 拼图半径，km，其实是正方形
            'hor_reso':1, # 水平方向分辨率，km
            'ver_reso':0.5, # 垂直方向分辨率，km
            'bdebug':True,
            'timestr':timestr,
            'bot_z_lev':0.5, # 最底层所在高度
            'top_z_lev':16,  # 最顶层所在高度
            'breplace':True, 
            'outname': 'mosaic_%s.nc'%timestr
            }
        '''

        rootpath=params['rootpath']
        outpath=params['outpath']
        radars=params['radars']
        origin_latitude = params['origin_latitude']
        origin_longitude = params['origin_longitude']
        mr = params['mosaic_range']
        hor_reso = params['hor_reso']
        ver_reso = params['ver_reso']
        bdebug = params['bdebug']
        timestr = params['timestr']
        outname = params['outname']
        bot_z_lev = params['bot_z_lev']
        top_z_lev = params['top_z_lev']
        breplace = params['breplace']

        # if timestr == '202307031912':
        #     pass
        if not os.path.exists(outpath): os.makedirs(outpath)
        # timestr = '201607190112'
        if not breplace and os.path.exists(outpath + os.sep + outname): 
            print(outname + ' already exits!')
            return False
        curt = datetime.strptime(timestr,'%Y%m%d%H%M')
        curt = curt.timestamp()
        timeinfo=dict()
        fileinfo=dict()
        nvalidradars = 0
        validradars=[]
        for radar in radars:
            curpath = rootpath  + os.sep + radar
            if not os.path.exists(curpath):
                # print(curpath + ' not exists!')
                # radars.remove(radar)
                continue
            
            curfiles = os.listdir(curpath)
            if len(curfiles)==0:
                print(curpath + ' file number = %d'%len(curfiles))
                continue
            
            validfiles = []
            for ff in curfiles:
                # if ff.find('ar2v') < 0:
                if ff.find('_CAP_FMT.bin.bz2') < 0:
                    continue
                else:
                    validfiles.append(ff)
            curfiles = sorted(validfiles)
            tmptime=[]
            tmpfile=[]
            for file in curfiles:
                if file.find(radar) < 0:
                    print(file + ' not contain  %s, so continue...'%radar)
                    # curfiles.remove(file)
                    continue

                # cft = datetime.strptime(file[5:5+14],'%Y%m%d%H%M%S')
                # Z_RADR_I_Z9090_20230703193348_O_DOR_SB_CAP_FMT.bin.bz2
                # cft = datetime.strptime(file[5:13]+file[14:20],'%Y%m%d%H%M%S')
                cft = datetime.strptime(file[15:29],'%Y%m%d%H%M%S')
                tmptime.append(cft.timestamp())
                tmpfile.append(curpath + os.sep + file)
            if len(tmptime)==0:
                continue
            else:
                timeinfo[radar]=tmptime
                fileinfo[radar]=tmpfile

            nvalidradars +=1
            validradars.append(radar)
        used_files = []
        if nvalidradars == 0:
            print('no valid files')
            return False

        for radar in validradars:
            # print(radar)
            tmpt = np.array(timeinfo[radar]) - curt
            # try:
            idx=list(abs(tmpt)).index(abs(tmpt).min())
            # except:
            #     pass
            if isinstance(idx,list):
                idx = idx[0]
            if abs(tmpt[idx]) > 240:
                continue
            used_files.append(fileinfo[radar][idx])
        # assert isinstance(filenames,list)

        if len(used_files)==0:
            print('Error: len(used_files)==0')
            return False
        xgridnum = int(2*mr/hor_reso) + 1
        ygridnum = int(2*mr/hor_reso) + 1
        zgridnum = int((top_z_lev - bot_z_lev)/ver_reso) + 1
        print('xgridnum = %d, ygridnum = %d, zgridnum = %d'%(xgridnum,ygridnum,zgridnum))
        radars=[]
        for filename in used_files:
            try:
                # radar = pyart.io.read_nexrad_archive(filename)
                radar = read_cnrad_fmt(filename)
            except:
                print(filename + ' read error!')
                continue
            radars.append(radar) 
            print('add file %s'%filename)

        sttime = time.time()
        grid = pyart.map.grid_from_radars(
            tuple(radars),
            grid_origin = [origin_latitude,origin_longitude],
            grid_shape=(zgridnum, ygridnum, xgridnum),
            grid_limits=((bot_z_lev*1000, top_z_lev*1000), (-1.0*mr*1000, 1.0*mr*1000), (-1.0*mr*1000, 1.0*mr*1000)),
            fields=['reflectivity',])#'differential_reflectivity','differential_phase'
            
        edtime = time.time()
        if bdebug:
            print('grid over! cost time = %d seconds'%(int(edtime - sttime)))


        outref = grid.fields['reflectivity']['data']
        # prepare for netcdf output
        # outref = outref *2 + 64
        # grid.fields['reflectivity']['data'] = outref
        # grid.fields['reflectivity']['valid_max'] = np.array(grid.fields['reflectivity']['valid_max'] * 2 + 64).astype(np.uint8)
        # grid.fields['reflectivity']['valid_min'] = np.array(grid.fields['reflectivity']['valid_min'] * 2 + 64).astype(np.uint8)
    
        grid.fields['reflectivity']['data'] = outref
        grid.fields['reflectivity']['valid_max'] = np.array(grid.fields['reflectivity']['valid_max'])
        grid.fields['reflectivity']['valid_min'] = np.array(grid.fields['reflectivity']['valid_min'])

        # write to netcdf
        # gx = grid.to_xarray()
        # gx.reflectivity.data=gx.reflectivity.astype(np.uint8)
        # gx = gx.drop_vars('ROI')
        # cref = np.nanmax(gx.reflectivity.data[0,:,:,:],axis=0)
        # # 增加变量到Dataset
        # gx["cref"]=(['y', 'x'],cref,{'scale':2,'offset':64,'decode':'dBZ = (cref-64)/2'})
        # # gx = gx.drop_vars('reflectivity')
        # gx.to_netcdf(outpath + os.sep + outname)

        pyart.io.write_grid(outpath + os.sep + outname ,grid)

        print(outpath + os.sep + outname + ' done!')

    def do_realtime(self,):
        if os.path.exists('file.lock'): 
            print('do_realtime still running')
            return False

        print('进入 do_realtime')

        # 创建锁文件，防止程序重复运行
        lockfile = open('file.lock','wt')
        lockfile.write(datetime.utcnow().strftime('%Y%m%d%H%M%S'))
        lockfile.close()

        rootpath = self.input_path_realtime
        outpath = self.output_path_realtime
        radars = self.radars
        center_lat = self.center_lat
        center_lon = self.center_lon
        mosaic_range = self.mosaic_range
        hor_reso = self.hori_reso
        ver_reso = self.verti_reso
        bshow_debug = self.bshow_debuginfo
        breplace = self.breplace
        levels = self.multi_levels

        allparams = []
        ct = datetime.utcnow()
        newct = ct - timedelta(minutes= ct.minute % 6 )
        timestr = newct.strftime('%Y%m%d%H%M')
        print('processing : ' + timestr)

        alloutfiles=[]
        mergename = outpath + os.sep +  'mosaic_%s.nc'%timestr
        if not breplace and os.path.exists(mergename):
            print(mergename + ' already exists!')
            return True

        for nn in range(len(levels)-1):

            outname = 'mosaic_%s_%d_tmp.nc'%(timestr,nn+1)
            # 分为多个进程，每个进程计算一段高度上的拼图，最后再进行拼接
            params={
            # 原始数据路径
            'rootpath':rootpath,
            # 输出数据路径
            'outpath': outpath,
            # 参与拼图的雷达站点
            'radars':radars, 
            'origin_latitude':center_lat, # 拼图的中心纬度
            'origin_longitude':center_lon, # 拼图的中心经度
            'mosaic_range':mosaic_range, # 拼图半径，km，其实是正方形
            'hor_reso':hor_reso, # 水平方向分辨率，km
            'ver_reso':ver_reso, # 垂直方向分辨率，km
            'bdebug':bshow_debug,
            'timestr':timestr, # 时间戳，程序按照这个时间来匹配数据
            'bot_z_lev':levels[nn] + ver_reso, # 最底层所在高度
            'top_z_lev':levels[nn+1],  # 最顶层所在高度
            'breplace':breplace, 
            'outname':outname,
            }

            alloutfiles.append(outpath + os.sep + outname)
            allparams.append(params)

        MAXP=len(levels)-1
        pools = Pool(MAXP)

        pools.map(self.make_mosaic, allparams)
        pools.close()
        pools.join()

        # 对数据进行拼接
        
        newgrid = mosaic_merge(files = alloutfiles)
        # write to file
        if newgrid is not None:
            newgrid.write(mergename)
            print(mergename + ' saved!')
                # time.sleep(2)
            
        else:
            print('merge error!')
        # delete temp files
        files = glob.glob(outpath + os.sep + '*tmp.nc')
        if len(files) >0:
            for file in files:
                os.remove(file)   
        
        # 删除锁文件，允许程序运行
        if os.path.exists('file.lock'): os.remove('file.lock')


    def do_archive(self,):
        print('进入 make_mosaic do_archive......')
        st = datetime.strptime(self.starttime,'%Y%m%d%H%M')
        et = datetime.strptime(self.endtime,'%Y%m%d%H%M')
        periods = int(((et - st).total_seconds() / 60)/self.tstep)+1
        print('periods = %d'%periods)
        dt = pd.date_range(st,freq='%dmin'%self.tstep,periods=periods)
        dt = pd.to_datetime(dt)
        alltimes=[tt.strftime('%Y%m%d%H%M') for tt in dt]

        
            
        # radars = self.radars
        # center_lat = _make_mosaic.center_lat
        # center_lon = _make_mosaic.center_lon
        # mosaic_range = _make_mosaic.mosaic_range
        # hor_reso = _make_mosaic.hori_reso
        # ver_reso = _make_mosaic.verti_reso
        # bshow_debug = self.bshow_debuginfo
        # breplace = _make_mosaic.breplace
        # levels = self.multi_levels

        # levels  高度层 单位KM
        # levels = [0, 5, 9, 12, 16]
        # levels = [0, 7, 10, 13, 16, 20] 82s
        # levels = [0, 7, 10, 13, 16, 20]
        # MAXP = int(cpu_count()*0.2)

        
        for timestr in alltimes:
            allparams = []
            print('processing : ' + timestr)

            alloutfiles=[]
            rootpath = self.input_path_archive + os.sep + timestr[0:4] + os.sep + timestr[0:8]
            outpath = self.output_path_archive

            if not os.path.exists(outpath):
                os.makedirs(outpath)
            mergename = outpath + os.sep +  'mosaic_%s.nc'%timestr
            if not self.breplace and os.path.exists(mergename):
                print(mergename + ' already exists!')
                continue

            for nn in range(len(self.multi_levels)-1):

                outname = 'mosaic_%s_%d_tmp.nc'%(timestr,nn+1)
                # 分为多个进程，每个进程计算一段高度上的拼图，最后再进行拼接
                params={
                # 原始数据路径
                'rootpath':rootpath,
                # 输出数据路径
                'outpath': outpath,
                # 参与拼图的雷达站点
                'radars':self.radars, 
                'origin_latitude':self.center_lat, # 拼图的中心纬度
                'origin_longitude':self.center_lon, # 拼图的中心经度
                'mosaic_range':self.mosaic_range, # 拼图半径，km，其实是正方形
                'hor_reso':self.hori_reso, # 水平方向分辨率，km
                'ver_reso':self.verti_reso, # 垂直方向分辨率，km
                'bdebug':self.bshow_debuginfo,
                'timestr':timestr, # 时间戳，程序按照这个时间来匹配数据
                'bot_z_lev':self.multi_levels[nn] + nn*self.verti_reso, # 最底层所在高度
                'top_z_lev':self.multi_levels[nn+1],  # 最顶层所在高度
                'breplace':self.breplace, 
                'outname':outname,
                }

                alloutfiles.append(outpath + os.sep + outname)
                allparams.append(params)

            MAXP=len(self.multi_levels)-1
            pools = Pool(MAXP)

            pools.map(self.make_mosaic, allparams)
            pools.close()
            pools.join()

            # 对数据进行拼接
            
            newgrid = mosaic_merge(files = alloutfiles)
            if newgrid is not None:
                # write to file
                newgrid.write(mergename)

                print(mergename + ' saved!')
            else:
                print('merge error!')
            # time.sleep(2)
        # delete temp files
            files = glob.glob(outpath + os.sep + '*tmp.nc')
            if len(files) >0:
                for file in files:
                    os.remove(file)

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

if __name__ == "__main__":

    print('starting mosaic program......')
    
    _make_mosaic = MAKE_RADAR_MOSAIC('make_mosaic_mp_archive.ini')

    if not _make_mosaic.berror:
        if _make_mosaic.run_mode == 'archive':
            _make_mosaic.do_archive()
        else:
            print('run mode is not archive!')

            

# %%
