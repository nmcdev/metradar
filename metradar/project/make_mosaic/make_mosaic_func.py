# _*_ coding: utf-8 _*_

# 制作三维拼图的核心函数


import pyart
import os
import time
import pandas as pd
from datetime import datetime,timedelta
import numpy as np
from multiprocessing import Pool
from metradar.core.mosaic_merge import mosaic_merge

import configparser
import warnings
import glob
warnings.filterwarnings('ignore')

from metradar.util.radar_common import RADAR_COMMON
from metradar.io.decode_fmt_pyart import read_cnrad_fmt
radarcommon = RADAR_COMMON()

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
        
    def __init__(self,rcfile):
        self.berror = False
        if not os.path.exists(rcfile):
            print(rcfile + ' not exists!')
            self.berror = True
            

        config = self._get_config_from_rcfile(rcfile)
        self.run_mode = config['RUN_MODE']['run_mode']
        

        self.radar_mode = config['COMMON_SETTING']['radar_mode']
        self.radar_sitesfile = config['COMMON_SETTING']['radar_sitesfile']

        if not os.path.exists(self.radar_sitesfile):
            print(self.radar_sitesfile + ' not exists!')
            self.berror = True
            

        self.realtime_peroids = int(config['REAL_TIME']['realtime_peroids'])

        self.filename_type = config['COMMON_SETTING']['filename_type']
        
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
            # Automatically calculate the radar station name within the target range according to longitude, latitude and radius
            #
            minlon = self.center_lon - self.mosaic_range / 100.0
            maxlon = self.center_lon + self.mosaic_range / 100.0
            minlat = self.center_lat - self.mosaic_range / 100.0
            maxlat = self.center_lat + self.mosaic_range / 100.0

            # load radar site file

            # Find the corresponding Chinese name according to the digital station number
            
            frad = open(self.radar_sitesfile,'rt',encoding='UTF-8')
            
            infos = frad.readlines()
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

                    radname_gr2[tmps[2]] = tmps[0]
                    staname_chn[tmps[2]] = tmps[1]
                    radarlat[tmps[2]]=float(tmps[3])
                    radarlon[tmps[2]]=float(tmps[4])
                    radheight[tmps[2]]=float(tmps[5])
            self.radars = list(staname_chn.values())

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
 
        
        try:
            et = datetime.strptime(self.endtime,'%Y%m%d%H%M')
        except:
            self.berror = True
            print('endtime set error!')


        # check time step
        if self.tstep < 0 or self.tstep > 60:
            print('tstep set error, should between 0 and 60')
            self.berror = True

        
        if (et - st).total_seconds() < self.tstep * 60:
            print('seconds between endtime and starttime is less than tstep, error!')
            self.berror = True

        if self.berror:
            return radarcommon.RETURN_CODE_PARAM_ERROR


    def make_mosaic(self, params):
    
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


        os.makedirs(outpath,exist_ok=True)
        # timestr = '20220415155500'
        if not breplace and os.path.exists(outpath + os.sep + outname): 
            print(outname + ' already exits!')
            return False
        curt = datetime.strptime(timestr,'%Y%m%d%H%M%S') 
        curt = curt.timestamp()
        timeinfo=dict()
        fileinfo=dict()
        nvalidradars = 0
        validradars=[]
        for radar in radars:
            curpath = rootpath + os.sep + radar
            if not os.path.exists(curpath):
                print(curpath + ' not exists!')
                # radars.remove(radar)
                continue
            
            curfiles = os.listdir(curpath)
            if len(curfiles)==0:
                print(curpath + ' file number = %d'%len(curfiles))
                continue
            
            validfiles = []
            for ff in curfiles:
                if ff.find('bz2') < 0:
                    continue
                else:
                    validfiles.append(ff)
            curfiles = sorted(validfiles)
            tmptime=[]
            tmpfile=[]
            for file in curfiles:
  

                # cft = datetime.strptime(file[5:5+14],'%Y%m%d%H%M%S')
                cft = datetime.strptime(file.split('_')[4],'%Y%m%d%H%M%S')
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
            return radarcommon.RETURN_CODE_DATA_ERROR

        xgridnum = int(2*mr/hor_reso) + 1
        ygridnum = int(2*mr/hor_reso) + 1
        zgridnum = int((top_z_lev - bot_z_lev)/ver_reso) + 1
        print('top_z_lev = %.1f, bot_z_lev = %.1f, xgridnum = %d, ygridnum = %d, zgridnum = %d'%(top_z_lev,bot_z_lev,xgridnum,ygridnum,zgridnum))
        
        radars=[]
        for filename in used_files:
            try:
                # radar = pyart.io.read_nexrad_archive(filename)
                radar = read_cnrad_fmt(filename)
            except:
                print(filename + ' read error!')
                continue
            radars.append(radar) 
            # print('add file %s'%filename)

        sttime = time.time()
        grid = pyart.map.grid_from_radars(
            tuple(radars),
            grid_origin = [origin_latitude,origin_longitude],
            grid_shape=(zgridnum, ygridnum, xgridnum),
            grid_limits=((bot_z_lev*1000, top_z_lev*1000), (-1.0*mr*1000, 1.0*mr*1000), (-1.0*mr*1000, 1.0*mr*1000)),
            fields=['reflectivity'])
            
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
        # # add var to Dataset
        # gx["cref"]=(['y', 'x'],cref,{'scale':2,'offset':64,'decode':'dBZ = (cref-64)/2'})
        # # gx = gx.drop_vars('reflectivity')
        # gx.to_netcdf(outpath + os.sep + outname)

        pyart.io.write_grid(outpath + os.sep + outname ,grid)

        print(outpath + os.sep + outname + ' done!')

        
        return radarcommon.RETURN_CODE_SUCESSED

    def do_realtime(self,):
        if os.path.exists('file_mosaic.lock'): 
            # check time If more than 10 minutes have passed, delete and run
            fin = open('file_mosaic.lock','rt')
            tstr = fin.readline()
            fin.close()
            brestart = False
            if len(tstr) == 0:
                brestart = True
            ctt = datetime.strptime(tstr,'%Y%m%d%H%M%S')
            if datetime.utcnow().timestamp() -  ctt.timestamp() > 60*10:
                brestart = True

            if brestart:
                os.remove('file_mosaic.lock')
            else:
                print('do_realtime is still running')
                return radarcommon.RETURN_CODE_SUCESSED

        print('entering do_realtime')

        # Create a lock file to prevent the program from running repeatedly
        lockfile = open('file_mosaic.lock','wt')
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
        newct = ct - timedelta(minutes= (ct.minute % 6 + 6))
        timestr = newct.strftime('%Y%m%d%H%M') + '00'
        print('processing : ' + timestr)

        alloutfiles=[]
        aa = self.filename_type.split('$')
        mergename = outpath + os.sep +  '%s%s%s'%(aa[0],timestr,aa[2])
        if not breplace and os.path.exists(mergename):
            print(mergename + ' already exists!')
            return radarcommon.RETURN_CODE_SUCESSED

        for nn in range(len(levels)-1):
            # 'mosaic_%s_%d_tmp.nc'%(timestr,nn+1) 
            outname = '%s%s_%d_tmp%s'%(aa[0],timestr,nn+1,aa[2])
            # It is divided into multiple processes. Each process calculates a piece of puzzle on height, and then splices it
            params={
            'rootpath':rootpath,  
            'outpath': outpath, 
            'radars':radars, 
            'origin_latitude':center_lat, 
            'origin_longitude':center_lon, 
            'mosaic_range':mosaic_range,
            'hor_reso':hor_reso,
            'ver_reso':ver_reso, 
            'bdebug':bshow_debug,
            'timestr':timestr, 
            'bot_z_lev':levels[nn] + ver_reso, 
            'top_z_lev':levels[nn+1], 
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

        # merge data
        
        newgrid = mosaic_merge(files = alloutfiles)
        # write to file
        if newgrid is not None:
            newgrid.write(mergename)
            print(mergename + ' saved!')
            

        else:
            print('merge error!')

        # delete temp files
        files = glob.glob(outpath + os.sep + '*tmp.nc')
        if len(files) >0:
            for file in files:
                os.remove(file)   
        
        # Delete the lock file and allow the program to run
        if os.path.exists('file_mosaic.lock'): os.remove('file_mosaic.lock')

        

    def do_archive(self,):

        st = datetime.strptime(self.starttime,'%Y%m%d%H%M')
        et = datetime.strptime(self.endtime,'%Y%m%d%H%M')
        periods = int(((et - st).total_seconds() / 60)/self.tstep)
        print('periods = %d'%periods)
        dt = pd.date_range(st,freq='%dmin'%self.tstep,periods=periods)
        dt = pd.to_datetime(dt)
        alltimes=[tt.strftime('%Y%m%d%H%M') for tt in dt]

        rootpath = self.input_path_archive
        outpath = self.output_path_archive

        radars = self.radars
        center_lat = self.center_lat
        center_lon = self.center_lon
        mosaic_range = self.mosaic_range
        hor_reso = self.hori_reso
        ver_reso = self.verti_reso
        bshow_debug = self.bshow_debuginfo
        breplace = self.breplace
        levels = self.multi_levels

        # levels  KM
        # levels = [0, 5, 9, 12, 16]
        # levels = [0, 7, 10, 13, 16, 20] 82s
        # levels = [0, 7, 10, 13, 16, 20]
        # MAXP = int(cpu_count()*0.2)

        
        for timestr in alltimes:
            allparams = []
            print('processing : ' + timestr)

            alloutfiles=[]
            mergename = outpath + os.sep +  'mosaic_%s.nc'%timestr
            if not breplace and os.path.exists(mergename):
                print(mergename + ' already exists!')
                continue

            for nn in range(len(levels)-1):

                outname = 'mosaic_%s_%d_tmp.nc'%(timestr,nn+1)
                
                params={
                
                'rootpath':rootpath,
               
                'outpath': outpath,
                
                'radars':radars, 
                'origin_latitude':center_lat,
                'origin_longitude':center_lon, 
                'mosaic_range':mosaic_range, 
                'hor_reso':hor_reso, 
                'ver_reso':ver_reso, 
                'bdebug':bshow_debug,
                'timestr':timestr, 
                'bot_z_lev':levels[nn] + ver_reso, 
                'top_z_lev':levels[nn+1], 
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

            # merge data
            
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
        
        
        return radarcommon.RETURN_CODE_SUCESSED