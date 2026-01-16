
# _*_ coding: utf-8 _*_


'''
将雨强文件转换为定量降水产品

'''

import os
from datetime import datetime,timedelta
from typing import Dict
import numpy as np
import pyart 
import configparser
import warnings
import scipy.interpolate
warnings.filterwarnings('ignore')
import pickle
from netCDF4 import Dataset
from threading import Thread
from multiprocessing import cpu_count
#添加时间控制器
import time
import sched
import sys
schedule = sched.scheduler(time.time, time.sleep)

from multiprocessing import Pool,cpu_count,freeze_support

BDEBUG = False
# sub function for reading config file
def ConfigFetchError(BaseException):
    pass

def _get_config_from_rcfile(rcfile):
    """
    Get configure information from config_dk_met_io.ini file.
    """

    print(rcfile)
    try:
        config = configparser.ConfigParser()
        config.read(rcfile,encoding='UTF-8')
    except IOError as e:
        raise ConfigFetchError(str(e))
    except Exception as e:
        raise ConfigFetchError(str(e))

    return config

def get_datetime_from_filename1(filename):
    timstr = filename[15:29]
    fyear = int(timstr[0:4])
    fmonth = int(timstr[4:6])
    fday = int(timstr[6:8])
    fhour = int(timstr[8:10])
    fmin = int(timstr[10:12])
    fsec = int(timstr[12:14])
    ft = datetime(fyear,fmonth,fday,fhour,fmin,fsec)
    return ft

class MyThread(Thread):
 
  def __init__(self, fullfilepath):
    Thread.__init__(self)
    self.filepath = fullfilepath
 
  def run(self):
    self.result = pyart.io.read_grid(self.filepath)
 
  def get_result(self):
    return self.result

def do_single(params:Dict):
    # 历史模式下，需要将所有文件都处理
    curt = params['curt']
    rd = params['rd']
    acc_hours = params['acc_hours']
    allfiles = params['allfiles']
    curoutpath = params['curoutpath']
    GRID_SHAPE = params['GRID_SHAPE']
    GRID_LIMITS = params['GRID_LIMITS']
    mosaic_lons = params['mosaic_lons']
    mosaic_lats = params['mosaic_lats']
    glons = params['glons']
    glats = params['glats']
    print(rd + ' 站当前最新的数据时间为: ' + curt.strftime('%Y-%m-%d %H:%M:%S'))
    td = timedelta(hours=acc_hours)
    pret = curt - td
    files=[]
    for ff in allfiles:
        ft = get_datetime_from_filename1(ff)
        if ft < pret or ft > curt:
            continue
        else:
            files.append(ff)
            # print(ff)

    outrain = np.zeros([len(files),GRID_XNUM,GRID_YNUM],dtype=float)

    # 定义时间戳列表
    tlist = []
    valid_filenum = 0
    rad_lon = 0
    rad_lat = 0
    rad_alti = 0
    rad_name = ''
    
    for filename in files:
        # print(filename)
        try:
            grid = pyart.io.read_grid(PATH_RR + os.sep + rd + os.sep + filename)
            pass
        except:
            print(PATH_RR + os.sep + rd + os.sep + filename + '读取失败，暂不处理！')
            try:
                os.remove(PATH_RR + os.sep + rd + os.sep + filename)
                print(PATH_RR + os.sep + rd + os.sep + filename + ' removed!')
            except:
                print('delete file failed! ' + filename)
                pass
            files.remove(filename)
            continue
        outrain[valid_filenum,:,:] = grid.fields['radar_estimated_rain_rate']['data'][0,:,:]
        dateTime_p = datetime.strptime(grid.time['units'][14::],'%Y-%m-%dT%H:%M:%SZ')
        tlist.append(dateTime_p)
        rad_lon = grid.radar_longitude['data']
        rad_lat = grid.radar_latitude['data']
        rad_alti = grid.radar_altitude['data']
        # rad_name = grid.metadata['instrument_name']
        valid_filenum +=1
        
    # 将最后一个文件的文件名中的时间作为当前站点qpe的时间时，不可以执行下一行    
    # tlist.append(curt) # 最后一个文件的权重由该文件的时间到当前规定的累积截止时间之间的差值
    # print('%s 站，有效文件数为: %d ,本站处理结束！'%(rd,valid_filenum))   
    # print('%s 站共找到 %d 个有效文件'%(rd,valid_filenum))
    outfilename = rd + '_' + curt.strftime('%Y%m%d_%H%M%S_') + '%02dhr.nc'%acc_hours

    curn = rd + '_%02dhr'%acc_hours + curt.strftime('_%Y%m%d_%H%M%S')
    bneed_process = True
    if os.path.exists(curoutpath + os.sep + outfilename):
        #如果目标文件已经存在，那么check它的源数据是否有更新，如果有更新，那么重新累加，如果没有，那么不累加，直接跳过

        if os.path.exists(PATH_TMP + os.sep + curn ):
            if os.path.getsize(PATH_TMP + os.sep + curn ) == 0:
                raise ValueError(f"文件 {PATH_TMP + os.sep + curn } 为空")
            try:
                f = open(PATH_TMP + os.sep + curn,'rb')
                dic_info = pickle.load(f)
                f.close()
                print()
            except ValueError as e:
                print(f"读取文件 {PATH_TMP + os.sep + curn } 时出错: {e}")
                dic_info = None
                
        if isinstance(dic_info,str):
            if set(tlist) == set(dic_info):
                print(curn + ' 源数据无变化，不重复处理！')
                bneed_process = False
            else:
                pass
        elif isinstance(dic_info,list):
           
            if set(tlist) == set(dic_info):
                print(curn + ' 源数据无变化，不重复处理！')
                bneed_process = False
            else:
                pass
        
    else:
        pass
    # 如果无需重复处理，就直接continue，进入下一站
    if not bneed_process:
        return True
    f = open(PATH_TMP + os.sep + curn,'wb')
    pickle.dump(tlist,f)
    f.close()
    #计算时间权重
    dt = list(np.diff(tlist))
    
    # 插入第一个文件的时间权重，限定一个范围，不要超过6分钟
    
    dt.insert(0,min(timedelta(seconds = acc_hours * 3600 - round(np.sum([gk.seconds for gk in dt]))),timedelta(seconds=360)))
    
    # print(dt)

    acs = 0
    wgts = []
    for tt in dt:
        acs += tt.seconds
        wgts.append(tt.seconds / (1* 3600)) # 由于计算的是小时雨强，因此，这里算权重的时候只能按1小时来算，即3600秒
    if acs > (acc_hours * 3600):
        # print('累积时段和设置的不一样，设置的时段是 %d 小时，当前计算的是 %.1f 小时'%(acc_hours,acs/3600))
        print('累积时段超过了设置的规定时段，设置的时段是 %d 小时，当前累积时长是 %.2f 小时'%(acc_hours,acs/3600))
    # print(wgts)


    # 累加降水
    acc_rain = np.zeros([GRID_XNUM,GRID_YNUM],dtype=float)

    for fn in np.arange(0,valid_filenum):
        acc_rain += np.reshape(outrain[fn],[GRID_XNUM,GRID_YNUM])*wgts[fn]

    

    # 将rain插值到大的网格上，方便下一步直接调用
    nz, ny, nx = GRID_SHAPE
    (z0, z1), (y0, y1), (x0, x1) = GRID_LIMITS
    g_lons,g_lats = pyart.core.transforms.cartesian_to_geographic_aeqd(
                                                            np.linspace(x0, x1, nx),
                                                            np.linspace(y0, y1, ny),
                                                            rad_lon,
                                                            rad_lat)
    mesh_lons,mesh_lats = np.meshgrid(g_lons,g_lats)
    mosaic_grid  = scipy.interpolate.griddata((mesh_lons.flatten(),mesh_lats.flatten()),acc_rain.flatten(),(glons,glats))

    # %% 输出到nc格式
    try:
        ncfile = Dataset(curoutpath + os.sep + outfilename + '.lock', 'w', format='NETCDF4')
    except:
        if os.path.exists(curoutpath + os.sep + outfilename+ '.lock'): 
            try:
                os.remove(curoutpath + os.sep + outfilename+ '.lock')
                ncfile = Dataset(curoutpath + os.sep + outfilename+ '.lock', 'w', format='NETCDF4')
            except:
                print(curoutpath + os.sep + outfilename +  '.lock' + ' 文件创建失败，删除成功，但无法再新建，跳过！')
                return False
        else:
            print(curoutpath + os.sep + outfilename + '.lock' + ' 文件创建失败，程序跳过！')
            return False
    
    used_hours = (curt.timestamp() - get_datetime_from_filename1(files[0]).timestamp())/3600
    used_hours = float('%.1f'%used_hours)

    hreso = MOSAIC_RESO
            
    # global
    ncfile.description = 'SINGLE RADAR QPE MOSAIC'
    ncfile.author = 'Zhu WenJian, kevin2075@163.com'
    ncfile.reso = hreso
    ncfile.sitenum = rd
    ncfile.radlon = rad_lon
    ncfile.radlat = rad_lat
    ncfile.start_lat = START_LAT
    ncfile.end_lat = END_LAT
    ncfile.start_lon = START_LON
    ncfile.end_lon = END_LON
    ncfile.product_time = curt.strftime('%Y%m%d%H%M') 
    ncfile.last_update = datetime.utcnow().strftime('%Y%m%d%H%M') 
    ncfile.basefile_info = files
    ncfile.used_hours = used_hours
    ncfile.acc_hours = acc_hours

    # dimensions
    nlons = mosaic_lons
    nlats = mosaic_lats

    ncfile.createDimension('longitude', len(nlons))
    ncfile.createDimension('latitude', len(nlats))

    # variables
    longitude = ncfile.createVariable('longitude', np.dtype('float32').char, ('longitude',))
    longitude.units = 'degrees_east'
    longitude.long_name = 'Longitude'
    longitude.CoordinateAxisType = 'Lon'

    latitude = ncfile.createVariable('latitude', np.dtype('float32').char, ('latitude',))
    latitude.units = 'degrees_north'
    latitude.long_name = 'Latitude'
    latitude.CoordinateAxisType = 'Lat'

    # H	unsigned short
    qpe_mosaic = ncfile.createVariable('qpe_mosaic', 'H', ('latitude', 'longitude'), zlib=True, least_significant_digit=1)
    qpe_mosaic.unit_scale = 0.1
    qpe_mosaic.long_name = "QPE MOSAIC SINGLE RADAR"
    qpe_mosaic.units='mm'
    # data
    qpe_mosaic[:,:] = mosaic_grid * 10
    longitude[:] = nlons
    latitude[:] = nlats

    ncfile.close()
    os.rename(curoutpath + os.sep + outfilename + '.lock',curoutpath + os.sep + outfilename)
    print(curoutpath + os.sep + outfilename + ' done!')
    return 0


def sub_func(params:tuple):
    acc_hours,rd = params
    curfilepath = PATH_RR + os.sep + rd
    curoutpath = PATH_QPE + os.sep + rd + os.sep

    GRID_SHAPE = (1,GRID_XNUM, GRID_YNUM)
    GRID_LIMITS = ((GRID_RESO, GRID_RESO), (-1000*(GRID_XNUM-1)/2, 1000*(GRID_XNUM-1)/2), (-1000*(GRID_XNUM-1)/2, 1000*(GRID_XNUM-1)/2))


    mosaic_lats = np.arange(START_LAT,END_LAT+MOSAIC_RESO/2,MOSAIC_RESO)
    mosaic_lons = np.arange(START_LON,END_LON+MOSAIC_RESO/2,MOSAIC_RESO)
    glons,glats = np.meshgrid(mosaic_lons,mosaic_lats)

    if not os.path.exists(curoutpath):

        os.makedirs(curoutpath,exist_ok=True)

    allfiles = os.listdir(curfilepath)
    allfiles = sorted(allfiles)
    if len(allfiles) == 0:
        return False
    for ff in allfiles:
        if ff.find('.nc') < 0 or ff.find('.lock') > 0:
            allfiles.remove(ff)
      
    # print('%s 站共找到 %d 个文件.'%(rd,len(allfiles)))

    # 开始累积降水
    # 将最后一个文件的文件名中的时间作为当前站点qpe的时间
    #设置当前截止时间
    
    #实时模式下，用最新的即可
    if RUNMODE ==0:
        curttime = get_datetime_from_filename1(allfiles[-1])
        params={}
        params['curt'] = curttime
        params['rd'] = rd
        params['acc_hours'] = acc_hours
        params['allfiles'] = allfiles
        params['curoutpath'] = curoutpath
        params['GRID_SHAPE'] = GRID_SHAPE
        params['GRID_LIMITS'] = GRID_LIMITS
        params['mosaic_lons'] = mosaic_lons
        params['mosaic_lats'] = mosaic_lats
        params['glons'] = glons
        params['glats'] = glats
        
        do_single(params)
    #历史模式下，需要将所有文件都处理
    else:
        allpms=[]
        freeze_support()
        pools = Pool(CPU_MAX)
        for f in allfiles:
            curt = get_datetime_from_filename1(f)
            params={}

            params['curt'] = curt
            params['rd'] = rd
            params['acc_hours'] = acc_hours
            params['allfiles'] = allfiles
            params['curoutpath'] = curoutpath
            params['GRID_SHAPE'] = GRID_SHAPE
            params['GRID_LIMITS'] = GRID_LIMITS
            params['mosaic_lons'] = mosaic_lons
            params['mosaic_lats'] = mosaic_lats
            params['glons'] = glons
            params['glats'] = glats
            # pools.apply_async(do_single,(params,))
            allpms.append(params)
        pools.map(do_single,allpms)
        pools.close()
        pools.join()

def get_qpe_from_rr(tstep=10):
    #设置累积时段
    # acc_hours = 3
    # RADARS = ['Z9734','Z9731','Z9739','Z9746']
    # PATH_RR = 'rain_rate'
    # PATH_QPE = 'out_qpe'
    #降水产品格点设置
    # GRID_XNUM = 701 #X方向格点数
    # GRID_YNUM = 701 #Y方向格点数
    # GRID_RESO = 1000 #网格分辨率，米

    # 用一个状态变量记录当前处理的数据相关信息，便于对比，减少重复计算量
    
  
    if len(ACC_HOURS)==0:
        print('累计时段为0小时，程序退出！')
        return False
    
    # 如果是实时模式
    if RUNMODE == 0:
        params=[]
        for acc_hours in ACC_HOURS: 
            for rd in RADARS:
                params.append((acc_hours,rd))

        freeze_support()
        print("CPUS = %d"%CPU_MAX)
        print('正在启用%d个CPU进行并行计算，等待保存结果......'%CPU_MAX)
        pool = Pool(CPU_MAX)
        pool.map(sub_func,params)
    else:
        for acc_hours in ACC_HOURS: 
            for rd in RADARS:
                sub_func((acc_hours,rd))
            

    if not BDEBUG:
        print('Waiting for new data......')
        schedule.enter(tstep, 0, get_qpe_from_rr, (tstep,))  
    else:
        pass

def _delete_old_data(tstep):
    
    print('delete expired data ...')
    curtime = datetime.now()
    pretime = curtime + timedelta(hours=-1 * int(data_save_hours))
    for rds in RADARS:
        #查询过期数据
        curpath = PATH_QPE + os.sep + rds + os.sep
        if not os.path.exists(curpath):
            continue
        ff = os.listdir(curpath)
        for f in ff:
            t = os.path.getctime(curpath + os.sep + f)
           
            if t < pretime.timestamp():
                os.remove(curpath + os.sep + f)
                print(['Delete file:' + curpath + os.sep + f])
            
       
    schedule.enter(tstep, 0, _delete_old_data, (tstep,))

if __name__ == '__main__':

    print('Usage: python s3_trans_rainrate_to_qpe.py inifile')
    # 所有参数（包含脚本名）
    all_args = sys.argv  
    # 实际参数（排除脚本名）
    inifile = sys.argv[1:]
    

    #如果未指定配置文件，则选用默认的配置
    if len(inifile) ==0:
        inifile = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/metradar/project/qpe/main_qpe_cfg.ini'
    else:
        inifile = inifile[0]
    
    config = _get_config_from_rcfile(inifile)
    RUNMODE = int(config['COMMON']['RUN_MODE'])#X方向格点数
    if RUNMODE == 0:
        BDEBUG = False
    else:
        BDEBUG = True

    ROOT_PATH = config['PATH_SETTING']['ROOT_PATH']
    CPU_MAX = int(cpu_count() * float(config['PARAMS']['CPU_RATE']))
    RADARS = config['RADAR_SITES']['RADARS'].split(',')

    PATH_RR = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_RR']
    PATH_QPE = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_QPE']
    PATH_TMP = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_TMP']

    if not os.path.exists(PATH_QPE):
        try:
            os.makedirs(PATH_QPE)
        except:
            print('创建目录失败：' + PATH_QPE)
    if not os.path.exists(PATH_TMP):
        try:
            os.makedirs(PATH_TMP)
        except:
            print('创建目录失败：' + PATH_TMP)

    #降水产品格点设置
    GRID_XNUM = int(config['PARAMS']['GRID_XNUM'])#X方向格点数
    GRID_YNUM = int(config['PARAMS']['GRID_YNUM']) #Y方向格点数
    GRID_RESO = int(config['PARAMS']['GRID_RESO']) #网格分辨率，米

    ACC_HOURS = [int(mm) for mm in config['PARAMS']['acc_hours'].split(',')]
    data_save_hours = config['DATA_SAVE_SETTING']['DATA_SAVE_HOURS']


    # 输出的大网格设置
    MOSAIC_RESO = float(config['PARAMS']['MOSAIC_RESO']) #网格分辨率，度
    START_LAT = float(config['PARAMS']['SOUTH_LAT']) #南纬
    END_LAT = float(config['PARAMS']['NORTH_LAT']) #北纬
    START_LON = float(config['PARAMS']['WEST_LON']) #西经
    END_LON = float(config['PARAMS']['EAST_LON']) #东经

    with open('current_pid_s3_%d.txt'%RUNMODE,'wt') as f:
        f.write('current pid is: %s'%str(os.getpid()) + '  ,' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    # 重复的时间间隔
    tstep = 10 # seconds

    if not BDEBUG:
        # 增加数据处理任务
        schedule.enter(0, 0, get_qpe_from_rr, (tstep,))    

        # 增加数据管理任务，定时删除旧文件
        schedule.enter(0, 0, _delete_old_data, (60,))

        schedule.run()
    else:
        get_qpe_from_rr(10)
    # get_qpe_from_rr(10,acc_hours,RADARS,PATH_RR,PATH_QPE,GRID_XNUM,GRID_YNUM,GRID_RESO)