# _*_ coding: utf-8 _*_

'''
将单站qpe产品进行拼图处理
'''
# %%
import os
import numpy as np
import glob
from datetime import  datetime,timedelta
from netCDF4 import Dataset
import xarray as xr
import pickle
import warnings
warnings.filterwarnings('ignore')
import configparser
#添加时间控制器
import time
import sched
import sys
from multiprocessing import Pool,cpu_count,freeze_support

schedule = sched.scheduler(time.time, time.sleep)

BDEBUG=False

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
    ft = datetime(fyear,fmonth,fday,fhour,fmin,fsec).timestamp()
    return ft

# Z9973_20240903_160311_01hr.nc
def get_datetime_from_filename2(filename):
    timstr = filename[6:14] + filename[15:21]
    fyear = int(timstr[0:4])
    fmonth = int(timstr[4:6])
    fday = int(timstr[6:8])
    fhour = int(timstr[8:10])
    fmin = int(timstr[10:12])
    fsec = int(timstr[12:14])
    ft = datetime(fyear,fmonth,fday,fhour,fmin,fsec).timestamp()
    return ft



def do_single(curt:datetime):


    # if BDEBUG:
    #     curt = datetime(2021,5,8,8,20,0)
    # else:
    #     curt = datetime.utcnow()   

    try:
        if not os.path.exists(PATH_TMP):
            os.makedirs(PATH_TMP)

        for acc_hours in ACC_HOURS:
            print('当前累积时段 = %d 小时'%acc_hours)
            print(curt.strftime('当前执行时间为：%Y年%m月%d日%H时%M分(UTC)'))
            # acc_hours = 3
            # #降水产品格点设置
            # START_LAT= 25
            # END_LAT  = 30
            # START_LON= 109
            # END_LON  = 114
            # MOSAIC_RESO = 0.01 #网格分辨率，经纬度，度

            mosaic_lats = np.arange(START_LAT,END_LAT+MOSAIC_RESO/2,MOSAIC_RESO)
            mosaic_lons = np.arange(START_LON,END_LON+MOSAIC_RESO/2,MOSAIC_RESO)
            glons,glats = np.meshgrid(mosaic_lons,mosaic_lats)

            GRID_XNUM = len(mosaic_lons)
            GRID_YNUM = len(mosaic_lats)

            
            # 获取所有单站数据的时间戳
            allfiles = glob.glob(PATH_QPE + '/*/*')
            dic_radfile = {rad:[] for rad in RADARS}
            dic_radtime = {rad:[] for rad in RADARS}
            
            for rd in RADARS:
                curfilepath = PATH_QPE + os.sep + rd
                print(curfilepath)
                files = glob.glob(curfilepath + os.sep + '*%02dhr.nc'%acc_hours)
                files = [file[-29::] for file in files]
                files = sorted(files,reverse=True)
                dic_radfile[rd] = files
                for ff in files:
                    dic_radtime[rd].append(get_datetime_from_filename2(ff))

            # 把过去30分钟的数据，每间隔6分钟的都处理一遍
            cur_min = (curt.minute // MOSAIC_STEP)*MOSAIC_STEP
            vlid_t = datetime(curt.year,curt.month,curt.day,curt.hour,cur_min,0)
            checktimes = np.arange(vlid_t.timestamp(),vlid_t.timestamp()-REDO_MINS*60-1,-MOSAIC_STEP*60)

            mosaic_path = PATH_QPE_MOSAIC + os.sep + '%02dhr'%acc_hours
            if not os.path.exists(mosaic_path):
                try:
                    os.makedirs(mosaic_path)
                except:
                    print('创建目录失败：' + mosaic_path)

            for mn in checktimes:
                print(datetime.fromtimestamp(mn).strftime('正在执行 %Y年%m月%d日%H时%M分(UTC) 的拼图......'))
                oldlist=[]
                
                outname = 'QPE_MOSAIC_' + datetime.fromtimestamp(mn).strftime('%Y%m%d%H%M') + '_%02dhr.nc'%acc_hours
                # 先判断该拼图是否已经存在，并且里面的文件列表无更新，如果是，就不重复处理，直接跳过
                if os.path.exists(mosaic_path + os.sep + outname):
                    try:
                        tmplst = xr.open_dataset(mosaic_path + os.sep + outname)
                        oldlist = tmplst.basefile_info
                    except:
                        print('读取文件失败：' + mosaic_path + os.sep + outname)
                
                validfnum=0
                dic_validfile = {rad:[] for rad in RADARS}
                for rd in RADARS:
                    if len(dic_radtime[rd]) == 0:
                        # 删除该键值
                        dic_validfile.pop(rd)
                        # dic_validfile[rd] = []
                        # print(rd + ' 无数据！')
                        continue
                    else:
                        tmpt = list(abs(np.array(dic_radtime[rd]) - mn))
                        #只查找时间差在正负3分钟以内的文件
                        if min(tmpt) <= 180:
                            dic_validfile[rd] = dic_radfile[rd][tmpt.index(min(tmpt))]
                            print('valid file: ' + dic_validfile[rd])
                            validfnum +=1
                        else:
                            pass
                            # print(dic_radfile[rd])
                            # print('%s 最短时间差为：%.1f秒，超过了三分钟的匹配时限'%(rd,min(tmpt)))
                        
                #查到当前时间的所有雷达的匹配数据后，进行拼图处理
                if validfnum == 0:
                    print(datetime.fromtimestamp(mn).strftime('%Y年%m月%d日%H时%M分(UTC)') + ' 有效数据个数为0！')
                    continue
                #   
                mosaic_grid=np.zeros([len(dic_validfile),GRID_YNUM,GRID_XNUM],dtype=float)    
                # 依次读取单站qpe产品，然后拼图
                rflag=0
                # dic_fileinfo = {key:[] for key in dic_validfile.keys()}
                fileinfo_all = []
                bbflag=False
                for rd in dic_validfile.keys():
                    curfilepath = PATH_QPE + os.sep + rd
                    curfilename = dic_validfile[rd]
                    if len(curfilename) == 0:
                        continue
                    if not os.path.exists(curfilepath + os.sep + curfilename):
                        print(curfilepath + os.sep + curfilename + ' 文件不存在！')
                        continue
                    try:
                        # grid = pyart.io.read_grid(curfilepath + os.sep + curfilename)
                        grid = xr.open_dataset(curfilepath + os.sep + curfilename)
                        if not set(grid.basefile_info) <= set(oldlist):
                            bbflag = True
                            break
                    except:
                        print(curfilepath + os.sep + curfilename + ' 文件读取失败, continue！')
                        continue
                if not bbflag:
                    print(outname + ' 拼图文件所用原始文件无更新，不重复处理！')
                    continue

                used_sites=[]
                used_lons=[]
                used_lats=[]
                used_hours=[]

                for rd in dic_validfile.keys():
                    curfilepath = PATH_QPE + os.sep + rd
                    curfilename = dic_validfile[rd]
                    if len(curfilename) == 0:
                        continue
                    if not os.path.exists(curfilepath + os.sep + curfilename):
                        print(curfilepath + os.sep + curfilename + ' 文件不存在！')
                        continue
                    try:
                        # grid = pyart.io.read_grid(curfilepath + os.sep + curfilename)
                        grid = xr.open_dataset(curfilepath + os.sep + curfilename)
                    except:
                        print(curfilepath + os.sep + curfilename + ' 文件打开失败！')
                        continue
                    if set(grid.basefile_info) <= set(oldlist):
                        print('%s 站原始文件无变化: '%rd)

                    print('%s 站需制作新产品或原始文件有更新，处理：'%rd + curfilename + ' loaded!')
    
                    # check site location
                    # 先检查rd是否在ALL_RADARS的索引里面
                    if rd not in ALL_RADARS.index:
                        print('%s 站不在站号表文件radarsites_national.pkl中，请注意！'%rd)
                    
                    if abs(grid.radlon - ALL_RADARS.loc[rd]['经度']) > 0.1 or abs(grid.radlat - ALL_RADARS.loc[rd]['纬度']) > 0.1:
                        print('%s 站的经度或纬度有异常，基数据中的记录的位置和站点表中记录的不一致，误差超过了0.1度，请注意，这里暂时以数据中记录的为准！'%rd)
                        print('基数据中记录的经度为%.3f, 纬度为%.3f'%(grid.radar_longitude['data'][0],grid.radar_latitude['data'][0]))
                        print('站号表文件radarsites_national.pkl中记录的经度为%.3f, 纬度为%.3f'%(ALL_RADARS.loc[rd]['经度'],ALL_RADARS.loc[rd]['纬度']))

                    used_sites.append(rd)
                    used_lons.append(grid.radlon)
                    used_lats.append(grid.radlat)

                    for tmpff in grid.basefile_info:
                        fileinfo_all.append(tmpff)
                    used_hours.append(grid.used_hours) 
                    #组合
                    mosaic_grid[rflag,:,:] = grid['qpe_mosaic'].values
                    
                    rflag += 1

                mosaic_grid = np.nanmean(mosaic_grid,axis=0)

                # 输出到nc格式
                try:
                    ncfile = Dataset(mosaic_path + os.sep + outname + '.lock', 'w', format='NETCDF4')
                except:
                    try:
                        os.remove(mosaic_path + os.sep + outname+ '.lock')
                        try:
                            ncfile = Dataset(mosaic_path + os.sep + outname+ '.lock', 'w', format='NETCDF4')
                        except:
                            print(mosaic_path + os.sep + outname +  '.lock' + ' 文件创建失败，删除成功，但无法再新建，跳过！')
                            continue
                    except:
                        print(mosaic_path + os.sep + outname + '.lock' + ' 文件创建失败，并且无法通过程序删除旧的同名文件，程序跳过！')
                        continue

                hreso = MOSAIC_RESO
                        
                # global
                ncfile.description = 'RADAR QPE MOSAIC '
                # ncfile.author = 'Zhu WenJian, zhuwj@cma.gov.cn'
                ncfile.reso = hreso
                ncfile.start_lat = START_LAT
                ncfile.end_lat = END_LAT
                ncfile.start_lon = START_LON
                ncfile.end_lon = END_LON
                ncfile.product_time = datetime.fromtimestamp(mn).strftime('%Y%m%d%H%M') 
                ncfile.last_update = curt.strftime('%Y%m%d%H%M') 
                ncfile.basefile_info = list(fileinfo_all)
                ncfile.used_sites = list(used_sites)
                ncfile.used_lons = list(used_lons)
                ncfile.used_lats = list(used_lats)
                ncfile.used_hours = used_hours
                
                unused_sites = [ss for ss in RADARS if not ss in used_sites]
                ncfile.unused_sites = list(unused_sites)
                unused_lons = [ALL_RADARS.loc[ll]['经度'] for ll in unused_sites]
                unused_lats = [ALL_RADARS.loc[ll]['纬度'] for ll in unused_sites]
                ncfile.unused_lons = list(unused_lons)
                ncfile.unused_lats = list(unused_lats)

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
                qpe_mosaic.long_name = "QPE MOSAIC"
                qpe_mosaic.units='mm'
                # data
                qpe_mosaic[:,:] = mosaic_grid * 10
                longitude[:] = nlons
                latitude[:] = nlats

                ncfile.close()
                try:
                    os.rename(mosaic_path + os.sep + outname + '.lock',mosaic_path + os.sep + outname)
                except:
                    print(mosaic_path + os.sep + outname + ' 重命名失败！')

                print(mosaic_path + os.sep + outname + ' done!')
    except:
        print(mosaic_path + os.sep + outname + ' + do_single 失败，跳过！')

def get_time_range_from_filelist():
    #从数据目录里面获取时间范围，用于后续处理
    trange ={}
    # 对雷达进行遍历
    for rd in RADARS:
        curpath = PATH_QPE + os.sep + rd
        if not os.path.exists(curpath):
            continue
        ff = os.listdir(curpath)
        ft=[]
        for f in ff:
            if f[-3:] == 'nc':
                continue
            #从文件名中获取时间
            t = get_datetime_from_filename2(f)
            ft.append(t)
        #对ft进行排序
        ft.sort()
        trange[rd] = [ft[0],ft[-1]]
        #计算时间范围
        trange[rd].append(int((trange[rd][1]-trange[rd][0])/3600))
        print('Radar %s has %d files, start time is %s, end time is %s, time range is %d hours'%(rd,len(ft),datetime.fromtimestamp(trange[rd][0]).strftime('%Y%m%d%H%M'),datetime.fromtimestamp(trange[rd][1]).strftime('%Y%m%d%H%M'),trange[rd][2]))

    #对所有雷达的时间范围进行合并，并将tmin和tmax存为datetime类型

    tmin = trange[RADARS[0]][0]
    tmax = trange[RADARS[0]][1]
    for rd in RADARS:
        if trange[rd][0] < tmin:
            tmin = trange[rd][0]
        if trange[rd][1] > tmax:
            tmax = trange[rd][1]
    # 把tmin往前取整10分钟，把tmax往后取整10分钟
    tmin = tmin - tmin%600
    tmax = tmax + 600 - tmax%600
    # print
    print('All radar has %d files, start time is %s, end time is %s, time range is %d hours'%(len(ft),datetime.fromtimestamp(tmin).strftime('%Y%m%d%H%M'),datetime.fromtimestamp(tmax).strftime('%Y%m%d%H%M'),int((tmax-tmin)/3600)))

    return tmin,tmax


def do_mosaic_qpe(tstep=10):
    # ACC_HOURS = [3],
    # START_LAT= 25,
    # END_LAT  = 30,
    # START_LON= 109,
    # END_LON  = 114,
    # MOSAIC_RESO = 0.01,
    # RADARS = ['Z9734','Z9731','Z9739','Z9746'],
    # PATH_QPE = 'out_qpe', 
    # PATH_QPE_MOSAIC='qpe_mosaic',
    # PATH_TMP='tmp',
    # REDO_MINS=20,

    #如果是历史模式
    if RUNMODE == 1: # 对小时进行累计
        # timestamp类型
        startt,endt = get_time_range_from_filelist()
        # 按拼图间隔，从startt，处理到endt，具体采用do_single函数
        # 每个时刻单独分配一个进程，采用多进程处理
        _pools = Pool(CPU_MAX)
        t = startt
        allt=[]
        while t <= endt:
            #将t转换为datetime类型
            allt.append(datetime.fromtimestamp(t))
            t += MOSAIC_STEP*60
            
        _pools.map(do_single,allt)
        _pools.close()
        _pools.join()
    elif RUNMODE == 2: # 对分钟进行累计
        # timestamp类型
        startt,endt = get_time_range_from_filelist()
        # 按拼图间隔，从startt，处理到endt，具体采用do_single函数
        # 每个时刻单独分配一个进程，采用多进程处理
        _pools = Pool(CPU_MAX)
        t = startt
        allt=[]
        while t <= endt:
            #将t转换为datetime类型
            allt.append(datetime.fromtimestamp(t))
            t += MOSAIC_STEP*60
            
        _pools.map(do_single,allt)
        _pools.close()
        _pools.join()
    else:
        #将curt设置为当前UTC时间
        curt = datetime.utcnow()
        
        do_single(curt)
    
    if not BDEBUG:
        print('Waiting for new data......')
        schedule.enter(tstep, 0, do_mosaic_qpe, (tstep,))

def _delete_old_data(tstep):
    
    print('delete expired data ...')
    curtime = datetime.now()
    pretime = curtime + timedelta(hours=-1*int(data_save_hours))
    
    #查询过期数据
    # curpath = PATH_TMP
    # if not os.path.exists(curpath):
    #     return False
    # ff = os.listdir(curpath)
    # for f in ff:
    #     t = os.path.getctime(curpath + os.sep + f)
    #     if t < pretime.timestamp():
    #         os.remove(curpath + os.sep + f)
    #         print(['Delete file:' + curpath + os.sep + f])

    for acc_hours in ACC_HOURS:          
        curpath = PATH_QPE_MOSAIC + os.sep + '%02dhr'%acc_hours
        if not os.path.exists(curpath):
            return False
        ff = os.listdir(curpath)
        for f in ff:
            t = os.path.getctime(curpath + os.sep + f)
            if t < pretime.timestamp():
                os.remove(curpath + os.sep + f)
                print(['Delete file:' + curpath + os.sep + f])

    schedule.enter(tstep, 0, _delete_old_data, (tstep,))

if __name__ == "__main__":    
    
    print('Usage: python s4_mosaic_qpe.py inifile')
    # 所有参数（包含脚本名）
    all_args = sys.argv  
    # 实际参数（排除脚本名）
    inifile = sys.argv[1:]
    

    #如果未指定配置文件，则选用默认的配置
    if len(inifile) ==0:
        inifile = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/metradar/project/qpe/main_qpe_cfg.ini'
        # inifile = 'archive_main_qpe_cfg.ini'
    else:
        inifile = inifile[0]
    
    config = _get_config_from_rcfile(inifile)
    RUNMODE = int(config['COMMON']['RUN_MODE'])#X方向格点数
    if RUNMODE == 0:
        BDEBUG = False
    else:
        BDEBUG = True

    
    ROOT_PATH = config['PATH_SETTING']['ROOT_PATH']
    BASEDATA_PATH = ROOT_PATH + os.sep + config['PATH_SETTING']['BASEDATA_PATH']

    RADARS = config['RADAR_SITES']['RADARS'].split(',')
    PATH_QPE = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_QPE']
    PATH_QPE_MOSAIC = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_QPE_MOSAIC']
    PATH_TMP = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_TMP']
    data_save_hours = config['DATA_SAVE_SETTING']['DATA_SAVE_HOURS']
    ACC_HOURS = [int(mm) for mm in config['PARAMS']['acc_hours'].split(',')]
    if not os.path.exists(PATH_QPE_MOSAIC):
        try:
            os.makedirs(PATH_QPE_MOSAIC)
        except:
            print('创建目录失败：' + PATH_QPE_MOSAIC)
            
    CPU_MAX = int(cpu_count() * float(config['PARAMS']['CPU_RATE']))
    #降水产品格点设置
    MOSAIC_RESO = float(config['PARAMS']['MOSAIC_RESO']) #网格分辨率，度
    START_LAT = float(config['PARAMS']['SOUTH_LAT']) #南纬
    END_LAT = float(config['PARAMS']['NORTH_LAT']) #北纬
    START_LON = float(config['PARAMS']['WEST_LON']) #西经
    END_LON = float(config['PARAMS']['EAST_LON']) #东经

    REDO_MINS = int(config['PARAMS']['REDO_MINS'])
    MOSAIC_STEP = int(config['PARAMS']['MOSAIC_STEP'])

    if not os.path.exists('radarsites_national.pkl'):
        print('radarsites_national.pkl 文件不存在，程序退出！')
        exit(-1)
    f = open('radarsites_national.pkl','rb')
    ALL_RADARS = pickle.load(f)
    f.close()

    with open('current_pid_s4_%d.txt'%RUNMODE,'wt') as f:
        f.write('current pid is: %s'%str(os.getpid()) + '  ,' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    
    tstep=10
    
    if not BDEBUG:
        # 增加数据处理任务
        schedule.enter(0, 0, do_mosaic_qpe, (tstep,))    

        # 增加数据管理任务，定时删除旧文件
        schedule.enter(0, 0, _delete_old_data, (60,))

        # 增加程序自动重启任务，以解决可能存在的bug
        schedule.run()
    else:
        do_mosaic_qpe(tstep)

# %%
