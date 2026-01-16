# _*_ coding: utf-8 _*_


# 多进程绘图，提高历史模式下绘图效率

# %%绘图
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.transforms import offset_copy
from matplotlib.font_manager import FontProperties
import cartopy.crs as ccrs
import nmc_met_graphics.plot.mapview as nmc_draw
from netCDF4 import Dataset
import warnings

warnings.filterwarnings('ignore')

import matplotlib as mpl
from datetime import datetime,timedelta
from multiprocessing import Pool,Manager,freeze_support,cpu_count
import sys

from metradar.config import CONFIG

# 资源文件路径
RESOURCES_PATH = CONFIG.get('SETTING','RESOURCES_PATH')
FONT_FILE = RESOURCES_PATH + '/fonts/YaHeiConsolasHybrid_1.12.ttf'

BDEBUG = False

manager = Manager()
# 创建一个dict，装入每个qpe拼图的文件的名称和修改时间
shared_dict = manager.dict()

if not BDEBUG:
    # linux : export MPLBACKEND=Agg
    mpl.use('Agg')

import configparser
#添加时间控制器
import time
import sched
schedule = sched.scheduler(time.time, time.sleep)
# sub function for reading config file
def ConfigFetchError(Exception):
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
    timstr = filename[11:23]
    fyear = int(timstr[0:4])
    fmonth = int(timstr[4:6])
    fday = int(timstr[6:8])
    fhour = int(timstr[8:10])
    fmin = int(timstr[10:12])
    fsec = 0
    ft = datetime(fyear,fmonth,fday,fhour,fmin,fsec).timestamp()
    return ft

def draw_single(params: dict):
    filename = params['filename']
    pic_path = params['pic_path']
    mosaic_path = params['mosaic_path']
    acc_hours = params['acc_hours']
    print('process file: ' + filename + '  acc_hours: ' + str(acc_hours))
    if filename.find('%02dhr.nc'%acc_hours) < 0 or filename.find('.lock') > 0:
        return -2
    outname = filename.replace('.nc','.png')

    if not BDEBUG:
        pass
        # 只要超过20分钟就不重新绘制
        if (datetime.utcnow().timestamp() - get_datetime_from_filename1(filename)) > 20*60:
            return -1

        if os.path.exists(pic_path + os.sep + outname) and (datetime.utcnow().timestamp() - get_datetime_from_filename1(filename)) > 20*60 :
            # print(pic_path + os.sep + outname + ' 已存在，且过时20分钟，不重新绘制！')
            return -3

    if not os.path.exists(mosaic_path + os.sep + filename):
        print(mosaic_path + os.sep + filename + ' 文件不存在！')
        return -4

    try:
        mosaic_file = Dataset(mosaic_path + os.sep + filename,'r')
    except:
        print(mosaic_path + os.sep + filename + ' 文件读取失败！')
        return -5
    
    # 检测临时状态
    curt_file = datetime.fromtimestamp(os.path.getmtime(mosaic_path + os.sep + filename))
    if filename in list(shared_dict.keys()):
        if curt_file == shared_dict[filename]:
            # 修改时间无变化，那么就不画图
            print(filename + ' 无修改，不重复绘图！')
            return 1

    shared_dict[filename] = curt_file   
    mosaic_grid = mosaic_file.variables['qpe_mosaic'][:,:] * mosaic_file.variables['qpe_mosaic'].unit_scale
    used_hours = mosaic_file.used_hours
    used_sites = mosaic_file.used_sites
    used_lons = mosaic_file.used_lons
    used_lats = mosaic_file.used_lats
    unused_sites = mosaic_file.unused_sites
    unused_lons = mosaic_file.unused_lons
    unused_lats = mosaic_file.unused_lats

    # 
    #降水产品格点设置
    START_LAT= mosaic_file.start_lat
    END_LAT  = mosaic_file.end_lat
    START_LON= mosaic_file.start_lon
    END_LON  = mosaic_file.end_lon
    GRID_RESO = mosaic_file.reso #网格分辨率，经纬度，度

    mosaic_lats = np.arange(START_LAT,END_LAT+GRID_RESO/2,GRID_RESO)
    mosaic_lons = np.arange(START_LON,END_LON+GRID_RESO/2,GRID_RESO)
    
    m = nmc_draw.BaseMap(projection='PlateCarree',central_longitude=110.0,res='h')
    fig =plt.figure(figsize=(8,8))
    ax = plt.axes(projection=m.proj)
    ax.set_extent([START_LON, END_LON, START_LAT, END_LAT])
    mosaic_grid[mosaic_grid<0.5] = np.nan
    projection = ccrs.PlateCarree()

    # cmap = parse_cmap('pyart_NWSRef')
    colors_1 = [[183,245,167],[120,215,110],[65,185,60],[50,140,45],[100,185,255],[0,5,255],[255,0,255]]
    colors_2 = [[168,242,142],[65,185,60],[100,185,255],[0,5,255],[255,0,255],[128,0,64]]
    levs_1 = [0,2.5,5,10,25,50,100,999]
    levs_2 = [0,10,25,50,100,250,999]
    if acc_hours < 3:
        colors = np.array(colors_1)/255
        levs = levs_1
    else:
        colors = np.array(colors_2)/255
        levs = levs_2
    cmap = mpl.colors.ListedColormap(colors, 'precipitation')
    pm = m.contourf(mosaic_lons, mosaic_lats, mosaic_grid[:,:],ax=ax,colors =colors,levels=levs)
    pm.set_clim(0,999)
    nmc_draw.add_china_map_2cartopy(ax, name='county', edgecolor='darkgray', lw=0.5)
    nmc_draw.add_china_map_2cartopy(ax, name='province', edgecolor='k', lw=1)
    
    tt1 = datetime.strptime(mosaic_file.product_time, '%Y%m%d%H%M') + timedelta(hours=8)
    tt2 = datetime.strptime(mosaic_file.last_update, '%Y%m%d%H%M') + timedelta(hours=8)
    m.title(left_title = '%d hours Radar QPE(%s)'%(acc_hours,tt1.strftime('%Y%m%d%H%M')), ax=ax, font_size=13)
    m.title(right_title = 'Last update:%s'%tt2.strftime('%Y%m%d%H%M'), ax=ax, font_size=13)
    m.gridlines()
    # m.cities()
    if np.array(used_sites).size > 0:
        ax.plot(used_lons, used_lats, marker='o', color='red', markersize=5, linewidth=0,
                alpha=0.7, transform=ccrs.PlateCarree())
        font = FontProperties(size=8, weight='bold')
        geodetic_transform = ccrs.Geodetic()._as_mpl_transform(ax)
        text_transform = offset_copy(geodetic_transform, units='dots', x=0,y=5)
        if np.array(used_hours).size == 1:
            ax.text(used_lons, used_lats, str(used_hours) + 'hr',verticalalignment='bottom', horizontalalignment='center',
                transform=text_transform, fontproperties=font, color='black')
        else:
            for bbx in range(np.array(used_hours).size):
                ax.text(used_lons[bbx], used_lats[bbx], str(used_hours[bbx]) + 'hr',verticalalignment='bottom', horizontalalignment='center',
                    transform=text_transform, fontproperties=font, color='black')


    if np.array(unused_sites).size > 0:
        ax.plot(unused_lons, unused_lats, marker='o', color='black', markersize=6, linewidth=0,
                alpha=0.7, transform=ccrs.PlateCarree())
    m.colorbar(pm)
    plt.savefig( pic_path + os.sep + outname,dpi=150)
    # 防止内存泄露
    plt.close()
    print(pic_path + os.sep + outname + ' done!')

    return 0
    
def drawpic(tstep=10):

    for acc_hours in ACC_HOURS:
        pic_path = pic_path_ori + os.sep + '%02dhr'%acc_hours
        if not os.path.exists(pic_path):
            os.makedirs(pic_path)
        mosaic_path = mosaic_path_ori + os.sep + '%02dhr'%acc_hours
        if not os.path.exists(mosaic_path):
            print(mosaic_path + ' 路径不存在！')
            continue
        files = os.listdir(mosaic_path)
        files = sorted(files)
        if len(files) == 0:
            continue
        all_pms =[]
        for filename in files:
            params = {}
            params['filename'] = filename
            params['pic_path'] = pic_path
            params['mosaic_path'] = mosaic_path
            params['acc_hours'] = acc_hours
            all_pms.append(params)

        # 构建多进程pool
        freeze_support()
        cores = CPU_MAX
        if cores > len(files):
            cores = len(files)
        print('总共采用 %d 个核心，处理 %d 个文件'%(cores,len(files)))
        pool = Pool(processes = cores)
        pool.map(draw_single, all_pms) 
        pool.close()
        pool.join()

    if not BDEBUG: # 实时模式
        print('Waiting for new data......')
        schedule.enter(tstep, 0, drawpic, (tstep,))

def _delete_old_data(tstep):
    
    print('delete expired data ...')
    curtime = datetime.now()
    pretime = curtime + timedelta(hours=-1*int(data_save_hours))
    
    #查询过期数据
    
    for acc_hours in ACC_HOURS:          
        curpath = pic_path_ori + os.sep + '%02dhr'%acc_hours
        if not os.path.exists(curpath):
            return False
        ff = os.listdir(curpath)
        for f in ff:
            t = os.path.getctime(curpath + os.sep + f)
            if t < pretime.timestamp():
                os.remove(curpath + os.sep + f)
                print(['Delete file:' + curpath + os.sep + f])

    schedule.enter(tstep, 0, _delete_old_data, (tstep,))

if __name__ == '__main__':
    
    print('Usage: python s5_draw_qpe_mosaic.py inifile')
    # 所有参数（包含脚本名）
    all_args = sys.argv  
    # 实际参数（排除脚本名）
    inifile = sys.argv[1:]
    

    #如果未指定配置文件，则选用默认的配置
    if len(inifile) ==0:
        inifile = 'qpe/main_qpe_cfg.ini'
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
    ACC_HOURS = [int(mm) for mm in config['PARAMS']['acc_hours'].split(',')]
    mosaic_path_ori = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_QPE_MOSAIC']
    pic_path_ori = ROOT_PATH + os.sep + config['GRAPH']['PIC_PATH']
    DPI = int(config['GRAPH']['PIC_DPI'])
    data_save_hours = config['DATA_SAVE_SETTING']['DATA_SAVE_HOURS']
    CPU_MAX = int(cpu_count() * float(config['PARAMS']['CPU_RATE']))

    
    with open('current_pid_s5.txt','wt') as f:
        f.write('current pid is: %s'%str(os.getpid()) + '  ,' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    import matplotlib as mpl
    import shutil

    # 获取 Matplotlib 字体目录
    font_dir = mpl.get_data_path() + '/fonts/ttf/'
    # 复制字体文件（需管理员权限）
    shutil.copy(FONT_FILE, font_dir)
    cache_dir = mpl.get_cachedir()
    shutil.rmtree(cache_dir) 

    if not BDEBUG:
        # 重复的时间间隔
        tstep = 10 # seconds

        # 增加数据处理任务
        schedule.enter(0, 0, drawpic, (tstep,))    

        # 增加数据管理任务，定时删除旧文件
        schedule.enter(0, 0, _delete_old_data, (60,))

        schedule.run()
    else:
        drawpic(10)