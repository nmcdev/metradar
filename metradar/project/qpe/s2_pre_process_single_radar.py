# _*_ coding: utf-8 _*_

'''
定时扫描本地文件夹中的雷达基数据，并处理成单站qpe，用nc格式存储
'''

# %%
import os
from metradar.project.qpe.get_rainrate_func import get_rainrate
from datetime import datetime,timedelta
import sys
import configparser
import warnings
warnings.filterwarnings('ignore')
from multiprocessing import Pool,cpu_count,freeze_support
#添加时间控制器
import time
import sched
schedule = sched.scheduler(time.time, time.sleep)

BDEBUG=False
# sub function for reading config file
def ConfigFetchError(BaseException):
    pass

def _get_config_from_rcfile(rcfile):
    """
    Get configure information from config_dk_met_io.ini file.
    """
    # print(os.getcwd())
    print(rcfile)
    try:
        config = configparser.ConfigParser()
        config.read(rcfile,encoding='UTF-8')
    except IOError as e:
        raise ConfigFetchError(str(e))
    except Exception as e:
        raise ConfigFetchError(str(e))

    return config

def process_single(tstep=10):
    
    if not os.path.exists(PATH_RR):
        os.makedirs(PATH_RR)
    
    pools = Pool(CPU_MAX)
    print('共使用 %d 个逻辑CPU进行并行处理......'%CPU_MAX)
    for rd in RADARS:
       
       # 这里要改为递归查找文件，目录结构是ROOT_PATH ，年，月，日，站号
       
       # 递归处理所有子文件夹中的文件
        allfiles = []
        allpaths = []
        for root, dirs, files in os.walk(ROOT_PATH  + os.sep + BASEDATA_PATH):
            for file in files:
                if file.endswith('.bz2'):
                    if file.find(rd) < 0:
                        continue
                    allfiles.append(file)
                    allpaths.append(os.path.join(root,file))
                    
        curoutpath = PATH_RR + os.sep + rd
        if not os.path.exists(curoutpath):
            os.makedirs(curoutpath)

        allfiles = sorted(allfiles)

        for ff in allfiles:
            if ff.find('FMT.bin') < 0:
                allfiles.remove(ff)
                continue 
        
        bfiles_valid=0
        for filename in allfiles:
            #先判断该文件的qpe产品是否存在，如果已经存在就不重复处理，否则就处理
            outname = filename.replace('.bz2','_rr.nc')
            if not os.path.exists(curoutpath + os.sep + outname):
                bfiles_valid += 1
        if bfiles_valid > 0:
            print('%s 站，共找到 %d 个待处理的新数据，正在处理，请耐心等待......'%(rd,bfiles_valid))
        else:
            print('%s 站，共找到 %d 个待处理的新数据'%(rd,bfiles_valid))

        for nn in range(len(allfiles)):
            #先判断该文件的qpe产品是否存在，如果已经存在就不重复处理，否则就处理

            outname = allfiles[nn].replace('.bz2','_rr.nc')
            
            if os.path.exists(curoutpath + os.sep + outname):
                pass
                # print(curoutpath + os.sep + outname + ' 已存在，不重复处理！')
            else:        
                # print('正在处理 '+ filename)
                pools.apply_async(get_rainrate,(allpaths[nn],allfiles[nn],curoutpath,outname,GRID_XNUM,GRID_YNUM,GRID_RESO))
 
    pools.close()
    pools.join()
    if not BDEBUG:
        print('Waiting for new data......')
        schedule.enter(tstep, 0, process_single, (tstep,))


def _delete_old_data(tstep):
    
    print('delete expired data ...')
    curtime = datetime.now()
    pretime = curtime + timedelta(hours=-1 * int(data_save_hours))
    for rds in RADARS:
        #查询过期数据
        curpath = PATH_RR + os.sep + rds + os.sep
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
    freeze_support()
    print('Usage: python s2_pre_process_single_radar.py inifile')
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
    # BASEDATA_PATH =  ROOT_PATH + os.sep + config['PATH_SETTING']['BASEDATA_PATH']
    BASEDATA_PATH =  config['PATH_SETTING']['BASEDATA_PATH']

    RADARS = config['RADAR_SITES']['RADARS'].split(',')

    PATH_RR = ROOT_PATH + os.sep + config['PATH_SETTING']['PATH_RR']

    data_save_hours = config['DATA_SAVE_SETTING']['DATA_SAVE_HOURS']
    #降水产品格点设置
    GRID_XNUM = int(config['PARAMS']['GRID_XNUM'])#X方向格点数
    GRID_YNUM = int(config['PARAMS']['GRID_YNUM']) #Y方向格点数
    GRID_RESO = int(config['PARAMS']['GRID_RESO']) #网格分辨率，米
    CPU_MAX = int(cpu_count() * float(config['PARAMS']['CPU_RATE']))
    
    with open('current_pid_s2_%d.txt'%RUNMODE,'wt') as f:
        f.write('current pid is: %s'%str(os.getpid()) + '  ,' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    # 重复的时间间隔
    tstep = 10 # seconds

    if not BDEBUG:
        # 增加数据处理任务
        schedule.enter(0, 0, process_single, (tstep,))    

        # 增加数据管理任务，定时删除旧文件
        schedule.enter(0, 0, _delete_old_data, (60,))


        schedule.run()
    else:
        process_single(tstep)
            


# %%