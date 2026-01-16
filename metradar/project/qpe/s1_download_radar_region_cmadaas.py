# _*_ coding: utf-8 _*_

"""
定时从天擎下载雷达基数据
@author: wenjianzhu
"""

# %%
import urllib3
import json as js
import os
import configparser
from datetime import datetime,timedelta
import time
import hashlib
import uuid
from nmc_met_io.retrieve_cmadaas import  cmadaas_radar_level2_by_timerange_and_id
#添加时间控制器
import sched
schedule = sched.scheduler(time.time, time.sleep)

# sub function for reading config file
def ConfigFetchError(Exception):
    pass

def _get_config_from_rcfile(rcfile):
    """
    Get configure information from config_dk_met_io.ini file.
    """

    try:
        config = configparser.ConfigParser()
        config.read(rcfile,encoding='UTF-8')
    except IOError as e:
        raise ConfigFetchError(str(e))
    except Exception as e:
        raise ConfigFetchError(str(e))

    return config


# 获取雷达基数据的代码如下：
def _download_radar_basedata(tstep,trange, staIds):
    
    # 获取当前UTC时间，构建时间范围，用于获取数据
    curtime = datetime.utcnow()
    pretime = curtime + timedelta(minutes=-1*trange)
    print("Search From: " + pretime.strftime("%Y-%m-%d %H:%M:%S") + " To: "+ curtime.strftime("%Y-%m-%d %H:%M:%S"))

    time_range = "[" + pretime.strftime("%Y%m%d%H%M%S") + "," + curtime.strftime("%Y%m%d%H%M%S") + "]"
        
    for rds in staIds:
        #创建输出路径    
        cmadaas_radar_level2_by_timerange_and_id(radar_ids=rds,time_range=time_range,outpath=outpath)

        
    print('Waiting for new data......')
    schedule.enter(tstep, 0, _download_radar_basedata, (tstep, trange, staIds,))

# 递归删除文件夹中的过期文件
def loop_delete_data(filepath,pretime):
    pass
    if os.path.exists(filepath):
        
        if os.path.isdir(filepath):
            ff = os.listdir(filepath)
            for f in ff:
                if os.path.isdir(filepath + os.sep + f):
                    loop_delete_data(filepath + os.sep + f,pretime)
                elif os.path.isfile(filepath + os.sep + f):
                    t = os.path.getctime(filepath + os.sep + f)
                    if t < pretime.timestamp():
                        os.remove(filepath + os.sep + f)
                        print(['Delete file:' + filepath + os.sep + f])

        elif os.path.isfile(filepath):
            t = os.path.getctime(filepath + os.sep + f)
            if t < pretime.timestamp():
                os.remove(filepath + os.sep + f)
                print(['Delete file:' + filepath + os.sep + f])
                

def _delete_old_data(tstep):

    print('delete expired data ...')
    curtime = datetime.now()
    pretime = curtime + timedelta(hours=-1 * int(data_save_hours))
    loop_delete_data(outpath,pretime)
    
    schedule.enter(tstep, 0, _delete_old_data, (tstep,))
     

if __name__=='__main__':
    with open('current_pid_s1.txt','wt') as f:
        f.write('current pid is: %s'%str(os.getpid()) + '  ,' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        
        
    config_file = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/metradar/project/qpe/main_qpe_cfg.ini'
    
    
    print(os.path.exists(config_file))
    config = _get_config_from_rcfile(config_file)
    staIds = config['RADAR_SITES']['RADARS'].split(',')
    dns = config['DAAS_SERVER']['DNS']
    port = config['DAAS_SERVER']['PORT']
    user_id = config['DAAS_SERVER']['USER_ID']
    pwd = config['DAAS_SERVER']['PASSWD']
    service_node_id = config['DAAS_SERVER']['SERVICE_NODE_ID']
    outpath = config['PATH_SETTING']['ROOT_PATH'] + os.sep + config['PATH_SETTING']['BASEDATA_PATH']
    data_code = config['DAAS_API_SETTING']['DATA_CODE']
    interface_id = config['DAAS_API_SETTING']['RADAR_API']
    elements = config['DAAS_API_SETTING']['ELEMENTS']
    q_tstep = config['QUIRY_SETTING']['QUIRY_STEP']
    q_trange = config['QUIRY_SETTING']['QUIRY_RANGE']
    data_save_hours = config['DATA_SAVE_SETTING']['DATA_SAVE_HOURS']
    # radarfile = config['PATH_SETTING']['RADARFILE']
        

    # 增加数据下载任务
    schedule.enter(0, 0, _download_radar_basedata, (int(q_tstep),int(q_trange),staIds,))    
    
    # 增加数据管理任务，定时删除旧文件
    schedule.enter(0, 0, _delete_old_data, (60,))

    # 增加程序自动重启任务，以解决可能存在的bug
    schedule.run()


# %%
