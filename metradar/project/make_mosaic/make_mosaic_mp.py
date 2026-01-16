# _*_ coding: utf-8 _*_


'''
make radarmosaic
wenjianzhu


'''

# %%
import os
from datetime import datetime,timedelta
import glob
import schedule
import time
import threading
from metradar.project.make_mosaic.make_mosaic_func import MAKE_RADAR_MOSAIC


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

if __name__ == "__main__":

    print('starting mosaic program......')
    with open('current_pid_mosaic.txt','wt') as f:
        f.write('当前进程号为: %s'%str(os.getpid()) + '  ,' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    # 路径需要根据实际情况进行修改
    _make_mosaic = MAKE_RADAR_MOSAIC('make_mosaic_mp.ini')

    if not _make_mosaic.berror:

        if _make_mosaic.run_mode == 'realtime':
            # Delete the lock file and allow the program to run
            if os.path.exists('file_mosaic.lock'): os.remove('file_mosaic.lock')
            
            if not os.path.exists(_make_mosaic.output_path_realtime):
                os.makedirs(_make_mosaic.output_path_realtime)

            if not os.path.exists(_make_mosaic.output_path_archive):
                os.makedirs(_make_mosaic.output_path_archive)

            # delete tmp.nc
            # delete temp files
            files = glob.glob(_make_mosaic.output_path_realtime + os.sep + '*tmp.nc')
            if len(files) >0:
                for file in files:
                    os.remove(file)   

            files = glob.glob(_make_mosaic.output_path_archive + os.sep + '*tmp.nc')
            if len(files) >0:
                for file in files:
                    os.remove(file) 

            # create task
            run_threaded(_make_mosaic.do_realtime)
            schedule.every(_make_mosaic.realtime_peroids*60).seconds.do(run_threaded, _make_mosaic.do_realtime)
            while True:
                schedule.run_pending()
                time.sleep(1)
             
        else: # archive mode
            _make_mosaic.do_archive()
            

# %%

