#!/bin/bash
source ~/.bashrc
conda activate radar
cd /home/wjzhu/OneDrive/PythonCode/MyWork/MyProject/radar_qpe_v1
nohup python monitor_process_s1.py > myout1.file 2>&1 &
nohup python s2_pre_process_single_radar.py > myout2.file 2>&1 &
nohup python s3_trans_rainrate_to_qpe.py > myout3.file 2>&1 &
nohup python s4_mosaic_qpe.py > myout4.file 2>&1 &
nohup python s5_draw_qpe_mosaic.py > myout5.file 2>&1 &
echo start all!

