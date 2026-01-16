
# _*_ coding: utf-8 _*_

'''
解析pup_rose文件

'''

# %%
import os
import numpy as np
import metradar.util.geo_transforms_pyart as geotrans
from datetime import datetime,timedelta
from .rose_structer import _unpack_from_buf,_structure_size
from .rose_structer import *

import xarray as xr
from pyart.core import Radar
from pyart.config import FileMetadata
from pyart.io.common import make_time_unit_str

class READ_ROSE(object):

    def __init__(self,):
        self.stiinfo = None
        self.mesoinfo = None
        self.hailinfo = None
        self.tvsinfo = None
        self.ssinfo = None
        pass
    
    # 将数字风暴序号转为字母数字组合

    def get_id_char(self,id_num):
        '''
        该函数的算法由张持岸提供
        '''
        if not isinstance(id_num,int):
            print('id_num is not int')
            return None
        if id_num > 0:
            num = (id_num-1) // 26
            tail = (id_num-1) % 26
            newid = '%s%d'%(chr(65+tail),num)
        else:
            newid = '0'
        return newid

    #将数字的风向转换为文字
    def get_wind_dir_name(self,wdir):
        wdir_cn = '未知'
        if not isinstance(wdir,int) and not isinstance(wdir,float):
            return wdir_cn
        if (wdir >= 348.76 and wdir <= 360) or (wdir >= 0 and wdir <= 11.25):
            wdir_cn = '北'
        elif wdir >= 11.26 and wdir <=33.75:
            wdir_cn = '北东北'
        elif wdir >= 33.76 and wdir <=56.25:
            wdir_cn = '东北'
        elif wdir >= 56.26 and wdir <=78.75:
            wdir_cn = '东东北'
        elif wdir >= 78.76 and wdir <101.25:
            wdir_cn = '东'
        elif wdir >= 101.26 and wdir <123.75:
            wdir_cn = '东东南'
        elif wdir >= 123.76 and wdir <146.25:
            wdir_cn = '东南'
        elif wdir >= 146.26 and wdir <168.75:
            wdir_cn = '南东南'
        elif wdir >= 168.76 and wdir <191.25:
            wdir_cn = '南'
        elif wdir >= 191.26 and wdir <213.75: 
            wdir_cn = '南西南'
        elif wdir >= 213.76 and wdir <236.25:
            wdir_cn = '西南'
        elif wdir >= 236.26 and wdir <258.75:
            wdir_cn = '西西南'
        elif wdir >= 258.76 and wdir <281.25:
            wdir_cn = '西'
        elif wdir >= 281.26 and wdir <303.75:
            wdir_cn = '西西北'
        elif wdir >= 303.76 and wdir <326.25:
            wdir_cn = '西北'
        elif wdir >= 326.26 and wdir <348.75:
            wdir_cn = '北西北'

        return wdir_cn
    
    def get_mda_rank(self,range_from_radar,shear_value):

        '''
        range_from_radar: km
        shear_value : m/s
        '''
        knot2ms = 0.514444445
        nmi     = 1.852 

        #x = np.array([0,30,60,90,120,150,180,210]) # x取值
        x  = np.arange(0,250) # x取值
        y1 = -1.*knot2ms*x/(24*nmi)+25*knot2ms
        y2 = -1.*knot2ms*x/(18*nmi)+35*knot2ms
        y3 = -1.*knot2ms*x/(14*nmi)+45*knot2ms

        curx = int(range_from_radar)
        if curx >=250 or curx <=0:
            print('range from radar should be larger than 0 and less than 250km ')
            return None
        
        cury = shear_value
        if cury < y1[curx]:
            return 1
        elif cury <y2[curx]:
            return 2
        elif cury < y3[curx]:
            return 3
        else:
            return 4

    def get_tvs_rank(self,lldv_value):

        # the "19" icon indicates that LLDV was at least 190 kts.

        ms2knot = 1.943844490

        value = lldv_value * ms2knot

        index = value // 10
        if index > 19:
            index=19

        return index+1
    
    # 解析风暴追踪产品
    def read_sti(self,filepath,filename):
        '''
        解析sti产品文件
        '''
        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        # pprint(prod_type)
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        pos = pos + 64 # 产品参数长度固定为64个


        # 获取产品数据块

        #获取STI头信息
        dic_sti_header = _unpack_from_buf(buf, pos, SIT_HEADER_BLOCK)
        pos = pos + _structure_size(SIT_HEADER_BLOCK)

        # 风暴追踪信息块
        # 风暴移动信息
        storm_motion_block=[]
        for nn in np.arange(dic_sti_header['storm_number']):
            storm_motion_block.append(_unpack_from_buf(buf, pos, STORM_MOTION_BLOCK))
            pos = pos + _structure_size(STORM_MOTION_BLOCK)

        # 风暴预报信息
        all_storm_fst_block=[]
        for nn in np.arange(dic_sti_header['storm_number']):
            dic_storm_fst_num = _unpack_from_buf(buf, pos, STORM_FST_HIS_NUM)
            pos = pos + _structure_size(STORM_FST_HIS_NUM)

            storm_fst_block=[]
            for nn in np.arange(dic_storm_fst_num['position_number']):
                storm_fst_block.append(_unpack_from_buf(buf, pos, STORM_FST_HIS_BLOCK))
                pos = pos + _structure_size(STORM_FST_HIS_BLOCK)
            all_storm_fst_block.append(storm_fst_block)

        # 风暴历史信息
        all_storm_his_block=[]
        for nn in np.arange(dic_sti_header['storm_number']):
            dic_storm_his_num = _unpack_from_buf(buf, pos, STORM_FST_HIS_NUM)
            pos = pos + _structure_size(STORM_FST_HIS_NUM)

            storm_his_block=[]
            for nn in np.arange(dic_storm_his_num['position_number']):
                storm_his_block.append(_unpack_from_buf(buf, pos, STORM_FST_HIS_BLOCK))
                pos = pos + _structure_size(STORM_FST_HIS_BLOCK)
            all_storm_his_block.append(storm_his_block)

        # 风暴属性表块数据
        # 风暴属性
        all_storm_prop=[]
        for nn in np.arange(dic_sti_header['storm_number']):
            dic_storm_prop = _unpack_from_buf(buf, pos, STORM_PROPERTY)
            pos = pos + _structure_size(STORM_PROPERTY)

            # 将风暴数字ID转换为字符串ID
            dic_storm_prop['id_char'] = self.get_id_char(dic_storm_prop['id'])

            # 将风暴移动信息融合进来
            dic_storm_prop['mv_spd'] = storm_motion_block[nn]['mv_spd']
            dic_storm_prop['mv_dir'] = storm_motion_block[nn]['mv_dir']

            all_storm_prop.append(dic_storm_prop)

        # 风暴构成表
        all_storm_comp=[]
        for nn in np.arange(dic_sti_header['storm_number']):
            dic_storm_comp = _unpack_from_buf(buf, pos, STORM_COMPONENT)
            pos = pos + _structure_size(STORM_COMPONENT)
            all_storm_comp.append(dic_storm_comp)

        # 风暴追踪适配数据
        dic_storm_track_param = _unpack_from_buf(buf, pos, STORM_TRACK_PARAM)
        pos = pos + _structure_size(STORM_TRACK_PARAM)


        allresult={}
        allresult['dic_gh'] = dic_gh
        allresult['dic_scfg'] = dic_scfg
        allresult['dic_tcfg'] = dic_tcfg
        allresult['cutinfo'] = cutinfo
        allresult['dic_prod_header'] = dic_prod_header
        allresult['dic_prod_param'] = dic_prod_param
        allresult['dic_sti_header'] = dic_sti_header
        allresult['storm_motion_block'] = storm_motion_block
        allresult['all_storm_fst_block'] = all_storm_fst_block
        allresult['all_storm_his_block'] = all_storm_his_block
        allresult['all_storm_prop'] = all_storm_prop
        allresult['all_storm_comp'] = all_storm_comp
        allresult['dic_storm_track_param'] = dic_storm_track_param

        # 将所有的方位角和距离转换成经纬度
        for nn in np.arange(dic_sti_header['storm_number']):
            pass
            x,y,z = geotrans.antenna_to_cartesian(storm_motion_block[nn]['range']/1000.0,storm_motion_block[nn]['azi'],0)
            clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
            storm_motion_block[nn]['lon'] = clon[0]
            storm_motion_block[nn]['lat'] = clat[0]
            all_storm_prop[nn]['lon'] = clon[0]
            all_storm_prop[nn]['lat'] = clat[0]
            for blk in all_storm_fst_block[nn]:
                x,y,z = geotrans.antenna_to_cartesian(blk['range']/1000.0,blk['azi'],0)
                clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
                blk['lon'] = clon[0]
                blk['lat'] = clat[0]
            for blk in all_storm_his_block[nn]:
                x,y,z = geotrans.antenna_to_cartesian(blk['range']/1000.0,blk['azi'],0)
                clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
                blk['lon'] = clon[0]
                blk['lat'] = clat[0]

            pass

        # 整理成track
        alltrackinfo=[]
        all_past_position=[] # 历史位置
        all_fst_position=[] # 预报位置
        all_current_position=[] # 当前位置
        for nn in np.arange(dic_sti_header['storm_number']):
            pass
            
            if all_storm_prop[nn]['type']==0: # 连续风暴
                trackinfo=[]
                
                # 先添加历史位置
                for blk in all_storm_his_block[nn][::-1]:
                    trackinfo.append([blk['lat'],blk['lon']])
                    all_past_position.append([blk['lat'],blk['lon']])

                # 添加当前风暴位置
                trackinfo.append([storm_motion_block[nn]['lat'],storm_motion_block[nn]['lon']])
                all_current_position.append([storm_motion_block[nn]['lat'],storm_motion_block[nn]['lon']])

                # 再添加预报位置
                for blk in all_storm_fst_block[nn]:
                    trackinfo.append([blk['lat'],blk['lon']])
                    all_fst_position.append([blk['lat'],blk['lon']])

                alltrackinfo.append(trackinfo)

        # allresult={}
        allresult['track'] = alltrackinfo
        allresult['marker_past'] = all_past_position
        allresult['marker_current'] = all_current_position
        allresult['marker_fst'] = all_fst_position

        self.stiinfo = allresult
        return allresult

    # 解析风暴结构文本产品
    def read_ss(self,filepath,filename):
        '''
        解析ss产品文件
        '''
        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        # pprint(prod_type)
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        pos = pos + 64 # 产品参数长度固定为64个


        # 获取产品数据块

        #获取SS头信息
        dic_ss_header = _unpack_from_buf(buf, pos, SS_HEAD_BLOCK)
        pos = pos + _structure_size(SS_HEAD_BLOCK)

        # SS结构信息
        ss_tab=[]
        for nn in np.arange(dic_ss_header['storm_number']):
            tmp_tab = _unpack_from_buf(buf, pos, SS_TAB)
            pos = pos + _structure_size(SS_TAB)
            # 将风暴数字ID转换为字符串ID
            tmp_tab['id_char'] = self.get_id_char(tmp_tab['storm_id'])
            
            ss_tab.append(tmp_tab)


        # 风暴趋势信息
        cell_trend=[]
        for nn in np.arange(dic_ss_header['storm_number']):
            cur_cellinfo={}
            cell_info=_unpack_from_buf(buf, pos, CELL_TREND)
            pos = pos + _structure_size(CELL_TREND)
            cur_cellinfo['head_info'] = cell_info
            # 历史体扫信息
            tmpcell_his=[]
            for im in np.arange(cell_info['his_vol_num']):
                tmpcell_his.append(_unpack_from_buf(buf, pos, HIS_VOL))
                pos = pos + _structure_size(HIS_VOL)
            cur_cellinfo['cell_info'] = tmpcell_his

            cell_trend.append(cur_cellinfo)

        # 风暴段适配数据
        seg_adapt = _unpack_from_buf(buf, pos, SEG_ADAPT)
        pos = pos + _structure_size(SEG_ADAPT)

        # 风暴质心适配数据
        centroid_adapt = _unpack_from_buf(buf, pos, CENTROIDS_ADAPT)
        pos = pos + _structure_size(CENTROIDS_ADAPT)

        # 风暴追踪适配数据
        storm_track_adapt = _unpack_from_buf(buf, pos, STORM_TRACK_PARAM)
        pos = pos + _structure_size(STORM_TRACK_PARAM)
        

        # 将所有的方位角和距离转换成经纬度
        for nn in np.arange(dic_ss_header['storm_number']):
            pass
            x,y,z = geotrans.antenna_to_cartesian(ss_tab[nn]['range']/1000.0,ss_tab[nn]['azi'],0)
            clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
            ss_tab[nn]['lon'] = clon[0]
            ss_tab[nn]['lat'] = clat[0]
        

        allresult={}
        allresult['ss'] = ss_tab
       
        self.ssinfo = allresult
        return allresult
    
    # 解析中气旋产品
    def read_mda(self,filepath,filename):
        '''
        解析meso产品文件
        '''
        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        # pprint(prod_type)
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        pos = pos + 64 # 产品参数长度固定为64个

        # 获取产品数据块

        #获取中气旋头信息
        dic_meso_header = _unpack_from_buf(buf, pos, MESO_HEADER_BLOCK)
        pos = pos + _structure_size(MESO_HEADER_BLOCK)

        # 中气旋表块
        meso_tab=[]
        for nn in np.arange(dic_meso_header['meso_number']):
            meso_tab.append(_unpack_from_buf(buf, pos, MESO_TABLE))
            pos = pos + _structure_size(MESO_TABLE)

        # 中气旋特征表
        meso_feature_tab=[]
        for nn in np.arange(dic_meso_header['feature_number']):
            meso_feature_tab.append(_unpack_from_buf(buf, pos, MESO_FEATURE_TAB))
            pos = pos + _structure_size(MESO_FEATURE_TAB)

        # 中气旋适配数据
        dic_meso_adapt_param = _unpack_from_buf(buf, pos, MESO_ADAPTATION_DATA)
        pos = pos + _structure_size(MESO_ADAPTATION_DATA)

        
        for nn in np.arange(dic_meso_header['meso_number']):
            # 将所有的方位角和距离转换成经纬度
            x,y,z = geotrans.antenna_to_cartesian(meso_tab[nn]['range']/1000.0,meso_tab[nn]['azi'],0)
            clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
            meso_tab[nn]['lon'] = clon[0]
            meso_tab[nn]['lat'] = clat[0]
            
            # 将数字序号转换成字母数字组合
            meso_tab[nn]['storm_id_char'] = self.get_id_char(meso_tab[nn]['storm_id'])
            meso_tab[nn]['feature_id_char'] = self.get_id_char(meso_tab[nn]['feature_id'])
            meso_tab[nn]['feature_type'] = meso_feature_tab[nn]['feature_type']
            
        for nn in np.arange(dic_meso_header['feature_number']):
            # 将所有的方位角和距离转换成经纬度
            x,y,z = geotrans.antenna_to_cartesian(meso_feature_tab[nn]['range']/1000.0,meso_feature_tab[nn]['azi'],0)
            clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
            meso_feature_tab[nn]['lon'] = clon[0]
            meso_feature_tab[nn]['lat'] = clat[0]
            
            # 将数字序号转换成字母数字组合
            meso_feature_tab[nn]['storm_id_char'] = self.get_id_char(meso_feature_tab[nn]['storm_id'])
            meso_feature_tab[nn]['feature_id_char'] = self.get_id_char(meso_feature_tab[nn]['feature_id'])


        allresult={}
        allresult['meso'] = meso_tab
        allresult['feature'] = meso_feature_tab
        
        self.mesoinfo = allresult
        return allresult

    # 解析龙卷涡旋特征产品
    def read_tvs(self,filepath,filename):
        '''
        解析tvs产品文件
        '''
        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        # pprint(prod_type)
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        pos = pos + 64 # 产品参数长度固定为64个

        # 获取产品数据块

        #获取TVS头信息
        dic_tvs_header = _unpack_from_buf(buf, pos, TVS_HEADER_BLOCK)
        pos = pos + _structure_size(TVS_HEADER_BLOCK)

        # TVS表块
        tvs_tab=[]
        for nn in np.arange(dic_tvs_header['tvs_number']):
            tvs_tab.append(_unpack_from_buf(buf, pos, TVS_TAB))
            pos = pos + _structure_size(TVS_TAB)

        # TVS适配数据
        dic_tvs_adapt_param = _unpack_from_buf(buf, pos, TVS_ADAPTATION_DATA)
        pos = pos + _structure_size(TVS_ADAPTATION_DATA)


        # 将所有的方位角和距离转换成经纬度
        for nn in np.arange(dic_tvs_header['tvs_number']):
            pass
            x,y,z = geotrans.antenna_to_cartesian(tvs_tab[nn]['range']/1000.0,tvs_tab[nn]['azi'],0)
            clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
            tvs_tab[nn]['lon'] = clon[0]
            tvs_tab[nn]['lat'] = clat[0]
            
            
            # 将数字序号转换成字母数字组合
            tvs_tab[nn]['storm_id_char'] = self.get_id_char(tvs_tab[nn]['storm_id'])



        allresult={}
        allresult['tvs'] = tvs_tab
        
        self.tvsinfo = allresult
        return allresult
    
    # 解析ppi产品文件
    def read_ppi(self,filepath,filename):
        '''
        解析ppi产品文件
        '''
        pass

        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
            return None
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        pos = pos + 64 # 产品参数长度固定为64个

        # 读取产品径向头信息
        dic_radial_header = _unpack_from_buf(buf, pos, RADIAL_HEADER)
        pos = pos + _structure_size(RADIAL_HEADER)

        print('数据类型: %d-%s'%(dic_radial_header['data_type'],PRODUCT_DATA_TYPE[dic_radial_header['data_type']]))
        # Value=(Code-Offset)/Scale
        # 读取径向数据RADIAL_DATA
        data=[]
        data_azi=[]
        binnum = None
        for nn in np.arange(dic_radial_header['radial_number']):
            radial_data_info = _unpack_from_buf(buf, pos, RADIAL_DATA)
            pos = pos + _structure_size(RADIAL_DATA)
            binnum = radial_data_info['num_bins']
            rdata = buf[pos:pos+dic_radial_header['bin_length']*radial_data_info['num_bins']]
            data.extend(rdata)
            pos = pos + dic_radial_header['bin_length']*radial_data_info['num_bins']
            # print(radial_data_info['start_azi'])
            data_azi.append(radial_data_info['start_azi'])
            pass
        data = np.array(data)
       
        
        # data = data.astype('uint8')
        data = data.astype('float32')
        nx = dic_radial_header['radial_number']
        ny = binnum
        print(nx,ny)
        # 将data转成numpy数组
        data = data.reshape((nx,ny))
        
        
        # decode data
        # ppiarray = (ppiarray - dic_radial_header['offset']) / dic_radial_header['scale'] 

        # 将方位角坐标转换成经纬度坐标
        sweep_azimuths =np.array(data_azi)
        ele = dic_prod_param['ele']
        range_reso = dic_radial_header['resolution']
        # 输出的距离库只保留一半
        ngates = int(ny//2*2) #//2*2 
        grid_x = np.arange(-1*ngates,ngates+1,1)
        grid_y = np.arange(-1*ngates,ngates+1,1)
        total_gates = len(grid_x)

        aa = np.meshgrid(grid_x,grid_y)
        azi_grid = np.arctan2(aa[0],aa[1])*180/np.pi
        azi_grid[azi_grid<0]+=360

        azi_reso = 360/len(sweep_azimuths)

        # 求方位角索引
        new_azi = azi_grid.flatten()
        t = new_azi-sweep_azimuths[0]
        t[t<0]+=360
        ray_number = np.round(t/azi_reso,0).astype(int)
        ray_number[ray_number==len(sweep_azimuths)]=0
        ray_number = np.reshape(ray_number,(total_gates,total_gates))

        # 求距离索引
        dis_grid = np.sqrt(aa[0]**2 + aa[1]**2)
        dis_grid = np.round(dis_grid.flatten(),0).astype(int)
        dis_grid = np.reshape(dis_grid,(total_gates,total_gates))

        # 对数据进行截断，在径向方向上
        data = data[:,0:ngates]

        # data_grid = np.zeros((total_gates,total_gates),dtype='uint8') + 255
        data_grid = np.zeros((total_gates,total_gates),dtype='float32')
        new_data = data_grid.flatten()
        new_spdata = data.flatten()
        
        pos_out = [i+j*total_gates for i in range(total_gates) for j in range(total_gates) if dis_grid[i,j] < ngates]
        d_out = [rn*ngates+dg for rn,dg in zip(ray_number.flatten(),dis_grid.flatten()) if dg < ngates]

        new_data[pos_out]=new_spdata[d_out]
       
        data_grid = np.reshape(new_data,(total_gates,total_gates))
       

        if total_gates % 2 == 0:
            out_grid = np.arange(int(-total_gates/2),int(total_gates/2))
        else:
            out_grid = np.arange(int(-(total_gates-1)/2),int((total_gates-1)/2)+1)
        # 将ougrid转换为经纬度坐标
        out_lon,out_lat = geotrans.cartesian_to_geographic_aeqd(out_grid*range_reso,out_grid*range_reso,dic_scfg['lon'],dic_scfg['lat'])
        outdata = (data_grid - dic_radial_header['offset']) / dic_radial_header['scale'] 
        minvalue = (dic_radial_header['min_value'] - dic_radial_header['offset']) / dic_radial_header['scale']
        maxvalue = (dic_radial_header['max_value'] - dic_radial_header['offset']) / dic_radial_header['scale']
        # outdata[outdata > 90] = -32
        # outdata[outdata < -10] = -32
        outdata[outdata >= maxvalue] = -9999
        outdata[outdata <= minvalue] = -9999
        varname = 'ref'
        units = 'dBZ'
        if dic_radial_header['data_type'] == 7:
            varname = 'zdr'
            units = 'dB'
        elif dic_radial_header['data_type'] == 9:
            varname = 'cc'
            units = '%'
        elif dic_radial_header['data_type'] == 11:
            varname = 'kdp'
            units = 'deg/km'
            pass
        data = xr.DataArray(np.array(outdata.transpose()),coords=[out_lat,out_lon],dims=['lat','lon'],name=varname)
        data.attrs['units'] = units
        # data.attrs['standard_name'] = 'equivalent_reflectivity_factor'
        # data.attrs['long_name'] = 'equivalent_reflectivity_factor'
        data.attrs['varname'] = varname
        data.attrs['radar_lat'] = dic_scfg['lat']
        data.attrs['radar_lon'] = dic_scfg['lon']
        data.attrs['ana_height'] = dic_scfg['ana_height']
        data.attrs['grid_num'] = total_gates
        data.attrs['grid_reso'] = range_reso
        data.attrs['elevation'] = ele
        data.attrs['obs_range'] = int(range_reso * (total_gates-1)/2)
        data.attrs['distance_unit'] = 'meter'
        data.attrs['missing_value'] = -9999
        data.attrs['datatype'] = 'float32'
        # data.attrs['decode_method'] = 'dbz = (data - %d) / %d'%(dic_radial_header['offset'],dic_radial_header['scale'])
        data.attrs['task_name'] = dic_tcfg['task_name'].decode('utf-8').strip('\x00')
        data.attrs['radar_type'] = dic_scfg['radar_type']
        data.attrs['scan_time'] = datetime.fromtimestamp(dic_tcfg['scan_stime']).strftime('%Y-%m-%d %H:%M:%S')
        try:
            data.attrs['site_name'] = dic_scfg['site_name'].decode('utf-8').strip('\x00')
        except:
            data.attrs['site_name'] = 'Unknown'
        data.attrs['site_id'] = dic_scfg['site_code'].decode('utf-8').strip('\x00')
        # data.attrs['offset'] = dic_radial_header['offset']
        # data.attrs['scale'] = dic_radial_header['scale']

        return data

    # 解析cr产品文件，栅格数据
    def read_cr(self,filepath,filename):
        '''
        解析cr产品文件
        '''
        pass

        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
            return None
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        print('产品类型: %d-%s'%(dic_prod_header['product_type'],prod_type))
        pos = pos + 64 # 产品参数长度固定为64个

        # 读取产品径向头信息
        dic_grid_header = _unpack_from_buf(buf, pos, GRID_HEADER)
        pos = pos + _structure_size(GRID_HEADER)
        print('数据类型: %d-%s'%(dic_grid_header['data_type'],PRODUCT_DATA_TYPE[dic_grid_header['data_type']]))
        # Value=(Code-Offset)/Scale
        # 读取栅格数据GRID_DATA
        
        if dic_grid_header['bin_length'] == 1:
            data1 = np.frombuffer(buf[pos:pos + dic_grid_header['row_side_len']*dic_grid_header['col_side_len']*dic_grid_header['bin_length']], '>u1')
            data1 = data1.astype('uint8')
        data2 = np.reshape(data1,(dic_grid_header['row_side_len'],dic_grid_header['col_side_len']))
        # data2[data2<dic_grid_header['min_value']]=255
        # 根据offset和scale进行解码
        data2 = (data2 - dic_grid_header['offset']) / dic_grid_header['scale']
        data2[data2 > 90] = -32
        data2[data2 < -10] = -32
        nlat = dic_grid_header['row_side_len']
        nlon = dic_grid_header['col_side_len']
        # print(nlat,nlon)
        lat_reso = dic_grid_header['row_resolution']
        lon_reso = dic_grid_header['col_resolution']

        # 将data由list转成二维numpy数组

        
        # 将直角坐标转换成经纬度坐标
       
        if nlat % 2 == 0:
            out_grid_lat = np.arange(int(-nlat/2),int(nlat/2))
        else:
            out_grid_lat = np.arange(int(-(nlat-1)/2),int((nlat-1)/2)+1)
        
        if nlon % 2 == 0:
            out_grid_lon = np.arange(int(-nlon/2),int(nlon/2))
        else:
            out_grid_lon = np.arange(int(-(nlon-1)/2),int((nlon-1)/2)+1)

        # 将ougrid转换为经纬度坐标
        out_lon,out_lat = geotrans.cartesian_to_geographic_aeqd(out_grid_lat*lat_reso,out_grid_lon*lon_reso,dic_scfg['lon'],dic_scfg['lat'])

        data = xr.DataArray(np.flipud(data2),coords=[out_lat,out_lon],dims=['lat','lon'],name='cref')
        data.attrs['units'] = 'dBZ'
        data.attrs['standard_name'] = 'composite_reflectivity_factor'
        data.attrs['long_name'] = 'composite_reflectivity_factor'
        data.attrs['radar_lat'] = dic_scfg['lat']
        data.attrs['radar_lon'] = dic_scfg['lon']
        data.attrs['ana_height'] = dic_scfg['ana_height']
        data.attrs['lat_grid_num'] = nlat
        data.attrs['lon_grid_num'] = nlon
        data.attrs['lat_grid_reso'] = dic_grid_header['row_resolution']
        data.attrs['lon_grid_reso'] = dic_grid_header['col_resolution']
        data.attrs['distance_unit'] = 'meter'
        data.attrs['missing_value'] = -32
        data.attrs['datatype'] = 'float32'
        # data.attrs['decode_method'] = 'dbz = (data - %d) / %d'%(dic_grid_header['offset'],dic_grid_header['scale'])
        data.attrs['task_name'] = dic_tcfg['task_name'].decode('utf-8').strip('\x00')
        data.attrs['radar_type'] = dic_scfg['radar_type']
        data.attrs['scan_time'] = datetime.fromtimestamp(dic_tcfg['scan_stime']).strftime('%Y-%m-%d %H:%M:%S')
        try:
            data.attrs['site_name'] = dic_scfg['site_name'].decode('utf-8').strip('\x00')
        except:
            data.attrs['site_name'] = 'Unknown'
        data.attrs['site_id'] = dic_scfg['site_code'].decode('utf-8').strip('\x00')

        return data

    # 解析vil产品文件，栅格数据
    def read_vil(self,filepath,filename):
        '''
        解析vil产品文件
        '''
        pass

        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        print('产品类型: %d-%s'%(dic_prod_header['product_type'],prod_type))
        pos = pos + 64 # 产品参数长度固定为64个

        # 读取产品径向头信息
        dic_grid_header = _unpack_from_buf(buf, pos, GRID_HEADER)
        pos = pos + _structure_size(GRID_HEADER)
        print('数据类型: %d-%s'%(dic_grid_header['data_type'],PRODUCT_DATA_TYPE[dic_grid_header['data_type']]))
        # Value=(Code-Offset)/Scale
        # 读取栅格数据GRID_DATA
        
        if dic_grid_header['bin_length'] == 1:
            data1 = np.frombuffer(buf[pos:pos + dic_grid_header['row_side_len']*dic_grid_header['col_side_len']*dic_grid_header['bin_length']], '>u1')
            data1 = data1.astype('uint8')
        data2 = np.reshape(data1,(dic_grid_header['row_side_len'],dic_grid_header['col_side_len']))
        # data2[data2<dic_grid_header['min_value']]=255
        # 根据offset和scale进行解码
        data2 = (data2 - dic_grid_header['offset']) / dic_grid_header['scale']
        data2[data2 > 120] =  0
        data2[data2 < 0] = 0
        nlat = dic_grid_header['row_side_len']
        nlon = dic_grid_header['col_side_len']
        # print(nlat,nlon)
        lat_reso = dic_grid_header['row_resolution']
        lon_reso = dic_grid_header['col_resolution']

        # 将data由list转成二维numpy数组

        
        # 将直角坐标转换成经纬度坐标
       
        if nlat % 2 == 0:
            out_grid_lat = np.arange(int(-nlat/2),int(nlat/2))
        else:
            out_grid_lat = np.arange(int(-(nlat-1)/2),int((nlat-1)/2)+1)
        
        if nlon % 2 == 0:
            out_grid_lon = np.arange(int(-nlon/2),int(nlon/2))
        else:
            out_grid_lon = np.arange(int(-(nlon-1)/2),int((nlon-1)/2)+1)

        # 将ougrid转换为经纬度坐标
        out_lon,out_lat = geotrans.cartesian_to_geographic_aeqd(out_grid_lat*lat_reso,out_grid_lon*lon_reso,dic_scfg['lon'],dic_scfg['lat'])

        data = xr.DataArray(np.flipud(data2),coords=[out_lat,out_lon],dims=['lat','lon'],name='vil')
        data.attrs['units'] = 'kg/m^2'
        data.attrs['standard_name'] = 'vertically_integrated_liquid_water'
        data.attrs['long_name'] = 'vertically_integrated_liquid_water'
        data.attrs['radar_lat'] = dic_scfg['lat']
        data.attrs['radar_lon'] = dic_scfg['lon']
        data.attrs['ana_height'] = dic_scfg['ana_height']
        data.attrs['lat_grid_num'] = nlat
        data.attrs['lon_grid_num'] = nlon
        data.attrs['lat_grid_reso'] = dic_grid_header['row_resolution']
        data.attrs['lon_grid_reso'] = dic_grid_header['col_resolution']
        data.attrs['distance_unit'] = 'meter'
        data.attrs['missing_value'] = 0
        data.attrs['datatype'] = 'float32'
        # data.attrs['decode_method'] = 'dbz = (data - %d) / %d'%(dic_grid_header['offset'],dic_grid_header['scale'])
        data.attrs['task_name'] = dic_tcfg['task_name'].decode('utf-8').strip('\x00')
        data.attrs['radar_type'] = dic_scfg['radar_type']
        data.attrs['scan_time'] = datetime.fromtimestamp(dic_tcfg['scan_stime']).strftime('%Y-%m-%d %H:%M:%S')
        try:
            data.attrs['site_name'] = dic_scfg['site_name'].decode('utf-8').strip('\x00')
        except:
            data.attrs['site_name'] = 'Unknown'
        data.attrs['site_id'] = dic_scfg['site_code'].decode('utf-8').strip('\x00')

        return data
    
    # 解析tops产品文件，栅格数据，回波顶高
    def read_tops(self,filepath,filename):
        '''
        解析tops产品文件
        '''
        pass

        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        print('产品类型: %d-%s'%(dic_prod_header['product_type'],prod_type))
        pos = pos + 64 # 产品参数长度固定为64个

        # 读取产品径向头信息
        dic_grid_header = _unpack_from_buf(buf, pos, GRID_HEADER)
        pos = pos + _structure_size(GRID_HEADER)
        print('数据类型: %d-%s'%(dic_grid_header['data_type'],PRODUCT_DATA_TYPE[dic_grid_header['data_type']]))
        # Value=(Code-Offset)/Scale
        # 读取栅格数据GRID_DATA
        
        if dic_grid_header['bin_length'] == 1:
            data1 = np.frombuffer(buf[pos:pos + dic_grid_header['row_side_len']*dic_grid_header['col_side_len']*dic_grid_header['bin_length']], '>u1')
            data1 = data1.astype('uint8')
        data2 = np.reshape(data1,(dic_grid_header['row_side_len'],dic_grid_header['col_side_len']))
        # data2[data2<dic_grid_header['min_value']]=255
        # 根据offset和scale进行解码
        
        
        
        data2[data2 >  dic_grid_header['max_value']] =  0
        data2 = (data2 - dic_grid_header['offset']) / dic_grid_header['scale']
        maxv = (dic_grid_header['max_value'] - dic_grid_header['offset']) / dic_grid_header['scale']
        data2[data2 > maxv] = 0

        nlat = dic_grid_header['row_side_len']
        nlon = dic_grid_header['col_side_len']
        # print(nlat,nlon)
        lat_reso = dic_grid_header['row_resolution']
        lon_reso = dic_grid_header['col_resolution']

        # 将data由list转成二维numpy数组

        
        # 将直角坐标转换成经纬度坐标
       
        if nlat % 2 == 0:
            out_grid_lat = np.arange(int(-nlat/2),int(nlat/2))
        else:
            out_grid_lat = np.arange(int(-(nlat-1)/2),int((nlat-1)/2)+1)
        
        if nlon % 2 == 0:
            out_grid_lon = np.arange(int(-nlon/2),int(nlon/2))
        else:
            out_grid_lon = np.arange(int(-(nlon-1)/2),int((nlon-1)/2)+1)

        # 将ougrid转换为经纬度坐标
        out_lon,out_lat = geotrans.cartesian_to_geographic_aeqd(out_grid_lat*lat_reso,out_grid_lon*lon_reso,dic_scfg['lon'],dic_scfg['lat'])

        data = xr.DataArray(np.flipud(data2),coords=[out_lat,out_lon],dims=['lat','lon'],name='et')
        data.attrs['units'] = 'km'
        data.attrs['standard_name'] = 'echo_top'
        data.attrs['long_name'] = 'echo_top'
        data.attrs['radar_lat'] = dic_scfg['lat']
        data.attrs['radar_lon'] = dic_scfg['lon']
        data.attrs['ana_height'] = dic_scfg['ana_height']
        data.attrs['lat_grid_num'] = nlat
        data.attrs['lon_grid_num'] = nlon
        data.attrs['lat_grid_reso'] = dic_grid_header['row_resolution']
        data.attrs['lon_grid_reso'] = dic_grid_header['col_resolution']
        data.attrs['distance_unit'] = 'kilometer'
        data.attrs['missing_value'] = 0
        data.attrs['datatype'] = 'float32'
        # data.attrs['decode_method'] = 'dbz = (data - %d) / %d'%(dic_grid_header['offset'],dic_grid_header['scale'])
        data.attrs['task_name'] = dic_tcfg['task_name'].decode('utf-8').strip('\x00')
        data.attrs['radar_type'] = dic_scfg['radar_type']
        data.attrs['scan_time'] = datetime.fromtimestamp(dic_tcfg['scan_stime']).strftime('%Y-%m-%d %H:%M:%S')
        try:
            data.attrs['site_name'] = dic_scfg['site_name'].decode('utf-8').strip('\x00')
        except:
            data.attrs['site_name'] = 'Unknown'
        data.attrs['site_id'] = dic_scfg['site_code'].decode('utf-8').strip('\x00')

        return data
    

    # 解析stp产品文件，栅格数据
    def read_stp(self,filepath,filename):
        '''
        解析stp产品文件
        '''
        pass

        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        print('产品类型: %d-%s'%(dic_prod_header['product_type'],prod_type))
        pos = pos + 64 # 产品参数长度固定为64个

        # 读取产品径向头信息
        dic_radial_header = _unpack_from_buf(buf, pos, RADIAL_HEADER)
        pos = pos + _structure_size(RADIAL_HEADER)
        print('数据类型: %d-%s'%(dic_radial_header['data_type'],PRODUCT_DATA_TYPE[dic_radial_header['data_type']]))
        # Value=(Code-Offset)/Scale
        # 读取径向数据RADIAL_DATA
        data=[]
        data_azi=[]
        binnum = None
        for nn in np.arange(dic_radial_header['radial_number']):
            radial_data_info = _unpack_from_buf(buf, pos, RADIAL_DATA)
            pos = pos + _structure_size(RADIAL_DATA)
            binnum = radial_data_info['num_bins']
            # rdata = buf[pos:pos+dic_radial_header['bin_length']*radial_data_info['num_bins']]
            rdata = np.frombuffer(buf[pos:pos+dic_radial_header['bin_length']*radial_data_info['num_bins']], '<u2')
            data.extend(rdata)
            pos = pos + dic_radial_header['bin_length']*radial_data_info['num_bins']
            # print(radial_data_info['start_azi'])
            data_azi.append(radial_data_info['start_azi'])
            pass
        data = np.array(data)
        # data[data > dic_radial_header['max_value']] = 0
        # data[data < dic_radial_header['min_value']] = 0
        
        data = data.astype('uint16')
        nx = dic_radial_header['radial_number']
        ny = binnum
        print(nx,ny)
        # 将data转成numpy数组
        data = data.reshape((nx,ny))

        # 将方位角坐标转换成经纬度坐标
        sweep_azimuths =np.array(data_azi)

        range_reso = dic_radial_header['resolution']
        # 输出的距离库只保留一半
        ngates = int(ny//2*2) #//2*2 
        grid_x = np.arange(-1*ngates,ngates+1,1)
        grid_y = np.arange(-1*ngates,ngates+1,1)
        total_gates = len(grid_x)

        aa = np.meshgrid(grid_x,grid_y)
        azi_grid = np.arctan2(aa[0],aa[1])*180/np.pi
        azi_grid[azi_grid<0]+=360

        azi_reso = 360/len(sweep_azimuths)

        # 求方位角索引
        new_azi = azi_grid.flatten()
        t = new_azi-sweep_azimuths[0]
        t[t<0]+=360
        ray_number = np.round(t/azi_reso,0).astype(int)
        ray_number[ray_number==len(sweep_azimuths)]=0
        ray_number = np.reshape(ray_number,(total_gates,total_gates))

        # 求距离索引
        dis_grid = np.sqrt(aa[0]**2 + aa[1]**2)
        dis_grid = np.round(dis_grid.flatten(),0).astype(int)
        dis_grid = np.reshape(dis_grid,(total_gates,total_gates))

        # 对数据进行截断，在径向方向上
        data = data[:,0:ngates]

        data_grid = np.zeros((total_gates,total_gates),dtype='uint16') + 0
        new_data = data_grid.flatten()
        new_spdata = data.flatten()
        # decode data
        new_spdata = (new_spdata - dic_radial_header['offset']) / dic_radial_header['scale'] 
        pos_out = [i+j*total_gates for i in range(total_gates) for j in range(total_gates) if dis_grid[i,j] < ngates]
        d_out = [rn*ngates+dg for rn,dg in zip(ray_number.flatten(),dis_grid.flatten()) if dg < ngates]

        new_data[pos_out]=new_spdata[d_out]
       
        data_grid = np.reshape(new_data,(total_gates,total_gates))


        if total_gates % 2 == 0:
            out_grid = np.arange(int(-total_gates/2),int(total_gates/2))
        else:
            out_grid = np.arange(int(-(total_gates-1)/2),int((total_gates-1)/2)+1)
        # 将ougrid转换为经纬度坐标
        out_lon,out_lat = geotrans.cartesian_to_geographic_aeqd(out_grid*range_reso,out_grid*range_reso,dic_scfg['lon'],dic_scfg['lat'])

        data = xr.DataArray(np.array(data_grid.transpose()),coords=[out_lat,out_lon],dims=['lat','lon'],name='stp')
        data.attrs['units'] = 'mm'
        data.attrs['standard_name'] = 'storm total precipitation'
        data.attrs['long_name'] = 'storm total precipitation'
        data.attrs['radar_lat'] = dic_scfg['lat']
        data.attrs['radar_lon'] = dic_scfg['lon']
        data.attrs['ana_height'] = dic_scfg['ana_height']
        data.attrs['grid_num'] = total_gates
        data.attrs['grid_reso'] = range_reso
        data.attrs['obs_range'] = int(range_reso * (total_gates-1)/2)
        data.attrs['distance_unit'] = 'meter'
        data.attrs['missing_value'] = 0
        data.attrs['datatype'] = 'uint16'
        # data.attrs['decode_method'] = 'qpe = (data - %d) / %d'%(dic_radial_header['offset'],dic_radial_header['scale'])
        data.attrs['task_name'] = dic_tcfg['task_name'].decode('utf-8').strip('\x00')
        data.attrs['radar_type'] = dic_scfg['radar_type']
        data.attrs['scan_time'] = datetime.fromtimestamp(dic_tcfg['scan_stime']).strftime('%Y-%m-%d %H:%M:%S')
        try:
            data.attrs['site_name'] = dic_scfg['site_name'].decode('utf-8').strip('\x00')
        except:
            data.attrs['site_name'] = 'Unknown'
        data.attrs['site_id'] = dic_scfg['site_code'].decode('utf-8').strip('\x00')
        # data.attrs['offset'] = dic_radial_header['offset']
        # data.attrs['scale'] = dic_radial_header['scale']

        return data

    # 解析ohp产品文件，栅格数据
    def read_ohp(self,filepath,filename):
        '''
        解析ohp产品文件
        '''
        pass

        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
            return None
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        print('产品类型: %d-%s'%(dic_prod_header['product_type'],prod_type))
        pos = pos + 64 # 产品参数长度固定为64个

        # 读取产品径向头信息
        dic_radial_header = _unpack_from_buf(buf, pos, RADIAL_HEADER)
        pos = pos + _structure_size(RADIAL_HEADER)
        print('数据类型: %d-%s'%(dic_radial_header['data_type'],PRODUCT_DATA_TYPE[dic_radial_header['data_type']]))
        # Value=(Code-Offset)/Scale
        # 读取径向数据RADIAL_DATA
        data=[]
        data_azi=[]
        binnum = None
        for nn in np.arange(dic_radial_header['radial_number']):
            radial_data_info = _unpack_from_buf(buf, pos, RADIAL_DATA)
            pos = pos + _structure_size(RADIAL_DATA)
            binnum = radial_data_info['num_bins']
            # rdata = buf[pos:pos+dic_radial_header['bin_length']*radial_data_info['num_bins']]
            rdata = np.frombuffer(buf[pos:pos+dic_radial_header['bin_length']*radial_data_info['num_bins']], '<u2')
            data.extend(rdata)
            pos = pos + dic_radial_header['bin_length']*radial_data_info['num_bins']
            # print(radial_data_info['start_azi'])
            data_azi.append(radial_data_info['start_azi'])
            pass
        data = np.array(data)
        data[data > dic_radial_header['max_value']] = 0
        data[data < dic_radial_header['min_value']] = 0
        
        data = data.astype('uint16')
        nx = dic_radial_header['radial_number']
        ny = binnum
        print(nx,ny)
        # 将data转成numpy数组
        data = data.reshape((nx,ny))

        # decode data
        # ppiarray = (ppiarray - dic_radial_header['offset']) / dic_radial_header['scale'] 

        
        # 将方位角坐标转换成经纬度坐标
        sweep_azimuths =np.array(data_azi)

        range_reso = dic_radial_header['resolution']
        # 输出的距离库只保留一半
        ngates = int(ny//2*2) #//2*2 
        grid_x = np.arange(-1*ngates,ngates+1,1)
        grid_y = np.arange(-1*ngates,ngates+1,1)
        total_gates = len(grid_x)

        aa = np.meshgrid(grid_x,grid_y)
        azi_grid = np.arctan2(aa[0],aa[1])*180/np.pi
        azi_grid[azi_grid<0]+=360

        azi_reso = 360/len(sweep_azimuths)

        # 求方位角索引
        new_azi = azi_grid.flatten()
        t = new_azi-sweep_azimuths[0]
        t[t<0]+=360
        ray_number = np.round(t/azi_reso,0).astype(int)
        ray_number[ray_number==len(sweep_azimuths)]=0
        ray_number = np.reshape(ray_number,(total_gates,total_gates))

        # 求距离索引
        dis_grid = np.sqrt(aa[0]**2 + aa[1]**2)
        dis_grid = np.round(dis_grid.flatten(),0).astype(int)
        dis_grid = np.reshape(dis_grid,(total_gates,total_gates))

        # 对数据进行截断，在径向方向上
        data = data[:,0:ngates]

        data_grid = np.zeros((total_gates,total_gates),dtype='uint16') + 0
        new_data = data_grid.flatten()
        new_spdata = data.flatten()

        new_spdata = (new_spdata - dic_radial_header['offset']) / dic_radial_header['scale'] 

        pos_out = [i+j*total_gates for i in range(total_gates) for j in range(total_gates) if dis_grid[i,j] < ngates]
        d_out = [rn*ngates+dg for rn,dg in zip(ray_number.flatten(),dis_grid.flatten()) if dg < ngates]

        new_data[pos_out]=new_spdata[d_out]
       
        data_grid = np.reshape(new_data,(total_gates,total_gates))
       

        if total_gates % 2 == 0:
            out_grid = np.arange(int(-total_gates/2),int(total_gates/2))
        else:
            out_grid = np.arange(int(-(total_gates-1)/2),int((total_gates-1)/2)+1)
        # 将ougrid转换为经纬度坐标
        out_lon,out_lat = geotrans.cartesian_to_geographic_aeqd(out_grid*range_reso,out_grid*range_reso,dic_scfg['lon'],dic_scfg['lat'])

        data = xr.DataArray(np.array(data_grid.transpose()),coords=[out_lat,out_lon],dims=['lat','lon'],name='ohp')
        data.attrs['units'] = 'mm'
        data.attrs['standard_name'] = 'one hour precipitation'
        data.attrs['long_name'] = 'one hour precipitation'
        data.attrs['radar_lat'] = dic_scfg['lat']
        data.attrs['radar_lon'] = dic_scfg['lon']
        data.attrs['ana_height'] = dic_scfg['ana_height']
        data.attrs['grid_num'] = total_gates
        data.attrs['grid_reso'] = range_reso
        data.attrs['obs_range'] = int(range_reso * (total_gates-1)/2)
        data.attrs['distance_unit'] = 'meter'
        data.attrs['missing_value'] = 0
        data.attrs['datatype'] = 'uint16'
        # data.attrs['decode_method'] = 'qpe = (data - %d) / %d'%(dic_radial_header['offset'],dic_radial_header['scale'])
        data.attrs['task_name'] = dic_tcfg['task_name'].decode('utf-8').strip('\x00')
        data.attrs['radar_type'] = dic_scfg['radar_type']
        data.attrs['scan_time'] = datetime.fromtimestamp(dic_tcfg['scan_stime']).strftime('%Y-%m-%d %H:%M:%S')
        try:
            data.attrs['site_name'] = dic_scfg['site_name'].decode('utf-8').strip('\x00')
        except:
            data.attrs['site_name'] = 'Unknown'
        data.attrs['site_id'] = dic_scfg['site_code'].decode('utf-8').strip('\x00')
        # data.attrs['offset'] = dic_radial_header['offset']
        # data.attrs['scale'] = dic_radial_header['scale']

        return data

    
    # 解析冰雹指数产品
    def read_hda(self,filepath,filename):
        '''
        解析hail产品文件
        '''
        fin = open(filepath + os.sep + filename,'rb')
        buf = fin.read()
        fin.close()
        buf_length = len(buf)
        pos = 0

        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        # pprint(dic_gh)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
        pos = pos + _structure_size(GENERIC_HEADER)

        # 获取站点信息
        dic_scfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos = pos + _structure_size(SITE_CONFIG)

        # 获取任务信息
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos = pos + _structure_size(TASK_CONFIG)

        # 获取扫描信息
        cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        # 获取产品头信息
        dic_prod_header = _unpack_from_buf(buf, pos, PRODUCT_HEADER_BLOCK)
        pos = pos + _structure_size(PRODUCT_HEADER_BLOCK)

        # 获取产品参数信息
        prod_type = PRODUCT_ID_NAME_MAP[dic_prod_header['product_type']]
        # pprint(prod_type)
        dic_prod_param = _unpack_from_buf(buf, pos, PRODUCT_DEPENDENT_PARAMETER[prod_type])
        pos = pos + 64 # 产品参数长度固定为64个

        # 获取产品数据块
        # 冰雹个数
        hail_number = _unpack_from_buf(buf, pos, HAIL_NUMBER)
        pos = pos + _structure_size(HAIL_NUMBER)

        # TVS表块
        hail_tab=[]
        for nn in np.arange(hail_number['hail_number']):
            hail_tab.append(_unpack_from_buf(buf, pos, HAIL_TAB))
            pos = pos + _structure_size(HAIL_TAB)

        # TVS适配数据
        dic_hail_adapt_param = _unpack_from_buf(buf, pos, HAIL_ADAPTATION_DATA)
        pos = pos + _structure_size(HAIL_ADAPTATION_DATA)

        # 将所有的方位角和距离转换成经纬度
        for nn in np.arange(hail_number['hail_number']):
            pass
            x,y,z = geotrans.antenna_to_cartesian(hail_tab[nn]['range']/1000.0,hail_tab[nn]['azi'],0)
            clon,clat = geotrans.cartesian_to_geographic_aeqd(x,y,dic_scfg['lon'],dic_scfg['lat'])
            hail_tab[nn]['lon'] = clon[0]
            hail_tab[nn]['lat'] = clat[0]

            # 将数字序号转换成字母数字组合
            hail_tab[nn]['hail_id_char'] = self.get_id_char(hail_tab[nn]['hail_id'])


        allresult={}
        allresult['hail'] = hail_tab

        self.hailinfo = allresult
        return allresult
    
        
if __name__ == "__main__":
    rootpath = 'testdata/rose_product'
    staid = 'Z9852'

    decoder = READ_ROSE()

    # # ppi zdr
    # filepath_ppi = '/Users/wenjianzhu/Downloads/CMADAAS/pup/Z9852/SS'

    # # ss
    # filepath_ss = rootpath + os.sep + staid + os.sep + 'SS'
    # filename_ss = 'Z_RADR_I_Z9852_20230421164954_P_DOR_CDD_SS_NUL_300_NUL_FMT.bin'
    # allresult_ss = decoder.read_ss(filepath_ss, filename_ss)
    

    # sti
    # filepath_sti = rootpath + os.sep + staid + os.sep + 'STI'
    # filename_sti = 'Z_RADR_I_Z9852_20230421164954_P_DOR_CDD_STI_NUL_300_NUL_FMT.bin'
    # allresult_sti = decoder.read_sti(filepath_sti, filename_sti)
    
    
    # # meso
    # filepath_meso = rootpath + os.sep + staid + os.sep + 'M'
    # filename_meso = 'Z_RADR_I_Z9852_20230421135042_P_DOR_CDD_M_NUL_200_NUL_FMT.bin'
    # allresult_meso = decoder.read_mda(filepath_meso, filename_meso)
   

    # # tvs
    # filepath_tvs = rootpath + os.sep + staid + os.sep + 'TVS'
    # filename_tvs = 'Z_RADR_I_Z9852_20230421201451_P_DOR_CDD_TVS_NUL_200_NUL_FMT.bin'
    # allresult_tvs = decoder.read_tvs(filepath_tvs, filename_tvs)
   


    # # hail
    # filepath_hail = rootpath + os.sep + staid + os.sep + 'HI'
    # filename_hail = 'Z_RADR_I_Z9852_20230421181642_P_DOR_CDD_HI_NUL_200_NUL_FMT.bin'
    # allresult_hail = decoder.read_hda(filepath_hail, filename_hail)
   

# %%
