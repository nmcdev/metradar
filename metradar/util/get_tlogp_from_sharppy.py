'''
从sharppy格式的探空数据中获取风廓线，作为背景场

朱文剑
'''

# %%
import os
import re
import numpy as np
from pyart.core.wind_profile import HorizontalWindProfile
import warnings
warnings.filterwarnings("ignore")

def get_profile(filepath,filename):
    """
    该函数读取sharppy格式的探空文件，提取高度、风速、风向等信息，构建并返回一个HorizontalWindProfile对象，便于后续风廓线分析和可视化。
    Parameters
    ----------
    filepath : str
        文件路径
    filename : str
        文件名
    Returns
    -------
    outprofile : HorizontalWindProfile
    
    补充说明：
    
    """

    fin = open(filepath + os.sep + filename,'rt')

    nflag_title=0
    nflag_raw=0
    nflag=0
    profile=dict()
    defaultvalue = '99999'
    for line in fin.readlines():
        # print(line)
        nflag = nflag + 1
        if line.find('%TITLE%') >=0 :
            nflag_title = nflag
        if line.find('%RAW%') >=0 :
            nflag_raw = nflag

            # 遇到文档结束
        if line.find('%END%') >=0 :
            break
        #获取站号，时间信息
        if nflag == nflag_title + 1:
            profile['staname'] = line.split(' ')[0]
        
        #获取经纬度信息
        if nflag == nflag_title + 2:
            tmp = re.findall(r"\d+\.?\d*",line)
            if len(tmp) == 2:
                profile['lon'] = tmp[0]
                profile['lat'] = tmp[1]
            else:
                profile['lon'] = defaultvalue
                profile['lat'] = defaultvalue
        
        #获取变量名称
        if nflag == nflag_title + 3:
            tmp = line.split(' ')
            varnames = [i.strip('\n') for i in tmp if i != '']
            # print(varnames)
            if len(varnames) ==6:
                for dd in range(len(varnames)):
                    profile[varnames[dd]]=[]
                    # exec('{} = []'.format(varnames[dd]))
            else:
                print('变量个数不够，请检查')
                break

        # 数据
        if nflag > nflag_raw:
            tmp = re.findall(r"\d+\.?\d*",line)
            if len(tmp) ==6:
                values = np.fromstring( line, sep=',')
                value_str = [str(value) for value in values]
                for dd in range(len(value_str)):
                    # exec('{}.append({})'.format(varnames[dd], value_str[dd]))
                    profile[varnames[dd]].append(values[dd])
            else:
                continue

    outprofile = HorizontalWindProfile(
            profile['HGHT'], np.array(profile['WSPD'])/1.94, profile['WDIR'], latitude=np.tile(float(profile['lat']), len(profile['HGHT'])), longitude=np.tile(float(profile['lon']), len(profile['HGHT'])))

    fin.close()
    print('profile read over!')
    return outprofile   

if __name__ == '__main__':
    filepath = 'tlogp'
    filename = '22060608.59280'
    profile = get_profile(filepath,filename)
    pass



# %%
