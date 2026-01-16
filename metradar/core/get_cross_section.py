# _*_ coding: utf-8 _*_

'''
从pyart的radar object中获取任意垂直剖面的数据

朱文剑


'''

import numpy as np
from pyart.core import Radar,antenna_to_cartesian
import math
# import time

R = 6371 # km
#该程序用来求直线方程系数
#目前没考虑垂直X轴的直线，需要完善
#kevin2075@163.com
def linefunc(x,y,x1,y1): #直接输入两个坐标就可以求出一条直线方程。
# if x~=x1
    a=(y1-y)/(x1-x)
    b=y-a*x
    return [a,b]


# 将多个数或列表组合成一个列表
def myconcat(vars:tuple):
    result=[]
    for vartmp in vars:
        if len(np.array(vartmp).shape)==0:
            result.append(vartmp)
        else:
            for var in vartmp:
                result.append(var)
   
    return result

def get_cross_radar(radar:Radar,params):

    # antenna_to_cartesian(147, 260, 1.5)
    # startangle=None,startrange=None,endangle=None,endrange=None
    # startangle = 8.5
    # startrange = 69.23 #km

    # endangle = 18.6;
    # endrange =  59.03 #km
    startangle = params['crs_start_azi']
    startrange = params['crs_start_range']
    endangle = params['crs_end_azi']
    endrange = params['crs_end_range']
    toph = params['top_height']

    startx = startrange*np.sin(np.radians(startangle))
    starty = startrange*np.cos(np.radians(startangle))
    endx   = endrange*np.sin(np.radians(endangle))
    endy   = endrange*np.cos(np.radians(endangle))

    baseh = 0; # km 垂直剖面的起始高度
    # toph = 20; # km 垂直剖面的上限高度

    validzlv=[]
    validvlv=[]
    validReles=[]
    validDeles=[]
    outdic=dict()

    for il in range(radar.nsweeps):
        # if sweep(il).RGates > 0
        # if il !=1 and il !=3:
        #     validzlv.append(il)
        #     validReles.append(radar.get_elevation(il).mean())
        # if il !=0 and il !=2:
        #     validvlv.append(il)
        #     validDeles.append(radar.get_elevation(il).mean())
        validzlv.append(il)
        validReles.append(round(radar.get_elevation(il).mean(),1))
        validvlv.append(il)
        validDeles.append(round(radar.get_elevation(il).mean(),1))

    tol_length = int(np.sqrt(np.power((endx-startx),2) + np.power((endy - starty),2)))

    #求方程坐标，这里暂时没考虑X=0的情况，后面要完善这一点
    [a,b] = linefunc(startx,starty,endx,endy)

    # 获取反射率垂直剖面
        
    #构建变量，确定坐标维度
    xreso = 0.05#km
    if endx < startx:
        xreso = -1*xreso

    xnum = int(tol_length  / abs(xreso))
    zreso = 0.05# km
    znum = int((toph - baseh)  / zreso)

    

    beamwidth = radar.get_azimuth(validzlv[-1])
    beamwidth = np.diff(beamwidth)
    beamwidth = np.mean(beamwidth[beamwidth>0])
    topel_R = max(validReles) + beamwidth / 2
    baseel_R = min(validReles) - beamwidth / 2

    topel_D = max(validDeles) + beamwidth / 2
    baseel_D = min(validDeles) - beamwidth / 2

    tmp=[]
    for nn in range(len(validReles)-1):
        tmp.append(np.mean([validReles[nn],validReles[nn+1]]))

    tmp_newRel = sorted(myconcat((tmp,validReles)))

    tmp=[]
    for nn in range(len(validDeles)-1):
        tmp.append(np.mean([validDeles[nn],validDeles[nn+1]]))

    tmp_newDel = sorted(tmp+validDeles)

    newRel = [baseel_R+tmp_newRel+topel_R]
    newDel = [baseel_D+tmp_newDel+topel_D]

    # for varkey in radar.fields.keys():
     
    nflag=0
    outdic=dict()
    outdic['vertical_km'] = toph - baseh
    outdic['horizontal_km'] = tol_length
    outdic['xreso'] = xreso
    outdic['yreso'] = zreso
    
    for varkey in radar.fields.keys():
        outdic[varkey]=[]
        outdic[varkey] = np.zeros([znum,xnum],dtype='float')*np.nan

    
    # 纯 for 循环版本 =====================
    valideles=[]
    validlvl=[]
    for varkey in radar.fields.keys():
        print(varkey)
        if varkey == 'reflectivity' :
            valideles = validReles
            validlvl = validzlv
        elif varkey == 'velocity':
            valideles = validDeles
            validlvl = validvlv 
        else:
            # print('暂时仅支持回波强度和径向速度')
            continue
        
        curfd = []
        for il in range(radar.nsweeps):
            curfd.append(radar.get_field(il,varkey))#sweep(targetsp).dbz(az,gates);

        for iz in range(znum):
            for ix in range(xnum):
                tmpcurx = startx + (ix * xreso) * math.cos(math.atan(a))
                
                tmpcury = a * tmpcurx + b
                tmpaz = (90 - math.degrees(math.atan2(tmpcury,tmpcurx)) + 360)%360
                
                curx = math.sqrt(math.pow(tmpcurx,2) + math.pow(tmpcury,2))#startrange + xreso * ix 
                curz = baseh + iz * zreso
                
                #计算相对雷达的仰角
                # ele = math.degrees(math.atan2(curz,curx))
                # 这里必须用math.pow，如果直接用称号会有较大误差
                # print('R=',R,' curz=',curz,' curx=',curx)
                ele = math.asin((math.pow((curz+R),2) - math.pow(curx,2) - math.pow(R,2))/2/curx/R)/np.pi*180

                if ele < baseel_R or ele > topel_R:
                    continue

                tmp = list(abs(np.array(valideles) - ele))
                idx = tmp.index(min(tmp))
                idx_el = idx
                
                targetsp = validlvl[idx_el]
                
                #获取方位角角标
                tmp = list(abs(radar.get_azimuth(targetsp) - tmpaz))
                idx = tmp.index(min(tmp))
                az = idx
                
                #经向距离，由该页面的公式进行反算
                #https://arm-doe.github.io/pyart/API/generated/pyart.core.antenna_to_cartesian.html#pyart.core.antenna_to_cartesian
                #根据s=R*??的公式反算r
                
                cur_range = (R+curz)*math.sin(curx/R)/math.cos(math.radians(valideles[idx_el]))
                gates = math.floor(cur_range/(radar.range['meters_between_gates'] / 1000)+0.5)-1
                # gates = math.floor(curx/math.cos(math.radians(valideles[idx_el]))/(radar.range['meters_between_gates'] / 1000)+0.5)-1
                
                if gates < radar.ngates:
                    #disp(sweep(targetsp).dbz(az,gates));
                    # if curfd[targetsp].data[az,gates] > -30 and not curfd[targetsp].mask[az,gates]:
                    # if curfd[targetsp].data[az,gates] > -5:
                    if not curfd[targetsp].mask[az,gates]:
                        outdic[varkey][iz,ix] = curfd[targetsp].data[az,gates]
                        nflag+=1
    
    return outdic
