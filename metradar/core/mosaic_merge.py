# _*_ coding: utf-8 _*_

'''
将不同层次的mosacdata stack在一起，主要用于多进程拼图时，提高效率
ZhuWJ

'''

# %%
import pyart
import os
import numpy as np


# 将多层的临时文件在高度维度上进行拼接
def mosaic_merge(files:list):

    grids = []
    for file in files:
        if not os.path.exists(file):
            print(file + ' not exists!')
            return None
        try:
            tmpgrid = pyart.io.read_grid(file)
            grids.append(tmpgrid)
            print(tmpgrid.nz,tmpgrid.ny,tmpgrid.nx)
        except:
            print(file + ' load error!')
            return None


    # newz = list(grid1.z['data']) + list(grid2.z['data'])
    newz = []
    for nn in range(len(grids)):
        for val in grids[nn].z['data']:
            newz.append(val)
   
    # trans list to mask array
    newz = np.ma.MaskedArray(newz)

    # 对数据进行垂直方向拼接
    newgrid = pyart.io.read_grid(files[0])
    newgrid.z['data'] = newz
    newgrid.nz = len(newz)

    # 对所有的field都要进行拼接
    for field in newgrid.fields.keys():
        newdata = np.ma.zeros((newgrid.nz,newgrid.ny,newgrid.nx),dtype='float32')
        newdata[:grids[0].nz,:,:] = grids[0].fields[field]['data']
        curidx=grids[0].nz
        for nn in range(len(grids)-1):
            newdata[curidx:curidx+grids[nn+1].nz,:,:] = grids[nn+1].fields[field]['data']
            curidx +=grids[nn+1].nz
  
        newgrid.fields[field]['data'] = np.ma.MaskedArray(newdata)

    
    return newgrid

    # 如果拼接成功，那么可以调用grid3.to_xarray()来检验，看返回是否成功
    # grid3.to_xarray()


if __name__ == "__main__":

    pass