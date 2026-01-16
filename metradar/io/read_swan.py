
# _*_ coding: utf-8 _*_

'''
读取swan格点数据

'''

# %%
import numpy as np
import xarray as xr
import bz2
import os
from datetime import datetime, timedelta
from metradar.graph.draw_latlon_func import draw_latlon


# define head structure
head_dtype = [
    ('ZonName', 'S12'),
    ('DataName', 'S38'),
    ('Flag', 'S8'),
    ('Version', 'S8'),
    ('year', 'i2'),
    ('month', 'i2'),     
    ('day', 'i2'),     
    ('hour', 'i2'),    
    ('minute', 'i2'),
    ('interval', 'i2'),
    ('XNumGrids', 'i2'),
    ('YNumGrids', 'i2'),
    ('ZNumGrids', 'i2'),
    ('RadarCount', 'i4'),
    ('StartLon', 'f4'),
    ('StartLat', 'f4'),
    ('CenterLon', 'f4'),
    ('CenterLat', 'f4'),
    ('XReso', 'f4'),
    ('YReso', 'f4'),
    ('ZhighGrids', 'f4', 40),
    ('RadarStationName', 'S20', 16),
    ('RadarLongitude', 'f4', 20),
    ('RadarLatitude', 'f4', 20),
    ('RadarAltitude', 'f4', 20),
    ('MosaicFlag', 'S1', 20),
    ('m_iDataType', 'i2'),
    ('m_iLevelDimension', 'i2'),
    ('offset','f4'),
    ('scale','f4'),
    ('Reserved', 'S160')]


def decode_swan(filepath,filename,scale=None,varattrs={'long_name': 'mosaic composite reflectivity', 'short_name': 'QPF', 'units': 'mm'}
                ,attach_forecast_period=False):

    if not os.path.exists(filepath+os.sep+filename):
        print(filepath + os.sep + filename + ' not exists!')
        return None
        
    fp = open(filepath + os.sep + filename, 'rb')

    if filename.find('.bz2') > 0:
        
        tmpbuf = fp.read()
        byteArray = bz2.decompress(tmpbuf)
    else:
        byteArray = fp.read()

    # read head information
    head_info = np.frombuffer(byteArray[0:1024], dtype=head_dtype)
    ind = 1024

    # get coordinates
    nlon = head_info['XNumGrids'][0].astype(np.int64)
    nlat = head_info['YNumGrids'][0].astype(np.int64)
    nlev = head_info['ZNumGrids'][0].astype(np.int64)
    dlon = head_info['XReso'][0].astype(np.float32)
    dlat = head_info['YReso'][0].astype(np.float32)
    lat = head_info['StartLat'][0] - np.arange(nlat)*dlat - dlat/2.0
    lon = head_info['StartLon'][0] + np.arange(nlon)*dlon - dlon/2.0
    level = head_info['ZhighGrids'][0][0:nlev]
    cur_scale = head_info['scale'][0].astype(np.float32)
    cur_offset = head_info['offset'][0].astype(np.float32)

    # retrieve data records
    data_type = ['u1', 'u1', 'u2', 'i2']
    data_type = data_type[head_info['m_iDataType'][0]]
    data_len = (nlon * nlat * nlev)
    data = np.frombuffer(
        byteArray[ind:(ind + data_len*int(data_type[1]))], 
        dtype=data_type, count=data_len)

    # convert data type
    data.shape = (nlev, nlat, nlon)
    data = data.astype(np.float32)
    if not scale is None:
        data = (data + scale[1]) * scale[0]
    else:
        data = data * cur_scale + cur_offset

    # reverse latitude axis
    # if len(data.shape) == 3:
    #     data = np.flip(data, 2)
    # else:
    data = np.flip(data, 1)
    lat = lat[::-1]

    # set time coordinates
    init_time = datetime(
        head_info['year'][0], head_info['month'][0], 
        head_info['day'][0], head_info['hour'][0], head_info['minute'][0])
    if attach_forecast_period:
        fhour = int(filename.split('.')[1])/60.0
    else:
        fhour = 0
    fhour = np.array([fhour], dtype=np.float64)
    time = init_time + timedelta(hours=fhour[0])
    init_time = np.array([init_time], dtype='datetime64[ns]')
    time = np.array([time], dtype='datetime64[ns]')

    # define coordinates
    time_coord = ('time', time)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})
    level_coord = ('level', level, {
        'long_name':'height', 'units':'m'})

    # create xarray
    data = np.expand_dims(data, axis=0)
    data = xr.Dataset({'data':(['time', 'level', 'lat', 'lon'], data, varattrs)},
        coords={'time':time_coord, 'level':level_coord, 'lat':lat_coord, 'lon':lon_coord})

    # add time coordinates
    data.coords['forecast_reference_time'] = init_time[0]
    data.coords['forecast_period'] = ('time', fhour, {
        'long_name':'forecast_period', 'units':'hour'})

    # add attributes
    data.attrs['Conventions'] = "CF-1.6"

    return data



if __name__ == "__main__":
#     pass

# %%
    # testdata/SWAN/

    # prod_type = 'MCR'
    prod_type = 'MCC'
    # prod_type = 'MKDP'
    # prod_type = 'MZDR'
    # prod_type = 'HCAMOSAIC'
    # prod_type = 'TOP'
    # prod_type = 'VIL'

    timestr = '20230316113000'
    filepath = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/testdata/SWAN'

    # filename = 'Z_OTHE_RADAMCR_20230316113000.bin.bz2'
    # filename = 'Z_OTHE_RADAMOSAIC_20230316113000.bin.bz2'
    # filename = 'Z_SWAN_RADAMCC_20230316113000.bin.bz2'
    # filename = 'Z_SWAN_RADAMKDP_20230316113000.bin.bz2'
    # filename = 'Z_SWAN_RADAMZDR_20230316113000.bin.bz2'
    # filename = 'Z_SWAN_RADAMHCA_20230316114000.bin.bz2'
    # filename = 'Z_SWAN_RADAMHCA_20220904110000.bin.bz2'

    if prod_type == 'TDMOSAIC':
        filename = 'Z_OTHE_RADAMOSAIC_' + timestr + '.bin.bz2'
    elif prod_type == 'MCC':
        filename = 'Z_SWAN_RADAMCC_' + timestr + '.bin.bz2'
    elif prod_type == 'MKDP':
        filename = 'Z_SWAN_RADAMKDP_' + timestr + '.bin.bz2'
    elif prod_type == 'MZDR':
        filename = 'Z_SWAN_RADAMZDR_' + timestr + '.bin.bz2'
    elif prod_type == 'HCAMOSAIC':
        filename = 'Z_SWAN_RADAMHCA_' + timestr + '.bin.bz2'
    elif prod_type == 'MCR':
        filename = 'Z_OTHE_RADAMCR_' + timestr + '.bin.bz2'
    elif prod_type == 'TOP':
        filename = 'Z_OTHE_RADAMTOP_' + timestr + '.bin.bz2'
    elif prod_type == 'VIL':
        filename = 'Z_OTHE_RADAMVIL_' + timestr + '.bin.bz2'


    outpath = '/home/wjzhu/OneDrive/PythonCode/MyWork/metradar/pic/SWAN'
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    print('reading file: ',filepath + os.sep + filename)
    if prod_type == 'MCR'  or prod_type == 'TOP' or prod_type == 'VIL':
        lev=0
    else:
        lev = 5
    print('level index: ',lev)
    data = decode_swan(filepath,filename,) # scale=[0.5,-64]
    outname = filename + '_%.1fkm.png'%data.level[lev].values

    slat = 24.5
    nlat = 29.5
    wlon = 103.5
    elon = 109.5
    # slat = float(data.lat.min().data)
    # nlat = float(data.lat.max().data)
    # wlon = float(data.lon.min().data)
    # elon = float(data.lon.max().data)

    newdata = data.data.sel(lat=slice(slat,nlat),lon=slice(wlon,elon))
    # ttdata = newdata.isel(time=0,level=5)
    # plt.show()
    # %%
    # newdata.isel(time=0,level=5).to_netcdf('/Users/wenjianzhu/Downloads/test.nc')
    tstr = filename.split('_')[3].split('.')[0]

    # CC的数据是从0-1，所以要处理一下

    if filename.find('MCR') > 0 :
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,
                outpath,outname,tstr,prefix_title='CR',dpi=800,units='dBZ',thred=0,)
    if filename.find('TOP') > 0 :
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,
                outpath,outname,tstr,prefix_title='ET',dpi=800,units='km',thred=2,)
    if filename.find('VIL') > 0 :
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,
                outpath,outname,tstr,prefix_title='VIL',dpi=800,units='kg/m2',thred=10,)
    elif filename.find('MCC') > 0 :
        draw_latlon(newdata.data[0,lev,:,:]* 100,newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,
                outpath,outname,tstr,prefix_title='CAPPI_CC_%.1fkm_'%data.level[lev].values,dpi=800,units='%',thred=0,)
    elif filename.find('MKDP') > 0 :
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,
                    outpath,outname,tstr,prefix_title='CAPPI_KDP_%.1fkm_'%data.level[lev].values,dpi=800,units='deg/km',thred=-2)
    elif filename.find('MZDR') > 0: 
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,
                outpath,outname,tstr,prefix_title='CAPPI_ZDR_%.1fkm_'%data.level[lev].values,dpi=800,units='dB',thred=-4,)
    elif filename.find('RADAMOSAIC') > 0:
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,outpath,
                    outname,tstr,prefix_title='CAPPI_REF_%.1fkm_'%data.level[lev].values,dpi=800,units='dBZ',thred=2,)
    elif filename.find('HCA')  > 0:
        draw_latlon(newdata.data[0,lev,:,:],newdata.lat.data,newdata.lon.data,slat,nlat,wlon,elon,outpath,
                    outname,tstr,prefix_title='CAPPI_HCA_%.1fkm_'%data.level[lev].values,dpi=800,units='cat',thred=0,)
    pass

    '''

    HCA  空=0 地物回波=1 晴空回波=2 干雪=3 湿雪=4 冰晶=5 霰=6  大雨滴=7  小到中雨=8  大雨=9   冰雹=10

    '''