'''
读取新的雷达拼图数据
朱文剑


数据格式说明

//
/////////////////////////////////////////////////////////////////////
//  组网定量估测降水系统拼图产品文件规则
//  1. 文件里命名为:
//		ACHN_XXX_20121012_090000_nn.bin
// 2. 文件格式:文件头 + 数据块 
// 3. 数据从西北，往东南写
//***************************************
typedef struct tagMQPE_MosiacProductHeaderStructure
{
	// 文件卷标，12字节(0-11), 
	char  label[4];	    //0-3	文件固定标识：MOC 
	char  Version[4];	//4-7	文件格式版本代码，如: 1.0，1.1，etc
	int   FileBytes;	//8-11	包含头信息在内的文件字节数，不超过2M

						//产品描述, 84字节(12-95) 
	short MosaicID;		  //12-13 拼图产品编号
	short coordinate;	  //14-15 坐标类型: 2=笛卡儿坐标,3=等经纬网格坐标
	char  varname[8];     //16-23 产品代码,如: ET,VIL,CR,CAP,OHP,OHPC
	char  description[64];//24-87 产品描述,如Composite Reflectivity mosaic
	int   BlockPos; //88-91 产品数据起始位置(字节顺序)
	int   BlockLen; //92-95  产品数据字节数

					//数据时间, 28字节(96-123) 
	int TimeZone;   //96-99 数据时钟,0=世界时,28800=北京时
	short yr;		//100-101	观测时间中的年份
	short mon;	    //102-103	观测时间中的月份（1－12）
	short day;		//104-105	观测时间中的日期（1－31）
	short hr;		//106-107	观测时间中的小时（00－23）
	short min;	    //108-109	观测时间中的分（00－59）
	short sec;	    //110-111	观测时间中的秒（00－59）
	int   ObsSeconds; //112-115	观测时间的seconds 
	unsigned short ObsDates; //116-117 观测时间中的Julian dates
	unsigned short GenDates; //118-119 产品处理时间的天数
	int   GenSeconds;   //120-123 产品处理时间的描述

						//数据区信息, 48字节(124-171) 
	int  edge_s;  //124-127 数据区的南边界，单位：1/1000度，放大1千倍
	int  edge_w;  //128-131 数据区的西边界，单位：1/1000度，放大1千倍
	int  edge_n;  //132-135 数据区的北边界，单位：1/1000度，放大1千倍
	int  edge_e;  //136-139 数据区的东边界，单位：1/1000度，放大1千倍
	int  cx;      //140-143 数据区中心坐标，单位：1/1000度，放大1千倍
	int  cy;      //144-147 数据区中心坐标，单位：1/1000度，放大1千倍
	int  nX;      //148-151 格点坐标为列数,
	int  nY;      //152-155 格点坐标为行数,
	int  dx;      //156-159 格点坐标为列分辨率，单位：1/10000度，放大1万倍
	int  dy;      //160-163 格点坐标为行分辨率，单位：1/10000度，放大1万倍
	short height; //164-165 雷达高度
	short Compress; //166-167 数据压缩标识, 0=无,1=bz2,2=zip,3=lzw
	int   num_of_radars; //168-171有多少个雷达进行了拼图

	int UnZipBytes; //数据段压缩前的字节数, //172-175
	short scale;     //176-177
	short unUsed; //178-179
	char  RgnID[8];  //180-187
	char units[8];  //180-187
	char  reserved[60];
}MOSAIC_PRODUCTHEADER;

-32768表示空白


'''

# %%
CODE1 = 'B'
CODE2 = 'H'
INT1 = 'B'
INT2 = 'H'
INT4 = 'I'
REAL4 = 'f'
REAL8 = 'd'
SINT1 = 'b'
SINT2 = 'h'
SINT4 = 'i'

MOSAIC_HEADER = (
    ('label', '4s'),
    ('Version', '4s'),
    ('FileBytes', INT4),
    ('MosaicID', INT2),
    ('coordinate', INT2),
    ('varname', '8s'),
	('description', '64s'),
    ('BlockPos', INT4),
    ('BlockLen', INT4),
    ('TimeZone', INT4),
    ('year', INT2),
    ('month', INT2),
	('day', INT2),
    ('hour', INT2),
    ('min', INT2),
    ('sec', INT2),
    ('ObsSeconds', INT4),
    ('ObsDates', INT2),
	('GenDates', INT2),
    ('GenSeconds', INT4),
    ('edge_s', INT4),
    ('edge_w', INT4),
    ('edge_n', INT4),
    ('edge_e', INT4),
	('cx', INT4),
    ('cy', INT4),
    ('nX', INT4),
    ('nY', INT4),
    ('dx', INT4),
    ('dy', INT4),
	('height', INT2),
    ('Compress', INT2),
    ('num_of_radars', INT4),
    ('UnZipBytes', INT4),
    ('scale', INT2),
    ('unUsed', INT2),
	('RgnID', '8s'),
	('units', '8s'),
	('reserved', '60s'),

)

import os
import numpy as np
import struct
import bz2
import xarray as xr
from datetime import datetime,timedelta


from multiprocessing import cpu_count, Pool,freeze_support

def _structure_size(structure):
        """ Find the size of a structure in bytes. """
        return struct.calcsize('<' + ''.join([i[1] for i in structure]))


def _unpack_from_buf( buf, pos, structure):
    """ Unpack a structure from a buffer. """
    size = _structure_size(structure)
    return _unpack_structure(buf[pos:pos + size], structure)


def _unpack_structure(string, structure):
    """ Unpack a structure from a string """
    fmt = '<' + ''.join([i[1] for i in structure])  # little-endian
    lst = struct.unpack(fmt, string)
    return dict(zip([i[0] for i in structure], lst))

def decode_mosaic(filepath,filename,minv=10,maxv = 80):

	if not os.path.exists(filepath+os.sep + filename):
		print(filepath+os.sep+filename + ' not exists!')
		return None,None
	try:
		fin = open(filepath + os.sep + filename,'rb')
	except:
		return None,None

	buf = fin.read()
	fin.close()
	pos = 0
	if len(buf)==0:
		return None
	dic_gh = _unpack_from_buf(buf, pos, MOSAIC_HEADER)
	pos +=_structure_size(MOSAIC_HEADER)
	databuf = bz2.decompress(buf[pos:])
	data = np.frombuffer(databuf, 'int16')
	data = np.reshape(data,[dic_gh['nY'],dic_gh['nX']])
	data=data.astype('float')
	# data[data == -32768] = np.NAN #  空白
	# data[data == -1280] = np.NAN # 有覆盖但无数据
	data[data < -300] = np.NAN
	data = np.flipud(data)/dic_gh['scale']
	data[data < int(minv)] = np.NAN
	data[data > int(maxv)] = np.NAN

	# set longitude and latitude coordinates
	lat = dic_gh['edge_s']/1000 + np.arange(dic_gh['nY'])*dic_gh['dy']/10000
	lon = dic_gh['edge_w']/1000 + np.arange(dic_gh['nX'])*dic_gh['dx']/10000

	# set time coordinates
	time = datetime(dic_gh['year'],dic_gh['month'],dic_gh['day'],dic_gh['hour'],dic_gh['min'],dic_gh['sec'])
	time = np.array([time], dtype='datetime64[m]')
	# data = np.expand_dims(data, axis=0)
	tmpdata = np.reshape(data, (1, data.shape[0], data.shape[1]))
	# define coordinates
	lev_coord = ('lev', [dic_gh['height'],], {'long_name':'height', 'units':'m', '_CoordinateAxisType':'Height'})
	lon_coord = ('lon', lon, {
	    'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
	lat_coord = ('lat', lat, {
	    'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})

	# create xarray
	varattrs = {'long_name': 'Refelectivity', 
	            'short_name': 'ref', 'units': 'dBZ',
				'maxv':maxv,
				'minv':minv}
	data = xr.Dataset({'ref':(['lev','lat', 'lon'], tmpdata, varattrs)},coords={ 'lev':lev_coord,'lat':lat_coord, 'lon':lon_coord})

	# add attributes
	data.attrs['Conventions'] = "CF-1.6"
	data.attrs['Origin'] = 'cmadaas'
	data.attrs['author'] = 'ZWJ'
	
	return data

def trans_single(param):

	filepath = param['filepath']
	filename = param['filename']
	outpath = param['outpath']
	pass
	varflag = 'unknown'
	if filename.find('CREF') > 0:
		varflag = 'CREF000'
	elif filename.find('QREF') > 0:
		varflag = 'QREF000'
	elif filename.find('_CAP_') > 0:
		varflag = 'CAP'
	if varflag == 'CAP':
		outname = 'ACHN.' + varflag + '.' + filename.split('_')[4][0:8] +'.' + \
			       filename.split('_')[4][8:14] + '_' + filename.split('_')[-1].split('.')[0] + '.nc'
	else:
		# outname = 'ACHN.' + varflag + '.' + filename.split('_')[4][0:8] +'.' + filename.split('_')[4][8:14] + '.nc'
		outname = 'ACHN.' + varflag + '.' + filename.split('_')[2] +'.' + filename.split('_')[3][0:6] + '.nc'
	if os.path.exists(outpath + os.sep + outname):
		print(outpath + os.sep + outname + ' already exists!')
		return True
	try:
		data=decode_mosaic(filepath,filename,minv=0,maxv=80)
		if data is None:
			return False
		datakey = list(data.keys())
		if 'cref' not in datakey:
			#将ref变量rename为cref
			data=data.rename({datakey[0]:'cref'})
		if not os.path.exists(outpath):
			os.makedirs(outpath)
		data.to_netcdf(outpath + os.sep + outname,encoding={'cref': {'zlib': True}})
		print(outpath + os.sep + outname + ' done!')
	except Exception as e:
		print(e)
		print(outpath + os.sep + outname + ' make error!')

# 批量转nc文件
def trans_bin_nc_batch(inpath,outpath,):

	if not os.path.exists(inpath):
		print(inpath + ' not exists!')
	if not os.path.exists(outpath):
		os.makedirs(outpath)

	files = os.listdir(inpath)
	files=sorted(files)
	if len(files)==0:
		print('no valid files')
		return False
	
	params=[]
	for filename in files:
		if filename.startswith('.') or filename.startswith('..'):
			continue
		if not filename.endswith('.bin'):
			continue
		curparam=dict()
		curparam['filepath'] = inpath
		curparam['filename'] = filename
		curparam['outpath'] = outpath# + os.sep + filename.replace('.bin','.nc')#filename.split('_')[9][0:4]
		params.append(curparam)
		

	# 多进程处理
	MAXP = int(cpu_count()*0.6)
	pools = Pool(MAXP)

	pools.map(trans_single, params)
	pools.close()
	pools.join()

if __name__ == '__main__':


	sourcepath = r'/Users/wenjianzhu/Downloads/CMADAAS/rdmosaic_bin'
	outpath = r'/Users/wenjianzhu/Downloads/CMADAAS/mosaicdata_nc'
	starttime = '20230701'
	endtime = '20230702'
	startt = datetime.strptime(starttime,'%Y%m%d')
	endt = datetime.strptime(endtime,'%Y%m%d')
	curt = startt
	# ACHN.CREF000.20221001.143000.nc
	prepath = ''
	while curt <= endt:
		pass
		curpath = sourcepath + os.sep + curt.strftime('%Y%m')
		if prepath == curpath:
			curt  = curt + timedelta(days=1)
			continue
		if not os.path.exists(curpath):
			curt  = curt + timedelta(days=1)
			continue
		
		trans_bin_nc_batch(curpath,outpath)
		curt  = curt + timedelta(days=1)
		prepath = curpath
