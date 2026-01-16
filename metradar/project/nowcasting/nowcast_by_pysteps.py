
# _*_ coding: utf-8 _*_

# 利用pysteps进行临近预报的框架程序

import matplotlib
matplotlib.use('agg')
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint
from pysteps import io, motion, nowcasts, rcparams, verification
from pysteps.utils import conversion, transformation
from pysteps.visualization import plot_precip_field, quiver
import os
import xarray as xr
from metradar.graph.draw_latlon_func import draw_mosaic
from datetime import datetime, timedelta

###############################################################################
# Read the radar input images
# ---------------------------
#
# First, we will import the sequence of radar composites.
# You need the pysteps-data archive downloaded and the pystepsrc file
# configured with the data_source paths pointing to data folders.

# Selected case

date = datetime.strptime("202506091100", "%Y%m%d%H%M")
data_source = rcparams.data_sources["fmi"]
n_leadtimes = 12

###############################################################################
# Load the data from the archive
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

root_path = data_source["root_path"]
path_fmt = data_source["path_fmt"]
fn_pattern = data_source["fn_pattern"]
fn_ext = data_source["fn_ext"]
importer_name = data_source["importer"]
importer_kwargs = data_source["importer_kwargs"]
timestep = data_source["timestep"]

# Find the input files from the archive
fns = io.archive.find_by_date(
    date, root_path, path_fmt, fn_pattern, fn_ext, timestep, num_prev_files=2
)

# Read the radar composites
importer = io.get_method(importer_name, "importer")
Z, _, metadata = io.read_timeseries(fns, importer, **importer_kwargs)

# write Z to ds
# set longitude and latitude coordinates
slat = 35.5
nlat = 39.5
wlon = 115
elon = 120
lat = np.linspace(slat,nlat,Z.shape[1])
lon = np.linspace(wlon,elon,Z.shape[2])

# set time coordinates
curtime = metadata['timestamps'][-1]
curtime = np.array([curtime], dtype='datetime64[m]')
# data = np.expand_dims(data, axis=0)

# define coordinates
time_coord = ('time', curtime)
lon_coord = ('lon', lon, {
    'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
lat_coord = ('lat', lat, {
    'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})

# create xarray
varattrs = {'long_name': 'Composite Refelectivity', 
            'short_name': 'cref', 'units': 'dBZ',
            'maxv':80,
            'minv':0}
data = xr.Dataset({'cref':(['lat', 'lon'], np.flipud(Z[-1]), varattrs)},
    coords={ 'lat':lat_coord, 'lon':lon_coord})

outpath = '/mnt/e/metradar_test/pic/pysteps_nowcast/'

tstr = metadata['timestamps'][-1].strftime('%Y%m%d%H%M')
outname = '%s_obs.png'%tstr
dpi = 600
thred=10
draw_mosaic(data.cref,data.lat.data,data.lon.data,slat,nlat,wlon,elon,outpath,outname,tstr,subtitle='实况',titlecolor='k',dpi=dpi,thred=thred)



# print(outpath + os.sep + outname + ' done!')
# Convert to rain rate
# R, metadata = conversion.to_rainrate(Z, metadata)

# # Plot the rainfall field
# fig1 = plt.figure(figsize=(6, 6))
# plot_precip_field(Z[-1, :, :], geodata=metadata)
# # plt.show()

# Store the last frame for plotting it later later
# R_ = R[-1, :, :].copy()

# Log-transform the data to unit of dBR, set the threshold to 0.1 mm/h,
# set the fill value to -15 dBR
# R, metadata = transformation.dB_transform(R, metadata, threshold=0.1, zerovalue=-15.0)

# Nicely print the metadata
# pprint(metadata)

###############################################################################
# Compute the nowcast
# -------------------
#
# The extrapolation nowcast is based on the estimation of the motion field,
# which is here performed using a local tracking approach (Lucas-Kanade).
# The most recent radar rainfall field is then simply advected along this motion
# field in oder to produce an extrapolation forecast.

# Estimate the motion field with Lucas-Kanade
# st = time.time()
oflow_method = motion.get_method("LK")
V = oflow_method(Z[-3:, :, :])

# Extrapolate the last radar observation
extrapolate = nowcasts.get_method("extrapolation")
# R[~np.isfinite(R)] = metadata["zerovalue"]
Z_f = extrapolate(Z[-1, :, :], V, n_leadtimes)
for nn in range(len(Z_f)):
    fsttime = metadata['timestamps'][-1] + timedelta(minutes=10*(nn+1))
    tstr = fsttime.strftime('%Y%m%d%H%M')
    # set time coordinates
    fsttime = np.array([fsttime], dtype='datetime64[m]')
    # data = np.expand_dims(data, axis=0)

    # define coordinates
    time_coord = ('time', fsttime)
    lon_coord = ('lon', lon, {
        'long_name':'longitude', 'units':'degrees_east', '_CoordinateAxisType':'Lon'})
    lat_coord = ('lat', lat, {
        'long_name':'latitude', 'units':'degrees_north', '_CoordinateAxisType':'Lat'})

    # create xarray
    varattrs = {'long_name': 'Composite Refelectivity', 
                'short_name': 'cref', 'units': 'dBZ',
                'maxv':80,
                'minv':0}
    data = xr.Dataset({'cref':(['lat', 'lon'], np.flipud(Z_f[nn]), varattrs)},
        coords={ 'lat':lat_coord, 'lon':lon_coord})
    
    outname = '%s_fst.png'%tstr
    dpi = 600
    thred=10
    draw_mosaic(data.cref,data.lat.data,data.lon.data,slat,nlat,wlon,elon,outpath,outname,tstr,subtitle='预报',titlecolor='r',dpi=dpi,thred=thred,add_title=1,prefix_title='雷达组合反射率拼图')    
   

# Back-transform to rain rate
# R_f = transformation.dB_transform(R_f, threshold=-10.0, inverse=True)[0]

# et = time.time()
# print("Execution time(s): ", et - st)
# Plot the motion field
# fig2 = plt.figure(figsize=(6, 6))
# plot_precip_field(Z_f[-1,:,:], geodata=metadata)
# quiver(V, geodata=metadata, step=50)
# plt.show()

###############################################################################
# Verify with FSS
# ---------------
#
# The fractions skill score (FSS) provides an intuitive assessment of the
# dependency of skill on spatial scale and intensity, which makes it an ideal
# skill score for high-resolution precipitation forecasts.

# Find observations in the data archive
fns = io.archive.find_by_date(
    date,
    root_path,
    path_fmt,
    fn_pattern,
    fn_ext,
    timestep,
    num_prev_files=0,
    num_next_files=n_leadtimes,
)
# Read the radar composites
Z_o, _, metadata_o = io.read_timeseries(fns, importer, **importer_kwargs)
# R_o, metadata_o = conversion.to_rainrate(R_o, metadata_o, 223.0, 1.53)

# Compute fractions skill score (FSS) for all lead times, a set of scales and 1 mm/h
fss = verification.get_method("FSS")
scales = [2, 4, 8, 16, 32, 64, 128]
thr = 5.0
score = []
for i in range(n_leadtimes):
    score_ = []
    for scale in scales:
        score_.append(fss(Z_f[i, :, :], Z_o[i + 1, :, :], thr, scale))
    score.append(score_)

# plt.figure()
# fig3 = plt.figure(figsize=(6, 6))
# x = np.arange(1, n_leadtimes + 1) * timestep
# plt.plot(x, score)
# plt.legend(scales, title="Scale [km]")
# plt.xlabel("Lead time [min]")
# plt.ylabel("FSS ( > 5 dBZ ) ")
# plt.title("Fractions skill score")
# plt.show()

# sphinx_gallery_thumbnail_number = 3