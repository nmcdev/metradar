#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 将FMT格式数据直接解析为pyart的radar object格式
# Wenjian Zhu
# 

import os

from pyart.io.nexrad_archive import _find_range_params,_find_scans_to_interp,_interpolate_scan
from pyart.io.common import make_time_unit_str, _test_arguments, prepare_for_read
from pyart.config import FileMetadata, get_fillvalue
from pyart.core.radar import Radar
from .cnrad_level2 import CNRADLevel2File
import warnings
import numpy as np


mapping = {
    "REF": "reflectivity",
    "VEL": "velocity",
    "SW": "spectrum_width",
    "PHI": "differential_phase",
    "ZDR": "differential_reflectivity",
    "RHO": "cross_correlation_ratio",
}

def read_cnrad_fmt(filename, field_names=None, additional_metadata=None,
                        file_field_names=False, exclude_fields=None,
                        include_fields=None, delay_field_loading=False,
                        station=None, scans=None,
                        linear_interp=True, **kwargs):
    # test for non empty kwargs
    _test_arguments(kwargs)

    # create metadata retrieval object
    filemetadata = FileMetadata('nexrad_archive', field_names,
                                additional_metadata, file_field_names,
                                exclude_fields, include_fields)

    # open the file and retrieve scan information
    nfile = CNRADLevel2File(prepare_for_read(filename))
    scan_info = nfile.scan_info(scans)

    # time
    time = filemetadata('time')
    time_start, _time = nfile.get_times(scans)
    time['data'] = _time
    time['units'] = make_time_unit_str(time_start)

    # range
    _range = filemetadata('range')
    first_gate, gate_spacing, last_gate = _find_range_params(
        scan_info, filemetadata)
    _range['data'] = np.arange(first_gate, last_gate, gate_spacing, 'float32')
    _range['meters_to_center_of_first_gate'] = float(first_gate)
    _range['meters_between_gates'] = float(gate_spacing)

    # metadata
    metadata = filemetadata('metadata')
    metadata['original_container'] = 'CINRAD_FMT'
    vcp_pattern = nfile.get_vcp_pattern()
    if vcp_pattern is not None:
        metadata['vcp_pattern'] = vcp_pattern
    if 'icao' in nfile.volume_header.keys():
        metadata['instrument_name'] = nfile.volume_header['icao'].decode()

    # scan_type
    scan_type = 'ppi'

    # latitude, longitude, altitude
    latitude = filemetadata('latitude')
    longitude = filemetadata('longitude')
    altitude = filemetadata('altitude')

    # if nfile._msg_type == '1' and station is not None:
    #     lat, lon, alt = get_nexrad_location(station)
    # elif 'icao' in nfile.volume_header.keys() and nfile.volume_header['icao'].decode()[0] == 'T':
    #     lat, lon, alt = get_nexrad_location(
    #         nfile.volume_header['icao'].decode())
    # else:
    #     lat, lon, alt = nfile.location()

    lat, lon, alt = nfile.location()

    latitude['data'] = np.array([lat], dtype='float64')
    longitude['data'] = np.array([lon], dtype='float64')
    altitude['data'] = np.array([alt], dtype='float64')

    # sweep_number, sweep_mode, fixed_angle, sweep_start_ray_index
    # sweep_end_ray_index
    sweep_number = filemetadata('sweep_number')
    sweep_mode = filemetadata('sweep_mode')
    sweep_start_ray_index = filemetadata('sweep_start_ray_index')
    sweep_end_ray_index = filemetadata('sweep_end_ray_index')

    if scans is None:
        nsweeps = int(nfile.nscans)
    else:
        nsweeps = len(scans)
    sweep_number['data'] = np.arange(nsweeps, dtype='int32')
    sweep_mode['data'] = np.array(
        nsweeps * ['azimuth_surveillance'], dtype='S')

    rays_per_scan = [s['nrays'] for s in scan_info]
    sweep_end_ray_index['data'] = np.cumsum(rays_per_scan, dtype='int32') - 1

    rays_per_scan.insert(0, 0)
    sweep_start_ray_index['data'] = np.cumsum(
        rays_per_scan[:-1], dtype='int32')

    # azimuth, elevation, fixed_angle
    azimuth = filemetadata('azimuth')
    elevation = filemetadata('elevation')
    fixed_angle = filemetadata('fixed_angle')
    azimuth['data'] = nfile.get_azimuth_angles(scans)
    elevation['data'] = nfile.get_elevation_angles(scans).astype('float32')
    fixed_agl = []
    for i in nfile.get_target_angles(scans):
        if i > 180:
            i = i - 360.
            warnings.warn("Fixed_angle(s) greater than 180 degrees present."
                          + " Assuming angle to be negative so subtrating 360",
                          UserWarning)
        else:
            i = i
        fixed_agl.append(i)
    fixed_angles = np.array(fixed_agl, dtype='float32')
    fixed_angle['data'] = fixed_angles

    # fields
    max_ngates = len(_range['data'])
    available_moments = set([m for scan in scan_info for m in scan['moments']])
    interpolate = _find_scans_to_interp(
        scan_info, first_gate, gate_spacing, filemetadata)

    fields = {}
    for moment in available_moments:
        field_name = filemetadata.get_field_name(moment)
        if field_name is None:
            continue
        dic = filemetadata(field_name)
        dic['_FillValue'] = get_fillvalue()
        if delay_field_loading and moment not in interpolate:
            # dic = LazyLoadDict(dic)
            # data_call = _NEXRADLevel2StagedField(
            #     nfile, moment, max_ngates, scans)
            # dic.set_lazy('data', data_call)
            pass
        else:
            mdata = nfile.get_data(moment, max_ngates, scans=scans)
            if moment in interpolate:
                interp_scans = interpolate[moment]
                warnings.warn(
                    "Gate spacing is not constant, interpolating data in " +
                    "scans %s for moment %s." % (interp_scans, moment),
                    UserWarning)
                for scan in interp_scans:
                    idx = scan_info[scan]['moments'].index(moment)
                    moment_ngates = scan_info[scan]['ngates'][idx]
                    start = sweep_start_ray_index['data'][scan]
                    end = sweep_end_ray_index['data'][scan]
                    if interpolate['multiplier'] == '4':
                        multiplier = '4'
                    else:
                        multiplier = '2'
                    _interpolate_scan(mdata, start, end, moment_ngates,
                                      multiplier, linear_interp)
            dic['data'] = mdata
        fields[field_name] = dic

    # instrument_parameters
    nyquist_velocity = filemetadata('nyquist_velocity')
    unambiguous_range = filemetadata('unambiguous_range')
    nyquist_velocity['data'] = nfile.get_nyquist_vel(scans).astype('float32')
    unambiguous_range['data'] = (
        nfile.get_unambigous_range(scans).astype('float32'))

    radar_beam_width_h = filemetadata('radar_beam_width_h')
    radar_beam_width_v = filemetadata('radar_beam_width_v')
    radar_beam_width_h['data'] = nfile.get_beam_width_h()
    radar_beam_width_v['data'] = nfile.get_beam_width_v()
    
    radar_atenna_gain = filemetadata('radar_antenna_gain')
    radar_receiver_bandwidth = filemetadata('radar_receiver_bandwidth')
    radar_atenna_gain['data'] = nfile.get_antenna_gain()
    radar_receiver_bandwidth['data'] = nfile.get_receiver_bandwidth()
    
    instrument_parameters = {'unambiguous_range': unambiguous_range,
                             'nyquist_velocity': nyquist_velocity, 
                             'radar_beam_width_h': radar_beam_width_h,
                             'radar_beam_width_v': radar_beam_width_v,
                             'radar_antenna_gain_h': radar_atenna_gain,
                             'radar_antenna_gain_v': radar_atenna_gain,
                             'radar_receiver_bandwidth': radar_receiver_bandwidth,
                             }

    nfile.close()
    return Radar(
        time, _range, fields, metadata, scan_type,
        latitude, longitude, altitude,
        sweep_number, sweep_mode, fixed_angle, sweep_start_ray_index,
        sweep_end_ray_index,
        azimuth, elevation,
        instrument_parameters=instrument_parameters)

if __name__ == "__main__":
    pass
    
    filepath = './testdata/FMT/'
    filename = 'Z_RADR_I_Z9417_20210910154708_O_DOR_SAD_CAP_FMT.bin'

    radar = read_cnrad_fmt(filepath + os.sep + filename)

    print(list(radar.fields.keys()))
    pass