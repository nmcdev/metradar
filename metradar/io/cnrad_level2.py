
# _*_ coding: utf-8 _*_

"""
Functions for reading CINRAD level 2 files.
some functions are based on pyart
https://github.com/ARM-DOE/pyart
by Wenjian Zhu

"""


import bz2
from datetime import datetime, timedelta
import struct
import warnings
import re
import numpy as np


class CNRADLevel2File(object):
    """
    Class for accessing data in a CINRAD Level II file.
    format file

    Parameters
    ----------
    filename : str
        Filename of Archive II file to read.

    Attributes
    ----------
    radial_records : list
        Radial (1 or 31) messages in the file.
    nscans : int
        Number of scans in the file.
    scan_msgs : list of arrays
        Each element specifies the indices of the message in the
        radial_records attribute which belong to a given scan.
    volume_header : dict
        Volume header.
    vcp : dict
        VCP information dictionary.
    _records : list
        A list of all records (message) in the file.
    _fh : file-like
        File like object from which data is read.
    _msg_type : '31' or '1':
        Type of radial messages in file.


    """

    def __init__(self, filename):
        """ initalize the object. """
        # read in the volume header and compression_record

        if hasattr(filename, 'read'):
            fh = filename
        else:
            fh = open(filename, 'rb')
        buf = fh.read()
        self._fh = fh
        pos = 0
        # 获取通用信息头
        dic_gh = _unpack_from_buf(buf, pos, GENERIC_HEADER)
        if dic_gh['magic_number'] != 0x4D545352:
            print('源数据格式错误！')
            return
        pos += _structure_size(GENERIC_HEADER)

        # 读取站点配置
        self.dic_stcfg = _unpack_from_buf(buf, pos, SITE_CONFIG)
        pos += _structure_size(SITE_CONFIG)

        self.radname = self.dic_stcfg['site_code'].decode('latin-1')[0:5]

        # 读取任务配置
        dic_tcfg = _unpack_from_buf(buf, pos, TASK_CONFIG)
        pos += _structure_size(TASK_CONFIG)

        # 获取扫描信息
        self.cutinfo = []
        for im in np.arange(dic_tcfg['cut_number']):
            dic_cutcfg = _unpack_from_buf(buf, pos, SCAN_CONFIG)
            self.cutinfo.append(dic_cutcfg)
            pos = pos + _structure_size(SCAN_CONFIG)

        csstr = dic_tcfg['task_name'].decode('latin-1')
        task_name = csstr[0:csstr.find('\x00')]
        self.vcp_type_num = [int(s) for s in re.findall(r'-?\d+\.?\d*', task_name)][0]

        # read the records from the buffer
        self._records = []
        buf_length = len(buf)
        
        while pos < buf_length:
            pos, dic = self._get_record_from_buf(buf, pos)
            self._records.append(dic)


        elev_nums = np.array([m['msg_header']['elevation_number']
                              for m in self._records])
        self.scan_msgs = [np.where(elev_nums == i + 1)[0]
                          for i in range(elev_nums.max())]
        self.nscans = len(self.scan_msgs)

        self._msg_type = '31'

        outdic_vh = dict(zip([i[0] for i in VOLUME_HEADER],
                            [k for k in np.zeros(len(VOLUME_HEADER))]))
        outdic_vh['tape'] = b'AR2V0006.'
        outdic_vh['extension'] = b'001'

        # 改为北京时间 
        # if time_type == 'BJT':
        #     dic_tcfg['scan_stime'] = dic_tcfg['scan_stime'] + 8*3600

        
        outdic_vh['date'] = int(dic_tcfg['scan_stime'] / 86400)  # 一天有86400秒
        outdic_vh['time'] = 1000 * (dic_tcfg['scan_stime'] - outdic_vh['date'] * 86400)
        outdic_vh['date'] = outdic_vh['date'] + 1
        outdic_vh['icao'] = self.radname.encode()

        self.volume_header = outdic_vh.copy()

        # 构建VCP信息，也就是MSG5

        msg_header = MSG_HEADER
        dic_msg_header = dict(zip([i[0] for i in MSG_HEADER],
                          [k for k in np.zeros(len(MSG_HEADER))]))
        
        dic_msg5_header = dict(zip([i[0] for i in MSG_5],
                          [k for k in np.zeros(len(MSG_5))]))
        
        dic_msg5_elev = dict(zip([i[0] for i in MSG_5_ELEV],
                          [k for k in np.zeros(len(MSG_5_ELEV))]))

        msg_header_size = _structure_size(MSG_HEADER)
        msg5_header_size = _structure_size(MSG_5)
        msg5_elev_size = _structure_size(MSG_5_ELEV)

        dic_msg_header['size'] = (msg5_elev_size*dic_tcfg['cut_number'] + msg5_header_size + msg_header_size)//2
        dic_msg_header['channels'] = 8
        dic_msg_header['type'] = 5
        dic_msg_header['seq_id'] = 1

        date_t = int(dic_tcfg['scan_stime'] / 86400)  # 一天有86400秒
        time_t = 1000 * (dic_tcfg['scan_stime'] - date_t * 86400)
        date_t = date_t + 1

        dic_msg_header['date'] = date_t
        dic_msg_header['ms'] = time_t
        dic_msg_header['segments'] = 1
        dic_msg_header['seg_num'] = 1

        dic_msg5_header['msg_size'] = (msg5_elev_size*dic_tcfg['cut_number'] + msg5_header_size)//2
        dic_msg5_header['pattern_type'] = 2
        
        
        dic_msg5_header['pattern_number'] = self.vcp_type_num
        dic_msg5_header['num_cuts'] = dic_tcfg['cut_number']
        dic_msg5_header['clutter_map_group'] = 257
        dic_msg5_header['doppler_vel_res'] = 2
        dic_msg5_header['pulse_width'] = 2
        dic_msg5_header['spare'] = b'0000000000'
        
        # 添加 MSG_5_ELEV  仰角信息
        cut_param=[]
        for nn in range(dic_tcfg['cut_number']):
            
            dic_msg5_elev['elevation_angle'] = int(self.cutinfo[nn]['elevation'] * 65536/360)
            # print(cutinfo[nn]['elevation'])
            dic_msg5_elev['channel_config'] = 0
            if nn==0 or nn==2:
                dic_msg5_elev['waveform_type'] = 1
                dic_msg5_elev['super_resolution'] = 11
                dic_msg5_elev['prf_number'] = 1
                dic_msg5_elev['prf_pulse_count'] = 28
                dic_msg5_elev['azimuth_rate'] = int(self.cutinfo[nn]['scan_speed'])
                dic_msg5_elev['ref_thresh'] = 16
                dic_msg5_elev['vel_thresh'] = 16
                dic_msg5_elev['sw_thresh'] = 16
                dic_msg5_elev['zdr_thres'] = 16
                dic_msg5_elev['phi_thres'] = 16
                dic_msg5_elev['rho_thres'] = 16
                dic_msg5_elev['edge_angle_1'] = 0
                dic_msg5_elev['dop_prf_num_1'] = 0
                dic_msg5_elev['dop_prf_pulse_count_1'] = 0
                dic_msg5_elev['spare_1'] = b'00'
                dic_msg5_elev['edge_angle_2'] = 0
                dic_msg5_elev['dop_prf_num_2'] = 0
                dic_msg5_elev['dop_prf_pulse_count_2'] = 0
                dic_msg5_elev['spare_2'] = b'00'
                dic_msg5_elev['edge_angle_3'] = 0
                dic_msg5_elev['dop_prf_num_3'] = 0
                dic_msg5_elev['dop_prf_pulse_count_3'] = 0
                dic_msg5_elev['spare_3'] = b'00'
            elif nn==1 or nn==3:
                dic_msg5_elev['waveform_type'] = 2
                dic_msg5_elev['super_resolution'] = 7
                dic_msg5_elev['prf_number'] = 0
                dic_msg5_elev['prf_pulse_count'] = 0
                dic_msg5_elev['azimuth_rate'] = int(self.cutinfo[nn]['scan_speed'])
                dic_msg5_elev['ref_thresh'] = 16
                dic_msg5_elev['vel_thresh'] = 16
                dic_msg5_elev['sw_thresh'] = 16
                dic_msg5_elev['zdr_thres'] = 16
                dic_msg5_elev['phi_thres'] = 16
                dic_msg5_elev['rho_thres'] = 16
                dic_msg5_elev['edge_angle_1'] = 5464
                dic_msg5_elev['dop_prf_num_1'] = 4
                dic_msg5_elev['dop_prf_pulse_count_1'] = 75
                dic_msg5_elev['spare_1'] = b'00'
                dic_msg5_elev['edge_angle_2'] = 38232
                dic_msg5_elev['dop_prf_num_2'] = 4
                dic_msg5_elev['dop_prf_pulse_count_2'] = 75
                dic_msg5_elev['spare_2'] = b'00'
                dic_msg5_elev['edge_angle_3'] = 60984
                dic_msg5_elev['dop_prf_num_3'] = 4
                dic_msg5_elev['dop_prf_pulse_count_3'] = 75
                dic_msg5_elev['spare_3'] = b'00'
            else:
                dic_msg5_elev['waveform_type'] = 4
                dic_msg5_elev['super_resolution'] = 14
                dic_msg5_elev['prf_number'] = 2
                dic_msg5_elev['prf_pulse_count'] = 8
                dic_msg5_elev['azimuth_rate'] = int(self.cutinfo[nn]['scan_speed'])
                dic_msg5_elev['ref_thresh'] = 16
                dic_msg5_elev['vel_thresh'] = 16
                dic_msg5_elev['sw_thresh'] = 16
                dic_msg5_elev['zdr_thres'] = 16
                dic_msg5_elev['phi_thres'] = 16
                dic_msg5_elev['rho_thres'] = 16
                dic_msg5_elev['edge_angle_1'] = 5464
                dic_msg5_elev['dop_prf_num_1'] = 4
                dic_msg5_elev['dop_prf_pulse_count_1'] =59
                dic_msg5_elev['spare_1'] = b'00'
                dic_msg5_elev['edge_angle_2'] = 38232
                dic_msg5_elev['dop_prf_num_2'] = 4
                dic_msg5_elev['dop_prf_pulse_count_2'] = 59
                dic_msg5_elev['spare_2'] = b'00'
                dic_msg5_elev['edge_angle_3'] = 60984
                dic_msg5_elev['dop_prf_num_3'] = 4
                dic_msg5_elev['dop_prf_pulse_count_3'] = 59
                dic_msg5_elev['spare_3'] = b'00'
            cut_param.append(dic_msg5_elev.copy())

        msg_5={}
        msg_5['header'] = dic_msg_header.copy()
        msg_5['msg5_header'] = dic_msg5_header.copy()
        msg_5['cut_parameters'] = cut_param.copy()

        self.radial_records = self._records.copy()
        self._records.append(msg_5)

        self.vcp = msg_5

        # self.vcp = None
        return

    def close(self):
        """ Close the file. """
        self._fh.close()

    def location(self):
        """
        Find the location of the radar.

        Returns all zeros if location is not available.

        Returns
        -------
        latitude : float
            Latitude of the radar in degrees.
        longitude : float
            Longitude of the radar in degrees.
        height : int
            Height of radar and feedhorn in meters above mean sea level.

        """
        if self._msg_type == '31':
            dic = self.radial_records[0]['VOL']
            height = dic['height'] + dic['feedhorn_height']
            return dic['lat'], dic['lon'], height
        else:
            return 0.0, 0.0, 0.0

    def scan_info(self, scans=None):
        """
        Return a list of dictionaries with scan information.

        Parameters
        ----------
        scans : list ot None
            Scans (0 based) for which ray (radial) azimuth angles will be
            retrieved.  None (the default) will return the angles for all
            scans in the volume.

        Returns
        -------
        scan_info : list, optional
            A list of the scan performed with a dictionary with keys
            'moments', 'ngates', 'nrays', 'first_gate' and 'gate_spacing'
            for each scan.  The 'moments', 'ngates', 'first_gate', and
            'gate_spacing' keys are lists of the NEXRAD moments and gate
            information for that moment collected during the specific scan.
            The 'nrays' key provides the number of radials collected in the
            given scan.

        """
        info = []

        if scans is None:
            scans = range(self.nscans)
        for scan in scans:
            nrays = self.get_nrays(scan)
            if nrays < 2:
                self.nscans -= 1
                continue
            msg31_number = self.scan_msgs[scan][0]
            msg = self.radial_records[msg31_number]
            nexrad_moments = ['REF', 'VEL', 'SW', 'ZDR', 'PHI', 'RHO', 'CFP']
            moments = [f for f in nexrad_moments if f in msg]
            ngates = [msg[f]['ngates'] for f in moments]
            gate_spacing = [msg[f]['gate_spacing'] for f in moments]
            first_gate = [msg[f]['first_gate'] for f in moments]
            info.append({
                'nrays': nrays,
                'ngates': ngates,
                'gate_spacing': gate_spacing,
                'first_gate': first_gate,
                'moments': moments})
        return info

    def get_vcp_pattern(self):
        """
        Return the numerical volume coverage pattern (VCP) or None if unknown.
        """
        if self.vcp is None:
            return None
        else:
            return self.vcp['msg5_header']['pattern_number']

    def get_nrays(self, scan):
        """
        Return the number of rays in a given scan.

        Parameters
        ----------
        scan : int
            Scan of interest (0 based).

        Returns
        -------
        nrays : int
            Number of rays (radials) in the scan.

        """
        return len(self.scan_msgs[scan])

    def get_range(self, scan_num, moment):
        """
        Return an array of gate ranges for a given scan and moment.

        Parameters
        ----------
        scan_num : int
            Scan number (0 based).
        moment : 'REF', 'VEL', 'SW', 'ZDR', 'PHI', 'RHO', or 'CFP'
            Moment of interest.

        Returns
        -------
        range : ndarray
            Range in meters from the antenna to the center of gate (bin).

        """
        dic = self.radial_records[self.scan_msgs[scan_num][0]][moment]
        ngates = dic['ngates']
        first_gate = dic['first_gate']
        gate_spacing = dic['gate_spacing']
        return np.arange(ngates) * gate_spacing + first_gate

    # helper functions for looping over scans
    def _msg_nums(self, scans):
        """ Find the all message number for a list of scans. """
        return np.concatenate([self.scan_msgs[i] for i in scans])

    def _radial_array(self, scans, key):
        """
        Return an array of radial header elements for all rays in scans.
        """
        msg_nums = self._msg_nums(scans)
        temp = [self.radial_records[i]['msg_header'][key] for i in msg_nums]
        return np.array(temp)

    def _radial_sub_array(self, scans, key):
        """
        Return an array of RAD or msg_header elements for all rays in scans.
        """
        msg_nums = self._msg_nums(scans)
        if self._msg_type == '31':
            tmp = [self.radial_records[i]['RAD'][key] for i in msg_nums]
        else:
            tmp = [self.radial_records[i]['msg_header'][key] for i in msg_nums]
        return np.array(tmp)

    def get_times(self, scans=None):
        """
        Retrieve the times at which the rays were collected.

        Parameters
        ----------
        scans : list or None
            Scans (0-based) to retrieve ray (radial) collection times from.
            None (the default) will return the times for all scans in the
            volume.

        Returns
        -------
        time_start : Datetime
            Initial time.
        time : ndarray
            Offset in seconds from the initial time at which the rays
            in the requested scans were collected.

        """
        if scans is None:
            scans = range(self.nscans)
        days = self._radial_array(scans, 'collect_date')
        secs = self._radial_array(scans, 'collect_ms') / 1000.
        offset = timedelta(days=int(days[0]) - 1, seconds=int(secs[0]))
        time_start = datetime(1970, 1, 1) + offset
        time = secs - int(secs[0]) + (days - days[0]) * 86400
        return time_start, time

    def get_azimuth_angles(self, scans=None):
        """
        Retrieve the azimuth angles of all rays in the requested scans.

        Parameters
        ----------
        scans : list ot None
            Scans (0 based) for which ray (radial) azimuth angles will be
            retrieved. None (the default) will return the angles for all
            scans in the volume.

        Returns
        -------
        angles : ndarray
            Azimuth angles in degress for all rays in the requested scans.

        """
        if scans is None:
            scans = range(self.nscans)
        if self._msg_type == '1':
            scale = 180 / (4096 * 8.)
        else:
            scale = 1.
        return self._radial_array(scans, 'azimuth_angle') * scale

    def get_elevation_angles(self, scans=None):
        """
        Retrieve the elevation angles of all rays in the requested scans.

        Parameters
        ----------
        scans : list or None
            Scans (0 based) for which ray (radial) azimuth angles will be
            retrieved. None (the default) will return the angles for
            all scans in the volume.

        Returns
        -------
        angles : ndarray
            Elevation angles in degress for all rays in the requested scans.

        """
        if scans is None:
            scans = range(self.nscans)
        if self._msg_type == '1':
            scale = 180 / (4096 * 8.)
        else:
            scale = 1.
        return self._radial_array(scans, 'elevation_angle') * scale

    def get_target_angles(self, scans=None):
        """
        Retrieve the target elevation angle of the requested scans.

        Parameters
        ----------
        scans : list or None
            Scans (0 based) for which the target elevation angles will be
            retrieved. None (the default) will return the angles for all
            scans in the volume.

        Returns
        -------
        angles : ndarray
            Target elevation angles in degress for the requested scans.

        """
        if scans is None:
            scans = range(self.nscans)
        if self._msg_type == '31':
            if self.vcp is not None:
                cut_parameters = self.vcp['cut_parameters']
            else:
                cut_parameters = [{'elevation_angle': 0.0}] * self.nscans
            scale = 360. / 65536.
            return np.array([cut_parameters[i]['elevation_angle'] * scale
                             for i in scans], dtype='float32')
        else:
            scale = 180 / (4096 * 8.)
            msgs = [self.radial_records[self.scan_msgs[i][0]] for i in scans]
            return np.round(np.array(
                [m['msg_header']['elevation_angle'] * scale for m in msgs],
                dtype='float32'), 1)

    def get_nyquist_vel(self, scans=None):
        """
        Retrieve the Nyquist velocities of the requested scans.

        Parameters
        ----------
        scans : list or None
            Scans (0 based) for which the Nyquist velocities will be
            retrieved. None (the default) will return the velocities for all
            scans in the volume.

        Returns
        -------
        velocities : ndarray
            Nyquist velocities (in m/s) for the requested scans.

        """
        if scans is None:
            scans = range(self.nscans)
        return self._radial_sub_array(scans, 'nyquist_vel') * 0.01

    def get_unambigous_range(self, scans=None):
        """
        Retrieve the unambiguous range of the requested scans.

        Parameters
        ----------
        scans : list or None
            Scans (0 based) for which the unambiguous range will be retrieved.
            None (the default) will return the range for all scans in the
            volume.

        Returns
        -------
        unambiguous_range : ndarray
            Unambiguous range (in meters) for the requested scans.

        """
        if scans is None:
            scans = range(self.nscans)
        # unambiguous range is stored in tenths of km, x100 for meters
        return self._radial_sub_array(scans, 'unambig_range') * 100.

    def get_data(self, moment, max_ngates, scans=None, raw_data=False):
        """
        Retrieve moment data for a given set of scans.

        Masked points indicate that the data was not collected, below
        threshold or is range folded.

        Parameters
        ----------
        moment : 'REF', 'VEL', 'SW', 'ZDR', 'PHI', 'RHO', or 'CFP'
            Moment for which to to retrieve data.
        max_ngates : int
            Maximum number of gates (bins) in any ray.
            requested.
        raw_data : bool
            True to return the raw data, False to perform masking as well as
            applying the appropiate scale and offset to the data.  When
            raw_data is True values of 1 in the data likely indicate that
            the gate was not present in the sweep, in some cases in will
            indicate range folded data.
        scans : list or None.
            Scans to retrieve data from (0 based). None (the default) will
            get the data for all scans in the volume.

        Returns
        -------
        data : ndarray

        """
        if scans is None:
            scans = range(self.nscans)

        # determine the number of rays
        msg_nums = self._msg_nums(scans)
        nrays = len(msg_nums)
        # extract the data
        set_datatype = False
        data = np.ones((nrays, max_ngates), '>B')
        for i, msg_num in enumerate(msg_nums):
            msg = self.radial_records[msg_num]
            if moment not in msg.keys():
                continue
            if not set_datatype:
                data = data.astype('>'+self._bits_to_code(msg, moment))
                set_datatype = True

            ngates = min(msg[moment]['ngates'], max_ngates,
                         len(msg[moment]['data']))
            data[i, :ngates] = msg[moment]['data'][:ngates]
        # return raw data if requested
        if raw_data:
            return data

        # mask, scan and offset, assume that the offset and scale
        # are the same in all scans/gates
        for scan in scans:  # find a scan which contains the moment
            msg_num = self.scan_msgs[scan][0]
            msg = self.radial_records[msg_num]
            if moment in msg.keys():
                offset = np.float32(msg[moment]['offset'])
                scale = np.float32(msg[moment]['scale'])
                mask = data <= 1
                scaled_data = (data - offset) / scale
                return np.ma.array(scaled_data, mask=mask)

        # moment is not present in any scan, mask all values
        return np.ma.masked_less_equal(data, 1)


    def _bits_to_code(self,msg, moment):
        """
        Convert number of bits to the proper code for unpacking.
        Based on the code found in MetPy:
        https://github.com/Unidata/MetPy/blob/40d5c12ab341a449c9398508bd41
        d010165f9eeb/src/metpy/io/_tools.py#L313-L321
        """
        if msg['header']['type'] == 1:
            word_size = msg[moment]['data'].dtype
            if word_size == 'uint16':
                return 'H'
            elif word_size == 'uint8':
                return 'B'
            else:
                warnings.warn(
                    ('Unsupported bit size: %s. Returning "B"', word_size))
                return 'B'

        elif msg['header']['type'] == 31:
            word_size = msg[moment]['word_size']
            if word_size == 16:
                return 'H'
            elif word_size == 8:
                return 'B'
            else:
                warnings.warn(
                    ('Unsupported bit size: %s. Returning "B"', word_size))
                return 'B'
        else:
            raise TypeError("Unsupported msg type %s", msg['header']['type'])


    def _decompress_records(self,file_handler):
        """
        Decompressed the records from an BZ2 compressed Archive 2 file.
        """
        file_handler.seek(0)
        cbuf = file_handler.read()    # read all data from the file
        decompressor = bz2.BZ2Decompressor()
        skip = _structure_size(VOLUME_HEADER) + CONTROL_WORD_SIZE
        buf = decompressor.decompress(cbuf[skip:])
        while len(decompressor.unused_data):
            cbuf = decompressor.unused_data
            decompressor = bz2.BZ2Decompressor()
            buf += decompressor.decompress(cbuf[CONTROL_WORD_SIZE:])

        return buf[COMPRESSION_RECORD_SIZE:]


    def _get_record_from_buf(self,buf, pos):
        """ Retrieve and unpack a NEXRAD record from a buffer. """
        dic={}
        new_pos = self._get_msg31_from_buf(buf, pos, dic)

        return new_pos, dic


    def _get_msg31_from_buf(self,buf, pos, dic):
        """ Retrieve and unpack a MSG31 record from a buffer. """

        dic_rhb = _unpack_from_buf(buf, pos, RADIAL_HEAD_BLOCK)
        pos2 = pos + _structure_size(RADIAL_HEAD_BLOCK)

        varnum = dic_rhb['mom_num']
        
        # 构建msg header
        outdic_mh = dict(zip([i[0] for i in MSG_HEADER],
                            [k for k in np.zeros(len(MSG_HEADER))]))
        outdic_mh['size'] = 3440  # 初始值，后面会改
        outdic_mh['channels'] = 8
        outdic_mh['type'] = 31
        outdic_mh['seq_id'] = 1
        Juliandate = int(dic_rhb['seconds'] / 86400)  # 一天有86400秒
        Seconds = 1000 * (dic_rhb['seconds'] - Juliandate * 86400)
        Juliandate = Juliandate + 1
        outdic_mh['date'] = Juliandate
        outdic_mh['ms'] = Seconds
        outdic_mh['segments'] = 1
        outdic_mh['seg_num'] = 1

        # 构建msg31 header
        outdic31 = dict(zip([i[0] for i in MSG_31],
                                    [k for k in np.zeros(len(MSG_31))]))
        # ang_reso = cutinfo[dic_rhb['ele_num'] - 1]['ang_reso']
        outdic31['id'] = self.radname.encode()
        outdic31['collect_ms'] = Seconds
        outdic31['collect_date'] = Juliandate
        outdic31['azimuth_number'] = dic_rhb['radial_num']
        outdic31['azimuth_angle'] = dic_rhb['azimuth']
        outdic31['compress_flag'] = 0
        outdic31['spare_0'] = 0
        outdic31['radial_length'] = 0
        outdic31['azimuth_resolution'] = 2# 2表示1度，1表示0.5度int(2 * ang_reso)
        outdic31['radial_stats'] = dic_rhb['radial_stats']
        
            
        # print(dic_rhb['radial_stats'])
        outdic31['elevation_number'] = dic_rhb['ele_num']
        outdic31['cut_sector'] = 1
        outdic31['elevation_angle'] = dic_rhb['elevation']
        outdic31['radial_blanking'] = 0
        outdic31['azimuth_mode'] = 25
        outdic31['block_count'] = 9
        # pointer for VOLUME_DATA_BLOCK based on MSG31 (68)
        # Volume Data Constant XVII-E
        outdic31['block_pointer_1'] = _structure_size(MSG_31)
        # Elevation Data Constant XVII-F
        outdic31['block_pointer_2'] = outdic31['block_pointer_1'] + _structure_size(VOLUME_DATA_BLOCK)
        # Radial Data Constant XVII-H
        outdic31['block_pointer_3'] = outdic31['block_pointer_2'] + _structure_size(ELEVATION_DATA_BLOCK)
        # Moment "REF" XVII-{B/I} 152？
        outdic31['block_pointer_4'] = 0
        # Moment "VEL"
        outdic31['block_pointer_5'] = 0
        # Moment "SW"
        outdic31['block_pointer_6'] = 0
        # Moment "ZDR"
        outdic31['block_pointer_7'] = 0
        # Moment "PHI"
        outdic31['block_pointer_8'] = 0
        # Moment "RHO"
        outdic31['block_pointer_9'] = 0
        

        # Volume Data Constant Type
        outdic_vol = dict(zip([i[0] for i in VOLUME_DATA_BLOCK],
                            [k for k in np.zeros(len(VOLUME_DATA_BLOCK))]))
        outdic_vol['block_type'] = b'R'
        outdic_vol['data_name'] = b'VOL'
        outdic_vol['lrtup'] = _structure_size(VOLUME_DATA_BLOCK)
        outdic_vol['version_major'] = 2
        outdic_vol['version_minor'] = 0

        outdic_vol['lat'] = self.dic_stcfg['lat']
        outdic_vol['lon'] = self.dic_stcfg['lon']
      

        if self.dic_stcfg['grd_height'] > 8000:
            outdic_vol['height'] = int(self.dic_stcfg['grd_height']/1000)
        else:
            outdic_vol['height'] = int(self.dic_stcfg['grd_height'])
        if self.dic_stcfg['ana_height'] > 8000:
            # 这里馈源高度减去地基高度，才是天线相对地基的高度
            outdic_vol['feedhorn_height'] = int(self.dic_stcfg['ana_height']/1000) - outdic_vol['height']
        else:
            outdic_vol['feedhorn_height'] = int(self.dic_stcfg['ana_height']) - outdic_vol['height']

        outdic_vol['refl_calib'] = 1.0
        outdic_vol['power_h'] = 0.0
        outdic_vol['power_v'] = 0.0
        outdic_vol['diff_refl_calib'] = 0.0
        outdic_vol['init_phase'] = 0.0
        outdic_vol['vcp'] = self.vcp_type_num
        outdic_vol['spare'] = b'aa'

        # Elevation Data Constant Type
        
        outdic_elv = dict(zip([i[0] for i in ELEVATION_DATA_BLOCK],
                        [k for k in np.zeros(len(ELEVATION_DATA_BLOCK))]))

        outdic_elv['block_type'] = b'R'
        outdic_elv['data_name'] = b'ELV'
        outdic_elv['lrtup'] = _structure_size(ELEVATION_DATA_BLOCK)
        outdic_elv['atmos'] = -12
        outdic_elv['refl_calib'] = -44.625

        # Radial Data Constant Type
        outdic_rad = dict(zip([i[0] for i in RADIAL_DATA_BLOCK],
                        [k for k in np.zeros(len(RADIAL_DATA_BLOCK))]))

        outdic_rad['block_type'] = b'R'
        outdic_rad['data_name'] = b'RAD'
        outdic_rad['lrtup'] = _structure_size(RADIAL_DATA_BLOCK)
        outdic_rad['unambig_range'] = int(self.cutinfo[dic_rhb['ele_num'] - 1]['max_range1'] / 100)
        outdic_rad['noise_h'] = -32.0
        outdic_rad['noise_v'] = 0
        outdic_rad['nyquist_vel'] = 100 * int(self.cutinfo[dic_rhb['ele_num'] - 1]['nyquist'])
        outdic_rad['spare'] = b'aa'
        outdic_rad['calib_dbz0_h'] = -44.95
        outdic_rad['calib_dbz0_v'] = -44.73
    
        # dic = {}    
        dic['header'] = outdic_mh
        dic['msg_header'] = outdic31
        dic['VOL'] = outdic_vol
        dic['ELV'] = outdic_elv
        dic['RAD'] = outdic_rad

        # 构建radial data
        for nn in range(0, varnum):
            block_name, block_dic,ptr = self._get_msg31_data_block(buf,pos2,dic_rhb)
            pos2 = ptr

            if not block_name is None:
                dic[block_name] = block_dic

        # dic['header'] = msg_31_header
        new_pos = pos2
        return new_pos


    def _get_msg31_data_block(self,buf,ptr,dic_rhb ):
        """ Unpack a msg_31 data block into a dictionary. """
        dic_rh = _unpack_from_buf(buf, ptr, RADIAL_HEAD)
        ptr = ptr + _structure_size(RADIAL_HEAD)
        data_type = dic_rh['data_type']
        real_buflen = dic_rh['length']
        real_gates = int(dic_rh['length'] / dic_rh['bin_len'])
        ref_reso = int(self.cutinfo[dic_rhb['ele_num'] - 1]['ref_reso'])
        vel_reso = int(self.cutinfo[dic_rhb['ele_num'] - 1]['vel_reso'])
        
        if dic_rh['bin_len'] == 1:
            data = np.frombuffer(buf[ptr: ptr + real_buflen], '<u1')
            ptr = ptr + real_buflen
        elif dic_rh['bin_len'] == 2:
            # data = np.zeros(real_buflen, dtype='uint16')
            data = np.frombuffer(buf[ptr: ptr + real_buflen], '<u2')
            ptr = ptr + real_buflen
        else:
            return None, None,ptr
        # dic = {}
        # dic['data'] = data
        # dic['length'] = real_buflen
        reso = 0
        if data_type == 2:
            block_name = 'REF'
            reso = ref_reso
        elif data_type == 3:
            block_name = 'VEL'
            reso = vel_reso
        elif data_type == 4:
            block_name = 'SW'
            reso = vel_reso
        elif data_type == 7:
            block_name = 'ZDR'
            reso = ref_reso
        elif data_type == 9:
            block_name = 'RHO'
            reso = ref_reso
        elif data_type == 10:
            block_name = 'PHI'
            reso = ref_reso
        else:
            return None, None,ptr
        
        outdic = dict(zip([i[0] for i in GENERIC_DATA_BLOCK],
                            [k for k in np.zeros(len(GENERIC_DATA_BLOCK))]))
        outdic['block_type'] = b'D'
        outdic['data_name'] = block_name.encode()
        outdic['reserved'] = 0
        outdic['ngates'] = real_gates
        outdic['first_gate'] = reso # self.cutinfo[dic_rhb['ele_num'] - 1]['start_range']
        outdic['gate_spacing'] = reso
        outdic['thresh'] = 100
        outdic['snr_thres'] = 16
        outdic['flags'] = 0
        outdic['word_size'] = 8 * dic_rh['bin_len']
        outdic['scale'] = float(dic_rh['scale'])
        outdic['offset'] = float(dic_rh['offset'])

        outdic['data'] = data


        return block_name, outdic,ptr

    def get_beam_width_h(self):
        """
        Return the horizontal beam width in degrees.

        Returns
        -------
        beam_width_h : float
            Horizontal beam width in degrees.

        """
        return [self.dic_stcfg['beamwidth_h'],]

    def get_beam_width_v(self):
        """
        Return the vertical beam width in degrees.

        Returns
        -------
        beam_width_v : float
            Vertical beam width in degrees.

        """
        return [self.dic_stcfg['beamwidth_v'],]
    
    def get_antenna_gain(self):
        """
        Return the antenna gain in dBi.

        Returns
        -------
        antenna_gain : float
            Antenna gain in dBi.

        """
        return [45,] # db
    
    def get_receiver_bandwidth(self):
        
        return [1.5e6,]  # Hz
    
    def _get_msg1_from_buf(self,buf, pos, dic):
        """ Retrieve and unpack a MSG1 record from a buffer. """
        msg_header_size = _structure_size(MSG_HEADER)
        msg1_header = _unpack_from_buf(buf, pos + msg_header_size, MSG_1)
        dic['msg_header'] = msg1_header

        sur_nbins = int(msg1_header['sur_nbins'])
        doppler_nbins = int(msg1_header['doppler_nbins'])

        sur_step = int(msg1_header['sur_range_step'])
        doppler_step = int(msg1_header['doppler_range_step'])

        sur_first = int(msg1_header['sur_range_first'])
        doppler_first = int(msg1_header['doppler_range_first'])
        if doppler_first > 2**15:
            doppler_first = doppler_first - 2**16

        if msg1_header['sur_pointer']:
            offset = pos + msg_header_size + msg1_header['sur_pointer']
            data = np.frombuffer(buf[offset:offset+sur_nbins], '>u1')
            dic['REF'] = {
                'ngates': sur_nbins,
                'gate_spacing': sur_step,
                'first_gate': sur_first,
                'data': data,
                'scale': 2.,
                'offset': 66.,
            }
        if msg1_header['vel_pointer']:
            offset = pos + msg_header_size + msg1_header['vel_pointer']
            data = np.frombuffer(buf[offset:offset+doppler_nbins], '>u1')
            dic['VEL'] = {
                'ngates': doppler_nbins,
                'gate_spacing': doppler_step,
                'first_gate': doppler_first,
                'data': data,
                'scale': 2.,
                'offset': 129.0,
            }
            if msg1_header['doppler_resolution'] == 4:
                # 1 m/s resolution velocity, offset remains 129.
                dic['VEL']['scale'] = 1.
        if msg1_header['width_pointer']:
            offset = pos + msg_header_size + msg1_header['width_pointer']
            data = np.frombuffer(buf[offset:offset+doppler_nbins], '>u1')
            dic['SW'] = {
                'ngates': doppler_nbins,
                'gate_spacing': doppler_step,
                'first_gate': doppler_first,
                'data': data,
                'scale': 2.,
                'offset': 129.0,
            }
        return pos + RECORD_SIZE



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


# NEXRAD Level II file structures and sizes
# The deails on these structures are documented in:
# "Interface Control Document for the Achive II/User" RPG Build 12.0
# Document Number 2620010E
# and
# "Interface Control Document for the RDA/RPG" Open Build 13.0
# Document Number 2620002M
# Tables and page number refer to those in the second document unless
# otherwise noted.
RECORD_SIZE = 2432
COMPRESSION_RECORD_SIZE = 12
CONTROL_WORD_SIZE = 4

# format of structure elements
# section 3.2.1, page 3-2
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

# Figure 1 in Interface Control Document for the Archive II/User
# page 7-2
VOLUME_HEADER = (
    ('tape', '9s'),
    ('extension', '3s'),
    ('date', 'I'),
    ('time', 'I'),
    ('icao', '4s')
)

# Table II Message Header Data
# page 3-7
MSG_HEADER = (
    ('size', INT2),                 # size of data, no including header
    ('channels', INT1),
    ('type', INT1),
    ('seq_id', INT2),
    ('date', INT2),
    ('ms', INT4),
    ('segments', INT2),
    ('seg_num', INT2),
)

# Table XVII Digital Radar Generic Format Blocks (Message Type 31)
# pages 3-87 to 3-89
MSG_31 = (
    ('id', '4s'),                   # 0-3
    ('collect_ms', INT4),           # 4-7
    ('collect_date', INT2),         # 8-9
    ('azimuth_number', INT2),       # 10-11
    ('azimuth_angle', REAL4),       # 12-15
    ('compress_flag', CODE1),       # 16
    ('spare_0', INT1),              # 17
    ('radial_length', INT2),        # 18-19
    ('azimuth_resolution', CODE1),  # 20
    ('radial_spacing', CODE1),      # 21
    ('elevation_number', INT1),     # 22
    ('cut_sector', INT1),           # 23
    ('elevation_angle', REAL4),     # 24-27
    ('radial_blanking', CODE1),     # 28
    ('azimuth_mode', SINT1),        # 29
    ('block_count', INT2),          # 30-31
    ('block_pointer_1', INT4),      # 32-35  Volume Data Constant XVII-E
    ('block_pointer_2', INT4),      # 36-39  Elevation Data Constant XVII-F
    ('block_pointer_3', INT4),      # 40-43  Radial Data Constant XVII-H
    ('block_pointer_4', INT4),      # 44-47  Moment "REF" XVII-{B/I}
    ('block_pointer_5', INT4),      # 48-51  Moment "VEL"
    ('block_pointer_6', INT4),      # 52-55  Moment "SW"
    ('block_pointer_7', INT4),      # 56-59  Moment "ZDR"
    ('block_pointer_8', INT4),      # 60-63  Moment "PHI"
    ('block_pointer_9', INT4),      # 64-67  Moment "RHO"
    ('block_pointer_10', INT4),     # Moment "CFP"
)


# Table III Digital Radar Data (Message Type 1)
# pages 3-7 to
MSG_1 = (
    ('collect_ms', INT4),           # 0-3
    ('collect_date', INT2),         # 4-5
    ('unambig_range', SINT2),       # 6-7
    ('azimuth_angle', CODE2),       # 8-9
    ('azimuth_number', INT2),       # 10-11
    ('radial_status', CODE2),       # 12-13
    ('elevation_angle', INT2),      # 14-15
    ('elevation_number', INT2),     # 16-17
    ('sur_range_first', CODE2),     # 18-19
    ('doppler_range_first', CODE2),  # 20-21
    ('sur_range_step', CODE2),      # 22-23
    ('doppler_range_step', CODE2),  # 24-25
    ('sur_nbins', INT2),            # 26-27
    ('doppler_nbins', INT2),        # 28-29
    ('cut_sector_num', INT2),       # 30-31
    ('calib_const', REAL4),         # 32-35
    ('sur_pointer', INT2),          # 36-37
    ('vel_pointer', INT2),          # 38-39
    ('width_pointer', INT2),        # 40-41
    ('doppler_resolution', CODE2),  # 42-43
    ('vcp', INT2),                  # 44-45
    ('spare_1', '8s'),              # 46-53
    ('spare_2', '2s'),              # 54-55
    ('spare_3', '2s'),              # 56-57
    ('spare_4', '2s'),              # 58-59
    ('nyquist_vel', SINT2),         # 60-61
    ('atmos_attenuation', SINT2),   # 62-63
    ('threshold', SINT2),           # 64-65
    ('spot_blank_status', INT2),    # 66-67
    ('spare_5', '32s'),             # 68-99
    # 100+  reflectivity, velocity and/or spectral width data, CODE1
)

# Table XI Volume Coverage Pattern Data (Message Type 5 & 7)
# pages 3-51 to 3-54
MSG_5 = (
    ('msg_size', INT2),
    ('pattern_type', CODE2),
    ('pattern_number', INT2),
    ('num_cuts', INT2),
    ('clutter_map_group', INT2),
    ('doppler_vel_res', CODE1),     # 2: 0.5 degrees, 4: 1.0 degrees
    ('pulse_width', CODE1),         # 2: short, 4: long
    ('spare', '10s')                # halfwords 7-11 (10 bytes, 5 halfwords)
)

MSG_5_ELEV = (
    ('elevation_angle', CODE2),  # scaled by 360/65536 for value in degrees.
    ('channel_config', CODE1),
    ('waveform_type', CODE1),
    ('super_resolution', CODE1),
    ('prf_number', INT1),
    ('prf_pulse_count', INT2),
    ('azimuth_rate', CODE2),
    ('ref_thresh', SINT2),
    ('vel_thresh', SINT2),
    ('sw_thresh', SINT2),
    ('zdr_thres', SINT2),
    ('phi_thres', SINT2),
    ('rho_thres', SINT2),
    ('edge_angle_1', CODE2),
    ('dop_prf_num_1', INT2),
    ('dop_prf_pulse_count_1', INT2),
    ('spare_1', '2s'),
    ('edge_angle_2', CODE2),
    ('dop_prf_num_2', INT2),
    ('dop_prf_pulse_count_2', INT2),
    ('spare_2', '2s'),
    ('edge_angle_3', CODE2),
    ('dop_prf_num_3', INT2),
    ('dop_prf_pulse_count_3', INT2),
    ('spare_3', '2s'),
)

# Table XVII-B Data Block (Descriptor of Generic Data Moment Type)
# pages 3-90 and 3-91
GENERIC_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),        # VEL, REF, SW, RHO, PHI, ZDR
    ('reserved', INT4),
    ('ngates', INT2),
    ('first_gate', SINT2),
    ('gate_spacing', SINT2),
    ('thresh', SINT2),
    ('snr_thres', SINT2),
    ('flags', CODE1),
    ('word_size', INT1),
    ('scale', REAL4),
    ('offset', REAL4),
    # then data
)

# Table XVII-E Data Block (Volume Data Constant Type)
# page 3-92
VOLUME_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),
    ('lrtup', INT2),
    ('version_major', INT1),
    ('version_minor', INT1),
    ('lat', REAL4),
    ('lon', REAL4),
    ('height', SINT2),
    ('feedhorn_height', INT2),
    ('refl_calib', REAL4),
    ('power_h', REAL4),
    ('power_v', REAL4),
    ('diff_refl_calib', REAL4),
    ('init_phase', REAL4),
    ('vcp', INT2),
    ('spare', '2s'),
)

# Table XVII-F Data Block (Elevation Data Constant Type)
# page 3-93
ELEVATION_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),
    ('lrtup', INT2),
    ('atmos', SINT2),
    ('refl_calib', REAL4),
)

# Table XVII-H Data Block (Radial Data Constant Type)
# pages 3-93
RADIAL_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),
    ('lrtup', INT2),
    ('unambig_range', SINT2),
    ('noise_h', REAL4),
    ('noise_v', REAL4),
    ('nyquist_vel', SINT2),
    ('spare', '2s')
)

##====================================================================================================
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

# 下面的文件头信息来源于 北京敏视达公司的双偏振天气雷达基数据格式，2016

# 表2-2
GENERIC_HEADER = (
    ('magic_number', INT4),
    ('major_version', INT2),
    ('minor_version', INT2),
    ('generic_type', INT4),
    ('product_type', INT4),
    ('reserved', '16s')
)

# 表2-3
SITE_CONFIG = (
    ('site_code', '8s'),
    ('site_name', '32s'),
    ('lat', REAL4),
    ('lon', REAL4),
    ('ana_height', INT4),
    ('grd_height', INT4),
    ('freq', REAL4),
    ('beamwidth_h', REAL4),
    ('beamwidth_v', REAL4),
    ('rda_version', INT4),
    ('radar_type', INT2),
    ('reserved', '54s')
)

# 表2-4
TASK_CONFIG = (
    ('task_name', '32s'),
    ('task_disp', '128s'),
    ('pol_type', INT4),
    ('scan_type', INT4),
    ('pulse_wid', INT4),
    ('scan_stime', INT4),
    ('cut_number', INT4),
    ('hor_noise', REAL4),
    ('ver_noise', REAL4),
    ('hor_calib', REAL4),
    ('ver_calib', REAL4),
    ('hor_noise_t', REAL4),
    ('ver_noise_t', REAL4),
    ('zdr_calib', REAL4),
    ('phi_calib', REAL4),
    ('ldr_calib', REAL4),
    ('reserved', '40s')
)

# 表2-5
SCAN_CONFIG = (
    ('process_mod', INT4),
    ('wave_form', INT4),
    ('prf1', REAL4),
    ('prf2', REAL4),
    ('dias_mode', INT4),
    ('azimuth', REAL4),
    ('elevation', REAL4),
    ('start_angle', REAL4),
    ('end_angle', REAL4),
    ('ang_reso', REAL4),
    ('scan_speed', REAL4),
    ('ref_reso', INT4),
    ('vel_reso', INT4),
    ('max_range1', INT4),
    ('max_range2', INT4),
    ('start_range', INT4),
    ('sample_num1', INT4),
    ('sample_num2', INT4),
    ('phase_mode', INT4),
    ('atmo_loss', REAL4),
    ('nyquist', REAL4),
    ('mom_mask', REAL8),
    ('mom_size_mask', REAL8),
    ('mgk_fil_mask', INT4),
    ('sqi_thr', REAL4),
    ('sig_thr', REAL4),
    ('csr_thr', REAL4),
    ('log_thr', REAL4),
    ('cpa_thr', REAL4),
    ('pmi_thr', REAL4),
    ('dplog_thr', REAL4),
    ('thr_reserved', '4s'),
    ('dbt_mask', INT4),
    ('dbz_mask', INT4),
    ('vel_mask', INT4),
    ('sw_mask', INT4),
    ('dp_mask', INT4),
    ('mask_reserved', '12s'),
    ('scan_sync', INT4),
    ('direction', INT4),
    ('grd_clutter_clsify', INT2),
    ('grd_clutter_filt', INT2),
    ('grd_clutter_filt_ntwidth', INT2),
    ('grd_clutter_filt_window', INT2),
    ('reserved', '72s')
)

# 表3-1
RADIAL_HEAD_BLOCK = (
    ('radial_stats', INT4),
    ('spot_blank', INT4),
    ('seq_num', INT4),
    ('radial_num', INT4),
    ('ele_num', INT4),
    ('azimuth', REAL4),
    ('elevation', REAL4),
    ('seconds', INT4),
    ('micro_seconds', INT4),
    ('len_data', INT4),
    ('mom_num', INT4),
    ('reserved', '20s')
)

# 表3-2
RADIAL_HEAD = (
    ('data_type', INT4),
    ('scale', INT4),
    ('offset', INT4),
    ('bin_len', INT2),
    ('flags', INT2),
    ('length', INT4),
    ('reserved', '12s')
)