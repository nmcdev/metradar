
# _*_ coding: utf-8 _*_

'''
记录rose产品的文件结构

'''
import struct


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
RADAR_TYPE = {
    1: 'SA',
    2: 'SB',
    3: 'SC',
    4: 'SAD',
    5: 'SBD',
    6: 'SCD',
    33: 'CA',
    34: 'CB',
    35: 'CC',
    36: 'CCJ',
    37: 'CD',
    38: 'CAD',
    39: 'CBD',
    40: 'CCD',
    41: 'CCJD',
    42: 'CDD',
    65: 'XA',
    66: 'XAD',
    68: 'XSD'
}

'''
radar_type
1–SA 2–SB
3–SC 4–SAD
5–SBD 6–SCD
33–CA 34–CB
35–CC 36–CCJ
37–CD 38–CAD
39–CBD 40–CCD
41–CCJD 42–CDD
65–XA 66–XAD
68-XSD
'''
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




# 表3-1 产品头块
PRODUCT_HEADER_BLOCK = (
    ('product_type', INT4),
    ('product_name', '32s'),
    ('product_time_ms', INT4),
    ('scan_start_ms', INT4),
    ('data_start_ms', INT4),
    ('data_end_ms', INT4),
    ('project_type', INT4),
    ('data_type1', INT4),
    ('data_type2', INT4),
    ('reserved', '64s')      
)

# 表 3-4 产品参数块
PRODUCT_DEPENDENT_PARAMETER={
    'PPI':(
        ('ele',REAL4), # 仰角，度
    ), 
    'CR':(
        ('top',INT4,), # 截断顶高，米
        ('bottom',INT4,),# 截断底高，米
    ), 
    'VIL':(
        ('top',INT4,), # 截断顶高，米
        ('bottom',INT4,),# 截断底高，米
    ), 
    'RHI':(
           ('azi',REAL4), # 方位角，度
           ('top',INT4,), #顶高，米
           ('bottom',INT4,), # 底高，米
    ),
    'CAPPI':(
           ('layers',INT4), # 层数
           ('top',INT4,),# 顶高，米
           ('bottom',INT4,),# 底高，米
           ('cappi_fill',INT4,), # 是否填充CAPPI，0-未填充，1-填充
    ),
    'MAX':(
           ('top',INT4,), # 截断顶高，米
           ('bottom',INT4,),# 截断底高，米
    ),
    'ET':(
           ('dbz_ct',REAL4,), # dBZ阈值
    ),
    'VCS':(
           ('start_azi',REAL4), # 起始方位角，度
           ('start_range',INT4,), # 起始距离，米
           ('end_azi',REAL4,), # 结束方位角，度
           ('end_range',INT4,), # 结束距离，米
           ('top',INT4,), # 顶高，米
           ('bottom',INT4,), # 底高，米
    ),
    'LRA':(
           ('top',INT4,),# 顶高，米
           ('bottom',INT4,),# 底高，米
    ),
    'LRM':(
           ('top',INT4,),# 顶高，米
           ('bottom',INT4,),# 底高，米
    ),
    'SRR':(
           ('ele',REAL4), # 仰角，度
           ('range_center',INT4,), # 中心距离，米
           ('azi_center',REAL4,), # 中心方位角，度
           ('side_len',INT4,), # 边长，米
           ('wspd',REAL4,), # 风速，米/秒
           ('wdir',REAL4,), # 风向，度
    ),
    'SRM':( 
           ('ele',REAL4), # 仰角，度
           ('wspd',REAL4,), # 风速，米/秒
           ('wdir',REAL4,), # 风向，度
    ),
    'SWA':(
           ('ele',REAL4),# 仰角，度
           ('range_center',INT4,), # 中心范围，米
           ('azi_center',REAL4,), # 中心方位，度
           ('side_len',INT4,), # 边长，米
    ),
    'WEA':(
           ('range',INT4), # 中心距离，米
           ('azi',REAL4,), # 中心方位角，度
           ('side_len',INT4,), # 边长，米
           ('levels',INT4,), # 层数
    ),
    'OHP':(
           ('base_prod',INT4), # 输入产品类型 产品类型为HSR,CAPPI或QPE，见表3-2
           ('cappi_height',INT4,), # CAPPI高度，米
           ('cappi_fill',INT4,), #是否CAPPI填充
           ('rain_gage_adjust',INT4,),# 是否雨量计修正
    ),
    'THP':(
           ('base_prod',INT4),# 输入产品类型
           ('cappi_height',INT4,),# CAPPI高度，米
           ('cappi_fill',INT4,),#是否CAPPI填充
           ('rain_gage_adjust',INT4,),# 是否雨量计修正
           ('hours',INT4,), # 用户请求的雨量积累的小时数
    ),
    'STP':(
           ('base_prod',INT4),# 输入产品类型
           ('cappi_height',INT4,),# CAPPI高度，米
           ('cappi_fill',INT4,),#是否CAPPI填充
           ('rain_gage_adjust',INT4,),# 是否雨量计修正
    ),
    'USP':(
           ('base_prod',INT4),# 输入产品类型
           ('cappi_height',INT4,),# CAPPI高度，米
           ('cappi_fill',INT4,),#是否CAPPI填充
           ('rain_gage_adjust',INT4,),# 是否雨量计修正
           ('hours',INT4,),# 用户请求的雨量积累的小时数
    ),
    'VAD':(
           ('layers',INT4),# VAD高度的个数
           # 接下来是第1~N层的高度，INT2类型
    ),
    'VWP':(
           ('layers',INT4), # VAD高度的个数
           # 接下来是第1~N层的高度，INT2类型
    ),
    'SHEAR':(
           ('ele',REAL4), # 仰角，度，基于切变数据的仰角
           ('radial_shear',INT4,),# 是否包含径向切变
           ('azi_shear',INT4,), # 是否包含方位切变
           ('ele_shear',INT4,),# 是否包含仰角切变
    ),
    'SWP':(
           ('max_range',INT4), # 最大范围
    ),
    'STI':(
           ('max_range',INT4),# 最大范围
    ),
    'HI':(
           ('max_range',INT4),# 最大范围
    ),
    'M':(
           ('max_range',INT4),# 最大范围
    ),
    'TVS':(
           ('max_range',INT4),# 最大范围
    ),
    'SS':(
           ('max_range',INT4),# 最大范围
    ),
    'GAGE':(
           ('max_range',INT4),# 最大范围
    ),
    'HCL':(
           ('ele',REAL4), # 仰角
    ),
}

# 产品类型列表3-2
PRODUCT_ID_NAME_MAP = {
    1: 'PPI',
    2: 'RHI',
    3: 'CAPPI',
    4: 'MAX',
    6: 'ET',
    8: 'VCS',
    9: 'LRA',
    10: 'LRM',
    13: 'SRR',
    14: 'SRM',
    18: 'CR',
    20: 'WEA',
    23: 'VIL',
    24: 'HSR',
    25: 'OHP',
    26: 'THP',
    27: 'STP',
    28: 'USP',
    31: 'VAD',
    32: 'VWP',
    34: 'SHEAR',
    36: 'SWP',
    37: 'STI',
    38: 'HI',
    39: 'M',
    40: 'TVS',
    41: 'SS',
    48: 'GAGE',
    51: 'HCL',
    52: 'QPE',

}

# 表6-21 ss产品头
SS_HEAD_BLOCK = (
     ('storm_number', INT4), # 风暴个数
)

# 表6-22 ss表结构
SS_TAB = (
    ('storm_id', INT4), # 风暴个数
    ('azi', REAL4), # 方位 度
    ('range', INT4), # 距离 米
    ('base', INT4), # 回波底 米
    ('top', INT4), # 回波顶 米
    ('vil', REAL4), # 垂直积分液态水含量 kg/m**2
    ('mref', REAL4), # 最大反射率 dBZ
    ('hmref', INT4), # 最大反射率高度 米
    
)

# 表6-23 风暴趋势数据
CELL_TREND = (
    ('storm_id', INT4), # 风暴ID
    ('his_vol_num', INT4), # 历史体扫个数,本表03～11项可能会重复
    # CELL_TREND_BASE 3~11项
)
HIS_VOL = (
    ('vol_time', INT4), # 体扫时间，秒
    ('height', INT4), # 高度 米
    ('base_h', INT4), # 回波底 米
    ('top_h', INT4), # 回波顶 米
    ('vil', INT4), # 垂直积分液态水含量 kg/m**2
    ('mref', INT4), # 最大反射率 dBZ
    ('hmref', INT4), # 最大反射率高度 米
    ('ph', INT4), # 冰雹概率 %
    ('psh', INT4), # 强冰雹概率 %
)

# 表6-24 SEGMENT ADAPTATION DATA
SEG_ADAPT = (
    ('REFLECTH1', INT4), # 反射率因子门限
    ('REFLECTH2', INT4), # 反射率因子门限
    ('REFLECTH3', INT4), # 反射率因子门限
    ('REFLECTH4', INT4), # 反射率因子门限
    ('REFLECTH5', INT4), # 反射率因子门限
    ('REFLECTH6', INT4), # 反射率因子门限
    ('REFLECTH7', INT4), # 反射率因子门限
    ('NREFLEVL', INT4), # 反射率因子等级数
    ('NUMAVGBN', INT4),  # 平均库数
    ('SEGRNGMX', INT4), # 段搜索距离
    ('MCOEFCTR', REAL4), # 系数因子
    ('MULTFCTR', REAL4), # 倍数因子
    ('MWGTFCTR', REAL4), # 权重因子

    ('SEGLENTH1', REAL4), # 段长度
    ('SEGLENTH2', REAL4), # 段长度
    ('SEGLENTH3', REAL4), # 段长度
    ('SEGLENTH4', REAL4), # 段长度
    ('SEGLENTH5', REAL4), # 段长度
    ('SEGLENTH6', REAL4), # 段长度
    ('SEGLENTH7', REAL4), # 段长度
    ('DRREFDFF', INT4), # 丢弃反射率因子差
    ('NDROPBIN', INT4),  # 丢弃库数
    ('NUMSEGMX', INT4), # 仰角段数
    ('RADSEGMX', INT4), # 径向段数
    
)

# 表6-25 CENTROIDS  ADAPTATION DATA
CENTROIDS_ADAPT = (
    ('CMPARETH1', INT4), # 风暴组1面积 平方千米
    ('CMPARETH2', INT4), # 风暴组2面积
    ('CMPARETH3', INT4), # 风暴组3面积
    ('CMPARETH4', INT4), # 风暴组4面积
    ('CMPARETH5', INT4), # 风暴组5面积
    ('CMPARETH6', INT4), # 风暴组6面积
    ('CMPARETH7', INT4), # 风暴组7面积
    ('RADIUSTH1', INT4), # 搜索半径1 千米
    ('RADIUSTH2', INT4),  # 搜索半径2
    ('RADIUSTH3', INT4), # 搜索半径3
    ('STMVILMX', REAL4), # 风暴单体VIL最大值 千克/平方米
    ('MXDETSTM', REAL4), # 最大单体个数
    ('OVLAPADJ', REAL4), # 邻近重叠距离 库数

    ('AZMDLTHR', REAL4), # 方位角差门限 度
    ('DEPTHDEL', REAL4), # 删除深度 千米
    ('HORIZDEL', REAL4), # 删除距离 千米
    ('ELVMERGE', REAL4), # 合并仰角 度
    ('HGTMERGE', REAL4), # 合并高度 千米
    ('HRZMERGE', REAL4), # 合并距离 千米
    ('NBRSEGMN', REAL4), # 最少段数
    ('NUMCMPMX', INT4), # 最多组数
    ('MXPOTCMP', INT4),  # 最多可能组数
    ('NUMSTMMX', INT4), # 最多单体数
    
)

# 表6-2
SIT_HEADER_BLOCK = (
    ('storm_number', INT4), # 风暴个数
    ('con_storm_number', INT4), # 连续风暴个数
    ('component_number', INT4), # 构成个数
    ('storm_speed_ave', REAL4), # 平均风暴移动速度 米/秒
    ('storm_dir_ave', REAL4), # 平均风暴移动方向 度
)

# 表6-3
STORM_MOTION_BLOCK = (
    ('azi', REAL4), # 风暴单体到雷达的方位：度
    ('range', INT4), # 风暴到雷达的距离：米
    ('mv_spd', REAL4), # 风暴的移动速度 米/秒
    ('mv_dir', REAL4), # 风暴单体的移动方向 度
    ('forecast_error', INT4),  # 预报错误，米
    ('mean_forecast_error', INT4), # 平均预报错误，米
    
)

# 表6-4 风暴预报或历史信息
STORM_FST_HIS_NUM = ('position_number', INT4), # 随后的位置个数N

STORM_FST_HIS_BLOCK = (
    ('azi', REAL4), # 第1个风暴的方位角，度
    ('range', INT4), # 第1个风暴的距离，米
    ('vol_time_position', INT4),# 第1个风暴的体扫时间，秒
    # 后面是N-1个风暴信息
)

# 表6-5 风暴属性
STORM_PROPERTY = (
    ('id', INT4), # 风暴ID
    ('type', INT4), # 风暴类型 0-连续风暴, 1–新生风暴
    ('vol_num', INT4), # 体扫个数
    ('azi', REAL4), # 方位，度
    ('range', INT4),  # 距离，米
    ('height', INT4), # 高度，米
    ('max_ref', REAL4), # 最大反射率，dBZ
    ('max_ref_height', INT4), # 最大反射率高度，米
    ('vil', REAL4), # VIL值，kg/m**2
    ('num_comp', INT4), # 构成的个数
    ('index_first_comp', INT4), # 第一个构成编号
    ('top_height', INT4), # 风暴顶高，米
    ('index_top', INT4), # 风暴顶的风暴编号
    ('base_height', INT4), # 风暴底高，米
    ('index_base', INT4), # 风暴底风暴编号
)

# 表6-6 风暴构成表
STORM_COMPONENT = (
    ('height', INT4), # 高度，米
    ('max_ref', REAL4), # 最大反射率，dBZ
    ('index_next', INT4), # 下一风暴构成编号
    
)

# 表6-7 风暴追踪适配数据
STORM_TRACK_PARAM = (
    ('def_direc', INT4), # 默认风向，度
    ('def_speed', REAL4),  # 默认风速，米/秒
    ('max_vtime', INT4), # 最大体扫时间间隔，分钟
    ('num_past_vol', INT4), # 历史体扫数
    ('cor_speed', REAL4), # 相关速度，米/秒
    ('min_speed', REAL4), # 最小速度，米/秒
    ('error_allow', INT4), # 允许误差 KM
    ('fst_step', INT4), # 预报间隔，分钟
    ('fst_num', INT4), # 预报个数
    ('error_step', INT4), # 误差间隔，分钟
   
)

# 表6-12 中尺度气旋头信息块
MESO_HEADER_BLOCK = (
    ('storm_number', INT4), # 风暴个数
    ('meso_number', INT4),  # 中尺度气旋个数
    ('feature_number', INT4), # 特征个数
)

# 表6-13 中尺度气旋表块
MESO_TABLE = (
    ('feature_id', INT4), # 中气旋特征ID
    ('storm_id', INT4), # 风暴ID
    ('azi', REAL4), # 方位角 度
    ('range', INT4), # 距离 米
    ('ele', REAL4), # 仰角 度
    ('ave_shear', REAL4), # 平均切变 E-3/S
    ('height', INT4), # 高度 米

    ('azi_diam', INT4), # 方位直径 米
    ('radial_diam', INT4),  # 径向直径 米 
    ('ave_rot_spd', REAL4), # 平均旋转速度 米/秒
    ('max_rot_spd', REAL4), # 最大旋转速度 米/秒
    ('top', INT4), # 顶高 米 
    ('base', INT4), # 底高 米
    ('base_azi', REAL4), # 回波底方位 度
    ('base_range', INT4), # 回波底距离 米
    ('base_ele', REAL4), # 回波底仰角 度
    ('max_t_shear', REAL4), # 最大切向切变 E-3/S
)

# 表6-14 中尺度气旋特征表
MESO_FEATURE_TAB = (
    ('feature_id', INT4), # 中气旋特征ID
    ('storm_id', INT4), # 风暴ID
    ('feature_type', INT4), # 特征类型 1–气旋 2–3D相关切变 3–非相关切变

    ('azi', REAL4), # 方位角 度
    ('range', INT4), # 距离 米
    ('ele', REAL4), # 仰角 度
    ('height', INT4), # 高度 米

    ('azi_diam', INT4), # 方位直径 米
    ('radial_diam', INT4),  # 径向直径 米 
    ('ave_shear', REAL4), # 平均切变 E-3/S
    ('max_shear', REAL4), # 最大切变 E-3/S
    ('ave_rot_spd', REAL4), # 平均旋转速度 米/秒
    ('max_rot_spd', REAL4), # 最大旋转速度 米/秒
    ('top', INT4), # 顶高 米 
    ('base', INT4), # 底高 米
    ('base_azi', REAL4), # 回波底方位 度
    ('base_range', INT4), # 回波底距离 米
    ('base_ele', REAL4), # 回波底仰角 度
   
)

# 表6-15 中尺度适配数据
MESO_ADAPTATION_DATA = (
    ('NPVTHR', INT4), # 特征向量个数门限
    ('FHTHR', REAL4), # 特征高度 千米
    ('HMTHR', REAL4), # 高角动量门限 平方千米/小时

    ('LMTHR', REAL4), # 低角动量门限 平方千米/小时
    ('HSTHR', REAL4), # 高切变门限 1/小时
    ('LSTHR', REAL4), # 低切变门限 1/小时
    ('MRTHR', REAL4), # 直径比率上限

    ('FMRTHR', REAL4), # 远比率上限
    ('NRTHR', REAL4),  # 近比率下限
    ('FNRTHR', REAL4), # 远比率下限
    ('RNGTHR', REAL4), # 距离门限 千米
    ('DISTHR', REAL4), # 最大径向差 千米
    ('AZTHR', REAL4), # 最大方位差 度
   
)


# 表6-17 TVS头信息
TVS_HEADER_BLOCK = (
    ('tvs_number', INT4), # 风暴个数
    ('etvs_numer', INT4),  # 中尺度气旋个数
)

# 表6-18 TVS表信息
TVS_TAB = (
    ('storm_id', INT4), # 风暴ID
    ('type', INT4), # TVS类型 1-TVS  2-ETVS
    ('azi', REAL4), # 方位角 度
    ('range', INT4), # 距离 米
    ('ele', REAL4), # 仰角 度
    ('lldv', REAL4), # 低层速度差值 米/秒
    ('adv', REAL4), # 平均速度差值 米/秒
    ('mxdv', REAL4), # 最大速度差值 米/秒
    ('hmdv', INT4),  # 最大速度差值高度 米
    ('depth', INT4), # 深度 米
    ('base', INT4), # 回波底高 米
    ('top', INT4), # 回波顶高 米 
    ('max_shear', REAL4), # 最大切变 10E-3/S
    ('h_max_shear', INT4), # 最大切变高度 米
   
)

# 表6-19 TVS适配数据
TVS_ADAPTATION_DATA = (
    ('MINREFL', INT4), # 最小反射率
    ('MINPVDV', REAL4), # 最小速度差
    ('MAXPVRNG', REAL4), # 最大模式向量距离

    ('MAXPVHT', REAL4), # 最大模式向量高度
    ('MAXNUMPV', REAL4), # 最大模式向量个数
    ('TH2DDV1', REAL4), # 差分速度门限1
    ('TH2DDV2', REAL4), # 差分速度门限2

    ('TH2DDV3', REAL4), # 差分速度门限3
    ('TH2DDV4', REAL4),  # 差分速度门限4
    ('TH2DDV5', REAL4), # 差分速度门限5
    ('TH2DDV6', REAL4), # 差分速度门限6
    ('MIN1DP2D', REAL4), # 最小模式向量个数
    ('MAXPVRD', REAL4), # 最大模式向量距离

    ('MAXPVAD', REAL4), # 最大模式方位距离
    ('MAX2DAR', REAL4),  # 二维特征最大比率
    ('THCR1', REAL4), # 搜索径向距离1
    ('THCR2', REAL4), # 搜索径向距离2
    ('THCRR', REAL4), # 径向距离门限
    ('MAXNUM2D', REAL4), # 最大二维特征数

    ('MIN2DP3D', REAL4), # 最小二维特征数
    ('MINTVSD', REAL4),  # 最小深度
    ('MINLLDV', REAL4), # 最小低层速度差
    ('MINMTDV', REAL4), # 最小速度差
    ('MAXNUM3D', REAL4), # 最大三维特征个数
    ('MAXNUMTV', REAL4), # 最大TVS个数

    ('MAXNUMET', REAL4), # 最大ETVS个数
    ('MINTVSBH', REAL4),  # TVS最小底高
    ('MINTVSBE', REAL4), # TVS最低仰角
    ('MINADVHT', REAL4), # 最小速度差高度
    ('MAXTSTMD', REAL4), # 最大风暴关联距离

)      

# 冰雹个数
HAIL_NUMBER = (
       ('hail_number', INT4), # 冰雹个数
)

# 表6-9 冰雹表信息
HAIL_TAB = (
    ('hail_id', INT4), # 冰雹ID
    ('azi', REAL4), # 方位角 度
    ('range', INT4), # 距离 米
    ('ph', INT4), # 冰雹概率
    ('psh', INT4), # 极端冰雹概率
    ('hsize', REAL4), # 冰雹大小 ，厘米
    ('rcm_code', INT4), # RCM编码
    
)

# 表6-10 HAIL适配数据
HAIL_ADAPTATION_DATA = (
    ('HT0MSL', INT4), # 0度层高度
    ('HT20MSL', REAL4), # -20度层高度
    ('HKECOF1', REAL4), # 动能系数1

    ('HKECOF2', REAL4), # 动能系数2
    ('HKECOF3', REAL4), # 动能系数3
    ('POSHCOF', REAL4), # 强冰雹概率系数
    ('POSHOFS', REAL4), # 强冰雹概率偏置量

    ('HSCOF', REAL4), # 冰雹尺寸系数
    ('HSEXP', REAL4),  # 冰雹尺寸指数
    ('LLHKEREF', REAL4), # 反射率下限
    ('ULHKEREF', REAL4), # 反射率上限
    ('RCMPRBL', REAL4), # RCM冰雹概率门限
    ('WTCOF', REAL4), # 报警门限系数

    ('MXHLRNG', REAL4), # 冰雹计算最大范围
    ('POHHDTH1', REAL4),  # 冰雹概率1高度差
    ('POHHDTH2', REAL4), # 冰雹概率2高度差
    ('POHHDTH3', REAL4), # 冰雹概率3高度差
    ('POHHDTH4', REAL4), # 冰雹概率4高度差
    ('POHHDTH5', REAL4), # 冰雹概率5高度差

    ('POHHDTH6', REAL4), # 冰雹概率6高度差
    ('POHHDTH7', REAL4),  # 冰雹概率7高度差
    ('POHHDTH8', REAL4), # 冰雹概率8高度差差
    ('POHHDTH9', REAL4), # 冰雹概率9高度差
    ('POHHDTH10', REAL4), # 冰雹概率10高度差
    ('MRPOHTH', REAL4), # 最小反射率门限
    ('RCMPSTV', REAL4), # RCM强冰雹概率
   

)  

# 表2-6 数据类型掩码定义
PRODUCT_DATA_TYPE = {
    1: '滤波前反射率（Total Reflectivity）',
    2: '滤波后反射率(Reflectivity)',
    3: '径向速度(Doppler Velocity)',
    4: '谱宽（Spectrum Width）',
    5: '信号质量指数（Signal Quality Index）',
    6: '杂波相位一致性（Clutter Phase Alignment）',
    7: '差分反射率（Differential Reflectivity）',
    8: '退偏振比（Liner Differential Ratio）',
    9: '协相关系数（Cross Correlation Coefficient）',
    10:'差分相移（Differential Phase）',
    11:'差分相移率（Specific Differential Phase）',
    12:'杂波可能性（Clutter Probability）',
    13:'数据标志，保留',
    14:'双偏振相态分类（Hydro Classification）',
    15:'杂波标志（Clutter Flag）',
    16:'信噪比（Signal Noise Ratio）',
    17:'数据标志，保留',
    18:'数据标志，保留',
    19:'数据标志，保留',
    20:'数据标志，保留',
    21:'数据标志，保留',
    22:'数据标志，保留',
    23:'数据标志，保留',
    24:'数据标志，保留',
    25:'数据标志，保留',
    26:'数据标志，保留',
    27:'数据标志，保留',
    28:'数据标志，保留',
    29:'数据标志，保留',
    30:'数据标志，保留',
    31:'数据标志，保留',
    32:'订正后反射率（Corrected Reflectivity）',
    33:'订正后径向速度(Corrected Doppler Velocity)',
    34:'订正后谱宽（Corrected Spectrum Width）',
    35:'订正后差分反射率(Corrected Differential Reflectivity)',
    72:'回波顶高（Echo Top）',
    73:'垂直积分液态水含量（Vertical Integrated Liquid）',
    75:'累计降水量（Accumulated Precipitation）',
}
# 表4-2 径向头信息块
RADIAL_HEADER=(
    ('data_type', INT4), # 数据类型
    ('scale', INT4), # 数据压缩比
    ('offset', INT4), # 数据偏移
    ('bin_length', INT2), # 数据长度
    ('flags', INT2), # 数据标志
    ('resolution', INT4), # 数据分辨率
    ('start_angle', INT4), # 起始角度
    ('max_range', INT4), # 最大距离
    ('radial_number', INT4), # 径向数
    ('max_value', INT4), # 最大值
    ('range_of_max_value', INT4), # 最大值距禈
    ('azi_of_max_value', REAL4), # 最大值方位
    ('min_value', INT4), # 最小值
    ('range_of_min_value', INT4), # 最小值距离
    ('azi_of_min_value', REAL4), # 最小值方位
    ('reserved', '8s') # 保留

)

# 表4-3 径向数据块信息
RADIAL_DATA=(
    ('start_azi', REAL4), # 起始方位角
    ('anglular_width', REAL4), # 角宽度
    ('num_bins', INT4), # 距离库个数
    ('reserved', '20s'), # 保留数据
    # 后续是第1~N个方位角的数据
)

# 表 4-4 柵格头信息块
GRID_HEADER=(
    ('data_type', INT4), # 数据类型
    ('scale', INT4), # 数据压缩比
    ('offset', INT4), # 数据偏移
    ('bin_length', INT2), # 数据长度
    ('flags', INT2), # 数据标志
    ('row_resolution', INT4), # 横轴分辨率
    ('col_resolution', INT4), # 纵轴分辨率
    ('row_side_len', INT4), # 横轴边长
    ('col_side_len', INT4), # 纵轴边长
    ('max_value', INT4), # 最大值
    ('range_of_max_value', INT4), # 最大值距禿
    ('azi_of_max_value', REAL4), # 最大值方位
    ('min_value', INT4), # 最小值
    ('range_of_min_value', INT4), # 最小值距禿
    ('azi_of_min_value', REAL4), # 最小值方位
    ('reserved', '8s'), # 保留
)

def _structure_size(structure):
    """ Find the size of a structure in bytes. """
    return struct.calcsize('<' + ''.join([i[1] for i in structure]))


def _unpack_structure(string, structure):
    """ Unpack a structure from a string """
    fmt = '<' + ''.join([i[1] for i in structure])  # little-endian
    lst = struct.unpack(fmt, string)
    return dict(zip([i[0] for i in structure], lst))

def _unpack_from_buf( buf, pos, structure):
    """ Unpack a structure from a buffer. """
    size = _structure_size(structure)
    return _unpack_structure(buf[pos:pos + size], structure)