# _*_ coding: utf-8 _*_

# Copyright (c) 2026 NMC Developers.
# Distributed under the terms of the GPL V3 License.

"""
Read configure file.
"""



import os
import configparser
from pathlib import Path


def _get_config_dir():
    """
    Get default configuration directory.
    """
    config_dir = Path.home() / ".metradar"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir

# Global Variables
CONFIG_DIR = _get_config_dir()


def _ConfigFetchError(BaseException):
    pass


def _get_config_from_rcfile(rc='config.ini'):
    """
    Get configure information from config_dk_met_io.ini file.
    """

    if not os.path.exists(rc):
        print(rc + ' not exists!')
        return None
  
    try:
        config = configparser.ConfigParser()
        config.read(rc,encoding='utf-8')
    except IOError as e:
        raise _ConfigFetchError(str(e))
    except BaseException as e:
        raise _ConfigFetchError(str(e))

    return config

# Global Variables
CONFIG = _get_config_from_rcfile(CONFIG_DIR / 'config.ini')
