

import numpy as np
def calc_td(temperature,relative_humidity):
    # temperature C degree
    # relative_humidity 0~1
    
    sat_pressure_0c = 6.112 #'millibar'

    # math:: 6.112 e^\frac{17.67T}{T + 243.5}
    vapor_pressure = relative_humidity * sat_pressure_0c * \
                    np.exp(17.67 * temperature/ (temperature +243.5))

    val = np.log(vapor_pressure / sat_pressure_0c)
    return (243.5 * val / (17.67 - val))

def wind_speed(u, v):
    r"""Compute the wind speed from u and v-components.

    Parameters
    ----------
    u : `pint.Quantity`
        Wind component in the X (East-West) direction
    v : `pint.Quantity`
        Wind component in the Y (North-South) direction

    Returns
    -------
    wind speed: `pint.Quantity`
        Speed of the wind

    See Also
    --------
    wind_components

    """
    speed = np.sqrt(u * u + v * v)
    return speed

def wind_direction(u, v, convention='from'):
    

    wdir = 90 - np.rad2deg(np.arctan2(-v, -u))
    origshape = wdir.shape
    wdir = np.atleast_1d(wdir)

    # Handle oceanographic convection
    if convention == 'to':
        wdir -= 180
    elif convention not in ('to', 'from'):
        raise ValueError('Invalid kwarg for "convention". Valid options are "from" or "to".')

    mask = wdir <= 0
    if np.any(mask):
        wdir[mask] += 360
    # avoid unintended modification of `pint.Quantity` by direct use of magnitude
    calm_mask = (np.asarray(u) == 0.) & (np.asarray(v) == 0.)
    # np.any check required for legacy numpy which treats 0-d False boolean index as zero
    if np.any(calm_mask):
        wdir[calm_mask] = 0
    return wdir.reshape(origshape)

def geopotential_to_height(geopotential):
    r"""Compute height above sea level from a given geopotential.

    Calculates the height above mean sea level from geopotential using the following formula,
    which is derived from the definition of geopotential as given in [Hobbs2006]_ Pg. 69 Eq
    3.21, along with an approximation for variation of gravity with altitude:

    .. math:: z = \frac{\Phi R_e}{gR_e - \Phi}

    (where :math:`\Phi` is geopotential, :math:`z` is height, :math:`R_e` is average Earth
    radius, and :math:`g` is standard gravity).


    Parameters
    ----------
    geopotential : `pint.Quantity`
        Geopotential

    Returns
    -------
    `pint.Quantity`
        Corresponding value(s) of height above sea level

    """
    Re = 6371008.7714
    g = 9.80665
    return (geopotential * Re) / (g * Re - geopotential)

if __name__ == "__main__":
    pass

    g = 9.80665
    print(geopotential_to_height(5884*g))