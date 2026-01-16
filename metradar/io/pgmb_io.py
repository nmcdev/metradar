#! /usr/bin/python3
#
def pgmb_write ( file_name,params, width, height, maxval, gray ):

#*****************************************************************************80
#
## PGMB_WRITE writes a binary PGM graphics file.
#
#  Licensing:
#
#    This code is distributed under the GNU LGPL license. 
#
#  Modified:
#
#    14 September 2018
#
#  Author:
#
#    John Burkardt
#
#  Parameters:
#
#    Input, string FILE_NAME, the name of the file.
#
#    Input, integer WIDTH, HEIGHT, the width and height of the graphics image.
#
#    Input, integer MAXVAL, the maximum allowed gray value.
#
#    Input, integer GRAY[WIDTH*HEIGHT], values between 0 and MAXVAL.
#
  import numpy as np
  import struct

  file_handle = open ( file_name, 'wb' )
#
#  Set up the header.
# 
  # params['obstimestr'] = 201609281600
  # params['left_lon'] = 109
  # params['right_lon'] = 117
  # params['bottom_lat'] = 33
  # params['upper_lat'] = 37

  pgm_header = 'P5\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 

  pgm_header = '# composite_area FIN\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# obstime %s\n'%params['obstimestr']
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# producttype CAPPI\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# productname LOWEST\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# param CorrectedReflectivity\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# metersperpixel_x 999.674053\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# metersperpixel_y 999.62859\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# projection radar {\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# type stereographic\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# centrallongitude 110\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# centrallatitude 90\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# truelatitude 60\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# bottomleft %.3f %.3f\n'%(params['left_lon'],params['bottom_lat'])
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# topright %.3f %.3f\n'%(params['right_lon'],params['upper_lat'])
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = '# }\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  # b'# composite_area FIN\n'
  # b'# projection_name SUOMI1\n'

  # b'# obstime 201609281600\n'
  # b'# producttype CAPPI\n'
  # b'# productname LOWEST\n'
  # b'# param CorrectedReflectivity\n'
  # b'# metersperpixel_x 999.674053\n'
  # b'# metersperpixel_y 999.62859\n'
  # b'# projection radar {\n'
  # b'# type stereographic\n'
  # b'# centrallongitude 25\n'
  # b'# centrallatitude 90\n'
  # b'# truelatitude 60\n'
  # b'# bottomleft 18.600000 57.930000\n'
  # b'# topright 34.903000 69.005000\n'
  # b'# }\n'

  pgm_header = f'{width} {height}\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
  pgm_header = f'{maxval}\n'
  file_handle.write ( bytearray ( pgm_header, 'ascii' ) ) 
#
#  Convert 2D array to 1D vector.
#
  grayV = np.reshape ( gray, width * height )
#
#  Pack entries of vector into a string of bytes, replacing each integer
#  as an unsigned 1 byte character.
#
  grayB = struct.pack ( '%sB' % len(grayV), *grayV )
  file_handle.write ( grayB )
 
  file_handle.close ( )

  return

def pgmb_write_test ( ):

#*****************************************************************************80
#
## PGMB_WRITE_TEST tests PGMB_WRITE.
#
#  Licensing:
#
#    This code is distributed under the GNU LGPL license. 
#
#  Modified:
#
#    14 May 2017
#
#  Author:
#
#    John Burkardt
#
  import numpy as np
  import platform

  print ( '' )
  print ( 'PGMB_WRITE_TEST:' )
  print ( '  Python version: %s' % ( platform.python_version ( ) ) )
  print ( '  PGMB_WRITE writes a binary PGM graphics file.' )

  file_name = '/Users/wenjianzhu/Downloads/pgmb_io_test.pgm'
  width = 24
  height = 7
  maxval = 255

  gray = np.array \
  ( [ \
    [ 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0], \
    [ 0,  64,  64,  64,  64,   0,   0, 128, 128, 128, 128,   0,   0, 192, 192, 192, 192,   0,   0, 255, 255, 255, 255,   0], \
    [ 0,  64,   0,   0,   0,   0,   0, 128,   0,   0,   0,   0,   0, 192,   0,   0,   0,   0,   0, 255,   0,   0, 255,   0], \
    [ 0,  64,  64,  64,   0,   0,   0, 128, 128, 128,   0,   0,   0, 192, 192, 192,   0,   0,   0, 255, 255, 255, 255,   0], \
    [ 0,  64,   0,   0,   0,   0,   0, 128,   0,   0,   0,   0,   0, 192,   0,   0,   0,   0,   0, 255,   0,   0,   0,   0], \
    [ 0,  64,   0,   0,   0,   0,   0, 128, 128, 128, 128,   0,   0, 192, 192, 192, 192,   0,   0, 255,   0,   0,   0,   0], \
    [ 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0]  \
  ] )

  pgmb_write ( file_name, width, height, maxval, gray )

  print ( '' )
  print ( '  Graphics data stored in file "%s".' % ( file_name ) )
#
#  Terminate.
#
  print ( '' )
  print ( 'PGMB_WRITE_TEST:' )
  print ( '  Normal end of execution.' )
  return

if ( __name__ == '__main__' ):
  pgmb_write_test ( )
