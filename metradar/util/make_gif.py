

from PIL import Image
import os


filepath = 'pic'
outpath = 'pic'
filenames = os.listdir(filepath)
filenames = sorted(filenames)
images = []



for filename in filenames:
    if filename.startswith('.') or filename.startswith('..'):
        continue
    if not filename.endswith('.png'):
        continue

    images.append(Image.open(filepath + os.sep + filename))
    print(filename + ' added!')
images[0].save(outpath + os.sep + '雷达外推示例.gif', format='GIF',
                    append_images=images[1:], save_all=True, duration=int(500), loop=0)
