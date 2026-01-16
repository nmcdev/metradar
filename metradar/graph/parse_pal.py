
# _*_ coding: utf-8 _*_

'''
专门用来解析色标文件的脚本
'''



import numpy as np
from matplotlib.colors import LinearSegmentedColormap, Normalize
import matplotlib.pyplot as plt
from matplotlib.colorbar import ColorbarBase
import re

# 请将色标文件中的solidcolor字段替换为color
def parse(file):
	lines = []
	vals = []
	red = []
	green = []
	blue = []
	last_color = None
	transit_flag = False
	with open(file, 'r',encoding='gb18030') as f:
		for line in f:
			if not line.lower().startswith('color:'):
				continue
			l = line.lower().lstrip('color: ').strip()
			lines.append(l)
	lines.sort(key=lambda x: float(x.split(' ')[0]))
	color_len = len(lines)
	for idx, l in enumerate(lines):
		segs = [i for i in l.split(' ') if i]
		vals.append(float(segs[0]))
		current_color = tuple(int(i) / 255 for i in segs[1:4])
		if color_len - idx == 2:
			transit_flag = transit_flag if transit_flag else False
		if not isinstance(last_color, tuple) and len(segs) == 3:
			red.append((0, current_color[0]))
			green.append((0, current_color[1]))
			blue.append((0, current_color[2]))
			last_color = current_color
		else:
			if len(segs) == 7 or color_len - idx == 2:
				transit_color = tuple(int(i) / 255 for i in segs[4:7])
				if transit_flag:
					red.append((last_color[0], current_color[0]))
					green.append((last_color[1], current_color[1]))
					blue.append((last_color[2], current_color[2]))
				else:
					red.append((current_color[0], current_color[0]))
					green.append((current_color[1], current_color[1]))
					blue.append((current_color[2], current_color[2]))
				last_color = transit_color
				transit_flag = True
			else:
				red.append((current_color[0], current_color[0]))
				green.append((current_color[1], current_color[1]))
				blue.append((current_color[2], current_color[2]))
				last_color = current_color
				transit_flag = False
	norm_array = (np.array(vals) - vals[0]) / (vals[-1] - vals[0])
	cdict = {'red':[], 'green':[], 'blue':[]}
	for idx in range(len(norm_array)):
		cdict['red'].append((norm_array[idx],) + red[idx])
		cdict['green'].append((norm_array[idx],) + green[idx])
		cdict['blue'].append((norm_array[idx],) + blue[idx])
	return LinearSegmentedColormap('cmap', cdict), Normalize(vals[0], vals[-1])

# 可以支持alpha，朱文剑，20210831
def parse_pro(file):
	lines = []
	vals = []
	red = []
	green = []
	blue = []
	alpha=[]
	last_color = None
	transit_flag = False
	units=''
	with open(file, 'r') as f:
		for line in f:
			if line.lower().startswith('units'):
				units = re.split(r"[ ]+",line)[1].strip()
			if not line.lower().startswith('color'):
				continue
			lines.append(line.strip())
	lines.sort(key=lambda x: float(re.split(r"[ ]+",x)[1]))
	color_len = len(lines)
	for idx, l in enumerate(lines):
		segs = [i for i in l.split(' ') if i]
		vals.append(float(segs[1]))
		
		current_color = tuple(int(i) / 255 for i in segs[2::])
		
		if color_len - idx == 2:
			transit_flag = transit_flag if transit_flag else False
		
		if len(segs) >=8 or color_len - idx == 2:
			if len(segs) == 10:
				transit_color = tuple(int(i) / 255 for i in segs[6:10])
			elif len(segs)==8:
				transit_color = tuple(int(i) / 255 for i in segs[5:8])
			
			if transit_flag:
				red.append((last_color[0], current_color[0]))
				green.append((last_color[1], current_color[1]))
				blue.append((last_color[2], current_color[2]))
				if segs[0]=='Color4:':
					alpha.append((last_color[3], current_color[3]))
				else:
					alpha.append((1, 1))
			else:
				red.append((current_color[0], current_color[0]))
				green.append((current_color[1], current_color[1]))
				blue.append((current_color[2], current_color[2]))
				if segs[0]=='Color4:':
					alpha.append((current_color[3], current_color[3]))
				else:
					alpha.append((1.0, 1.0))
			if len(segs) == 10 or len(segs) == 8:
				last_color = transit_color
				transit_flag = True
		else:
			red.append((current_color[0], current_color[0]))
			green.append((current_color[1], current_color[1]))
			blue.append((current_color[2], current_color[2]))
			if segs[0]=='Color4:':
				alpha.append((current_color[3], current_color[3]))
			else:
				alpha.append((1.0, 1.0))
			last_color = current_color
			transit_flag = False

	norm_array = (np.array(vals) - vals[0]) / (vals[-1] - vals[0])
	cdict = {'red':[], 'green':[], 'blue':[], 'alpha':[]}
	
	for idx in range(len(norm_array)):
		cdict['red'].append((norm_array[idx],) + red[idx])
		cdict['green'].append((norm_array[idx],) + green[idx])
		cdict['blue'].append((norm_array[idx],) + blue[idx])
		cdict['alpha'].append((norm_array[idx],) + alpha[idx])
	
	outdic=dict()
	outdic['cmap'] = LinearSegmentedColormap('cmap', cdict)
	outdic['norm'] = Normalize(vals[0], vals[-1])
	outdic['units'] = units
	return outdic

if __name__ == "__main__":

	outdic = parse_pro('gr2_colors/IR_dark_alpha.pal')
	fig = plt.figure(figsize=(3, 11))
	ax = plt.gca()
	cbar = ColorbarBase(ax, orientation="vertical", cmap=outdic['cmap'], norm=outdic['norm'])
	plt.show()