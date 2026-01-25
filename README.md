<div align="center">

# Welcome to METRADAR📡

## 🚀 Quick start
### 📡metradar 主要面向一线气象工作者，提供简单易用的python接口，用于读取、处理和可视化中国天气雷达数据，并利用国际成熟的雷达工具包如pyart、wradlib、pydda、pysteps构建了质量控制、降水反演、风场反演、临近预报等工程。


<div align="left">

### ✨强烈推荐先从examples目录下的所有notebook入手！
### ✨由于很多算法功能都是以完整项目形式共享，因此，建议用户将整个代码包从github上下载到本地进行后续开发！
### ✨培训视频连接：https://www.bilibili.com/video/BV112BfBzEXq?vd_source=46a14909e851f4367d42b635b08f9160
### 📚主要功能包括
* 1，雷达数据下载，自动站数据下载，下载功能集成在nmc_met_io工具包中；
* 2，雷达基数据的读取（中国气象局最新的标准数据格式FMT）、绘图；
* 3，ROSE PUP 产品读取、绘图；
* 4，雷达拼图解码、绘图，支持SWAN格式拼图、中国气象探测中心拼图等；
* 5，雷达和自动站综合分析及绘图，自动站数据诊断分析包括散度、涡度计算、等值线客观分析等。
* 6，雷达质控、批量绘图、三维组网产品制作、降水估测、三维风场反演、回波临近预报、雷达和自动站时间序列图等，其中部分算法以项目形式开源，提供一键式处理流程。

## 🛠️安装
用下面的命令行进行安装:

* 使用pypi安装源安装(https://pypi.org/project/metradar/)
```
  pip install metradar
```
### 📂若要构建全功能运行环境，建议安装顺序如下：
* conda create -n radar312 python=3.12
* conda activate radar312
* pip install metradar
* conda install -c conda-forge pysteps -y
* conda install -c conda-forge arm_pyart -y
* conda install -c conda-forge gdal -y
* pip install tensorflow
* pip install tensorflow-probability
* pip install tf_keras

## 📜设置配置文件

* 在系统用户目录下("C:\Users\用户名"(windows)或"/home/用户名/"(Linux)), 建立文件夹".metradar"(若Windows下无法直接创建, 在命令窗口中输入`mkdir .metradar`创建)
* 在".metradar"中创建文本文件"config.ini", 内容模板为:
```
# 用于metradar相关参数设置
# 建议使用vscode进行编辑，或notepad++等编辑器进行编辑
# RESOURCES_PATH路径下存放了各种资源文件，包括地图、字体、色标等
[SETTING]
RESOURCES_PATH = /home/wjzhu/metradar/resources
```

### ⚠️注意事项：
* 资源文件RESOURCES 目录下的stations目录下的文件都是样例文件，不代表真实坐标，使用时，请自行按照同样的格式准备站点文件。
* 样例测试数据需要单独下载，链接：https://github.com/zhuwenjian/metradar_testdata

## 🚀补充材料：
### * 方便国内用户安装和使用的补充材料如下：
为了方便大家尽快上手metradar，将完整的python运行环境打包共享了（包括Linux系统和Windows系统的），从下面的夸克网盘或百度网盘下载即可。下载后，至于如何使用，请问AI智能体“如何用conda unpack命令还原用conda pack打包的虚拟环境”，你会得到详细的指南。此外，地图文件，测试数据等也可以从下面对应链接获取。如果下载离线包安装后依然无法正常使用，可能是操作系统和离线包不匹配，此时，建议手动安装各个包，具体指南见最后面的说明。

======================夸克网盘链接如下===========

Linux（Ubuntu22.04）上metradar完整版python运行环境离线包radar312_env.tar.gz
链接：https://pan.quark.cn/s/e7f7e4e3c268

Windows11上metradar完整版python运行环境离线包radar312_win.tar.gz
链接：https://pan.quark.cn/s/cec100cc4898

样例测试数据metradar_testdata
链接：https://pan.quark.cn/s/c756f461c7d8

metradar工具包介绍.pptx
链接：https://pan.quark.cn/s/0012cb00a686

metradar地图文件.zip
链接：https://pan.quark.cn/s/28a4f86a9e53

=================百度网盘链接如下==========

Linux（Ubuntu22.04）上metradar完整版python运行环境离线包radar312_env.tar.gz
链接：https://pan.baidu.com/s/18nqi-LxJoaXGiLWYITj6lQ?pwd=5wew 
提取码：5wew

Windows11上metradar完整版python运行环境离线包radar312_win.tar.gz
链接: https://pan.baidu.com/s/1TRaTvXq5xTz38atFnUa4gw?pwd=8vvg 提取码: 8vvg 

metradar工具包介绍.pptx
链接：https://pan.baidu.com/s/1Rfzo3zGu23hl4uvWPvsl9g?pwd=5wew 
提取码：5wew

metradar地图文件.zip
链接：https://pan.baidu.com/s/1e3tvtGzI4KlZU33bJq0BHg?pwd=5wew 
提取码：5wew

样例测试数据metradar_testdata
链接：https://pan.baidu.com/s/1cYUHGL6Cqk4rK5uZCXrUWg?pwd=5wew 
提取码：5wew

==============手动逐个包安装运行环境步骤：===========

首先，如果是Windows，安装anaconda软件  ，然后通过conda prompt命令行工具启动dos窗口；如果是Linux，可安装miniconda即可，然后确保conda初始化成功，如果conda没安装好，请寻求AI的帮助和指导。一切就绪之后，然后依次执行下面的命令行：

* conda create -n radar312 python=3.12 -y
* conda activate radar312 
* pip install metradar
* conda install -c conda-forge pysteps
* conda install -c conda-forge arm_pyart -y
* conda install -c conda-forge gdal -y
* pip install tensorflow
* pip install tensorflow-probability
* pip install tf_keras

### 👇适合新手的保姆级材料来了，主要内容是：1，共享了miniconda的安装文件；2，上传了完整的安装视频。链接如下：

==================夸克网盘=================

Linux版本的miniconda连接：
Miniconda3-latest-Linux-x86_64.sh
链接：https://pan.quark.cn/s/16c39c495762

Windows版本的miniconda连接：
Miniconda3-latest-Windows-x86_64.exe
链接：https://pan.quark.cn/s/392491e5b2a0

================百度网盘==================

Linux版本的miniconda安装文件
Miniconda3-latest-Linux-x86_64.sh
链接: https://pan.baidu.com/s/1aMFKaNbVfkPbIHziAH5lhg?pwd=jj5b 提取码: jj5b 

Windows版本的miniconda安装文件
Miniconda3-latest-Linux-x86_64.sh
链接: https://pan.baidu.com/s/1aMFKaNbVfkPbIHziAH5lhg?pwd=jj5b 提取码: jj5b 

Linux下安装metradar的视频：
https://www.bilibili.com/video/BV15DzCB1EsC/?spm_id_from=333.1387.list.card_archive.click

Windows下安装metradar的视频：
https://www.bilibili.com/video/BV1d2zCB7E1R?spm_id_from=333.788.videopod.sections

Good Luck！

### 如果下载离线安装环境，在还原的过程中，出现类似下面的提示：
PS C:\Users\xxx\anaconda3\envs\radar312_win\Scripts> .\conda-unpack.exe
SyntaxError: Non-UTF-8 code starting with '\xcb' in file \\?\C:\Users\xxx\anaconda3\envs\radar312_win\Scripts\conda-unpack-script.py on line 655, but no encoding declared; see https://peps.python.org/pep-0263/ for details

具体解决方案如下，将解压后的\C:\Users\xxx\anaconda3\envs\radar312_win\Scripts\conda-unpack-script.py脚本在VScode编辑器下用GBK编码方式打开，然后把# -*- coding: utf-8 -*- 添加到该conda-unpack-script.py文件的最前面，然后用utf-8编码进行保存即可。再用conda-unpack.exe命令，就可以正常了。