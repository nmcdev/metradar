<div align="center">

# Welcome to METRADAR📡

## 🚀 Quick start
### 📡metradar 主要面向一线气象工作者，提供简单易用的python接口，用于读取、处理和可视化中国天气雷达数据，并兼容国际成熟的雷达工具包如pyart、wradlib、pydda、pysteps等。
### ✨强烈推荐先从examples目录下的所有notebook入手！
### ✨由于很多算法功能都是以完整项目形式共享，因此，建议用户将整个代码包从github上下载到本地进行后续开发！




<div align="left">

### 📚主要功能包括
* 1，雷达数据下载，自动站数据下载，下载功能集成在nmc_met_io工具包中；
* 2，雷达基数据的读取（中国气象局最新的标准数据格式FMT）、绘图；
* 3，ROSE PUP 产品读取、绘图；
* 4，雷达拼图解码、绘图，支持SWAN格式拼图、中国气象探测中心拼图等；
* 5，雷达和自动站综合分析及绘图，自动站数据诊断分析包括散度、涡度计算、等值线客观分析等。
* 6，雷达质控、批量绘图、三维组网产品制作、降水估测、三维风场反演、回波临近预报、雷达和自动站时间序列图等，其中部分算法以项目形式开源，提供一键式处理流程。

## 🛠️Install
Using the fellowing command to install packages:

* 使用pypi安装源安装(https://pypi.org/project/metradar/)
```
  pip install metradar
```
### 📂若要构建全功能运行环境，建议安装顺序如下：
* conda create -n radar312 python=3.12
* conda activate radar312
* pip install metradar
* conda install -c conda-forge arm_pyart -y
* conda install -c conda-forge cartopy -y
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
* 资源文件RESOURCES和样例测试数据需要单独下载
