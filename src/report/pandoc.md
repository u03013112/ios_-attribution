# pandoc 做格式转化

目前的方案是使用pandoc将markdown转化为pdf

## 需要在基础镜像上安装pandoc

暂时不再重新build镜像，而是在容器启动后安装pandoc。

需要执行的命令如下：

```
apt-get install -y pandoc
apt-get install -y pandoc texlive-latex-base
apt-get install -y pandoc texlive-full
apt-get install -y fonts-wqy-zenhei
```

## 转化执行命令

```
pandoc /src/src/report/report.md -o ./report.pdf --latex-engine=xelatex
```

## 要求

在md的头上添加如下内容：

```
---
CJKmainfont: WenQuanYi Zen Hei
---
```
