# ComfyUI 部署

下载 ComfyUI

```bash
git clone https://github.com/YanWenKun/ComfyUI-Docker.git
```

网络代理脚本
添加文件storage/scripts/set-proxy.sh

文件内容如下

```bash
#!/bin/bash
set -eu
export HTTP_PROXY=http://172.17.0.1:20171
export HTTPS_PROXY=$HTTP_PROXY
export http_proxy=$HTTP_PROXY
export https_proxy=$HTTP_PROXY
export NO_PROXY="localhost,*.local,*.internal,[::1],fd00::/7,
10.0.0.0/8,127.0.0.0/8,169.254.0.0/16,172.16.0.0/12,192.168.0.0/16,
10.*,127.*,169.254.*,172.16.*,172.17.*,172.18.*,172.19.*,172.20.*,
172.21.*,172.22.*,172.23.*,172.24.*,172.25.*,172.26.*,172.27.*,
172.28.*,172.29.*,172.30.*,172.31.*,172.32.*,192.168.*,
*.cn,ghproxy.com,*.ghproxy.com,ghproxy.org,*.ghproxy.org,
gh-proxy.com,*.gh-proxy.com,ghproxy.net,*.ghproxy.net"
export no_proxy=$NO_PROXY
echo "[INFO] 代理设置为 $HTTP_PROXY"
```

其中172.17.0.1是通过下面方式获得

```bash
HOST_IP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
```

然后启动docker

```bash
cd ComfyUI-Docker
docker compose up -d
```

下载模型

下载到storage/ComfyUI/models/checkpoints目录，依旧使用代理

```bash
curl -x http://127.0.0.1:20171 -L -o v1-5-pruned.safetensors "https://huggingface.co/runwayml/stable-diffusion-v1-5/resolve/main/v1-5-pruned.safetensors?download=true"
```

在weiUI上进行组件的配置或者自行下载到storage/ComfyUI/custom_nodes，比如汉化界面

```bash
cd storage/ComfyUI/custom_nodes
git clone https://github.com/AIGODLIKE/AIGODLIKE-COMFYUI-TRANSLATION.git
```

然后在webUI上重启服务即可

在UI模式下安装插件ComfyUI-AnimateDiff-Evolved。
然后再下载动画模型，放到storage/ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models/下

```bash
curl -x http://127.0.0.1:20171 -L -o v3_sd15_mm.ckpt "https://huggingface.co/guoyww/animatediff/resolve/main/v3_sd15_mm.ckpt"
```

其中模型下载地址来自https://github.com/guoyww/AnimateDiff?tab=readme-ov-file，这是这个插件在github上的地址。可能还有别的模型，需要之后再找。

