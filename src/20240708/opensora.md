# open sora 部署

安装docker（阿里云主机）

```bash
sudo apt update
apt install docker-ce
```


获得代码，在合适的位置，比如 /home/git下

```bash
git clone https://github.com/hpcaitech/Open-Sora.git
```

制作docker镜像
`注意：由于阿里云主机的连接外网有困难，可能需要使用加速器或者代理`

```bash
docker build -t opensora .
```

docker对nvidia的支持

```bash
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
```

修改  /etc/docker/daemon.json

```json
{
    "runtimes": {
        "nvidia": {
            "path": "nvidia-container-runtime",
            "runtimeArgs": []
        }
    }
}
```

重启docker

```bash
sudo systemctl restart docker
```

docker 启动
进入opensora目录
    
```bash
docker run -ti --gpus all -v .:/workspace/Open-Sora --network=host -d --name opensora opensora
```


设置代理


```bash
export HTTP_PROXY=http://localhost:20171
export HTTPS_PROXY=http://localhost:20171
export NO_PROXY=localhost,127.0.0.1
```

本地部署 gradio

```bash
pip install gradio spaces
python gradio/app.py --port 2002
```



