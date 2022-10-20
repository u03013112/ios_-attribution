#!/bin/bash
SRC=~/Documents/git/ios_-attribution
DST=root@192.168.40.62:/home/git/
trap 'exit' INT
while :
    do
        echo '----------------------------------------------------------------'
        fswatch -r -L -1 ${SRC}
        date
        rsync -av --exclude={".*","__pycache__/*","data/*.csv","mod/*.h5"} --delete ${SRC} ${DST}
        say 红鲤鱼与绿鲤鱼与驴
    done