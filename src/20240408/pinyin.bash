#!/bin/bash

# 遍历所有参数
for file in "$@"
do
  # 获取文件的目录、文件名和扩展名
  dir=$(dirname "$file")
  base=$(basename "$file")
  ext="${base##*.}"
  fname="${base%.*}"

  # 将文件名转换为拼音
  pinyin=$(pypinyin -s NORMAL "$fname" | tr -d ' ')

  # 构造新的文件路径
  newfile="$dir/$pinyin.$ext"

  # 重命名文件
  mv "$file" "$newfile"
  echo "$file -> $newfile"
done
