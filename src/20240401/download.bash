#!/bin/bash

input_file="ads20240329.txt"
output_dir="/Users/u03013112/Downloads/ads20240401"
counter=1

mkdir -p "${output_dir}"

while IFS= read -r url
do
  output_file="${output_dir}/${counter}.mp4"
  wget -O "${output_file}" "${url}"
  counter=$((counter + 1))
done < "${input_file}"
