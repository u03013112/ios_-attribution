ffmpeg -i /Users/u03013112/Downloads/CFC0747EN原片720p3000.mp4 -vf "scale=720:1080,drawbox=x=0:y=0:w=64:h=36:color=black@1:t=fill" -c:a copy /Users/u03013112/Downloads/CFC0747EN原片720p3000b.mp4
ffmpeg -i /Users/u03013112/Downloads/连珠车原片720p2000.mp4 -vf "drawbox=x=0:y=0:w=64:h=36:color=white@1:t=fill" -c:a copy /Users/u03013112/Downloads/连珠车原片720p2000b.mp4
ffmpeg -i /Users/u03013112/Downloads/连珠车原片720p1250.mp4 -vf "drawbox=x=0:y=0:w=64:h=36:color=red@1:t=fill" -c:a copy /Users/u03013112/Downloads/连珠车原片720p1250b.mp4


ffmpeg -i /Users/u03013112/Downloads/CFC0747EN原片.mp4 -vf "scale=720:-1,drawbox=x=0:y=0:w=64:h=36:color=black@1:t=fill" -b:v 3000k -c:a copy /Users/u03013112/Downloads/CFC0747EN原片720p3000b.mp4
ffmpeg -i /Users/u03013112/Downloads/CFC0747EN原片.mp4 -vf "scale=720:-1,drawbox=x=0:y=0:w=64:h=36:color=white@1:t=fill" -b:v 2000k -c:a copy /Users/u03013112/Downloads/CFC0747EN原片720p2000b.mp4
ffmpeg -i /Users/u03013112/Downloads/CFC0747EN原片.mp4 -vf "scale=720:-1,drawbox=x=0:y=0:w=64:h=36:color=red@1:t=fill" -b:v 1250k -c:a copy /Users/u03013112/Downloads/CFC0747EN原片720p1250b.mp4


ffmpeg -i 连珠车原片.mp4 -c:v libx265 -crf 30 -preset veryslow -profile:v main -vf "scale=720:-1,drawbox=x=0:y=0:w=64:h=36:color=yellow@1:t=fill" -x265-params "psy-rd=2.0:psy-rdoq=1.0:aq-mode=3:merange=32:bframes=6:ref=6" -c:a aac -b:a 96k 连珠车ffmpegb.mp4
