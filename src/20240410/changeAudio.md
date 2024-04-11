

ffmpeg -i your_video.mp4 -i ret.aiff -c:v copy -map 0:v:0 -map 1:a:0 output.mp4


ffmpeg -i /Users/u03013112/Downloads/日韩AI测试素材/TH_20240227_KOL0009JP-KOL日语_JTT_LIVEDGE_1080X1350_JA_无水印.mp4 -i audios/ret.aiff -c:v copy -map 0:v:0 -map 1:a:0 output.mp4