import cv2

def cutVideo(srcVideoFilename, saveVideoFilename, s=0, d=3, fps=0):
    # 打开源视频文件
    cap = cv2.VideoCapture(srcVideoFilename)
    
    # 获取源视频的帧速率和帧数
    original_fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    # 如果未指定帧速率，则使用源视频的帧速率
    if fps == 0:
        fps = original_fps
    
    # 计算开始帧和结束帧
    start_frame = int(s * original_fps)
    end_frame = int((s + d) * original_fps)
    
    # 设置视频写入器
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(saveVideoFilename, fourcc, fps, 
                          (int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), 
                           int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))))
    
    # 跳到开始帧
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    
    # 读取并写入指定时间段的视频帧
    current_frame = start_frame
    while current_frame < end_frame and current_frame < total_frames:
        ret, frame = cap.read()
        if not ret:
            break
        out.write(frame)
        current_frame += 1
    
    # 释放资源
    cap.release()
    out.release()


if __name__ == "__main__":
    cutVideo("/Users/u03013112/Downloads/视频-剧情-一镜到底.mp4", "output.mp4", s=0, d=3, fps=0)

