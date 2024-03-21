# 将视频分段
import os
import cv2

def cutVideo(videoPath, savePath, seconds=10):
    if not os.path.exists(savePath):
        os.makedirs(savePath)

    cap = cv2.VideoCapture(videoPath)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    totalFrame = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    print(f'fps: {fps}, totalFrame: {totalFrame}')

    frameCount = 0
    segmentCount = 1
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = None

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frameCount % (fps * seconds) == 0:
            if out is not None:
                out.release()

            output_filename = f'{savePath}/segment_{segmentCount}.mp4'
            out = cv2.VideoWriter(output_filename, fourcc, fps, (frame.shape[1], frame.shape[0]))
            segmentCount += 1

        if out is not None:
            out.write(frame)

        frameCount += 1

    if out is not None:
        out.release()

    cap.release()
    print(f'frameCount: {frameCount}')
    return frameCount


if __name__ == '__main__':
    videoPath = '/Users/u03013112/Downloads/视频-剧情-多分镜.mp4'
    savePath = '/Users/u03013112/Downloads/output'
    cutVideo(videoPath, savePath)