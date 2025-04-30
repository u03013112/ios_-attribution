import cv2

video_path = '/src/data/videos/1.mp4'
import os
if not os.path.exists(video_path):
    print("Error: Video file does not exist.")

print(cv2.getBuildInformation())

cap = cv2.VideoCapture(video_path)

if not cap.isOpened():
    print("Error: Could not open video.")
else:
    ret, frame = cap.read()
    if not ret:
        print("Error: Could not read frame.")
    else:
        print("Frame read successfully.")

cap.release()
