import os
import subprocess
import pandas as pd
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from config import SPEECH_KEY,SPEECH_REGION

import os
import azure.cognitiveservices.speech as speechsdk

speech_key = SPEECH_KEY
service_region = SPEECH_REGION

speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=service_region)
# Note: the voice setting will not overwrite the voice element in input SSML.
speech_config.speech_synthesis_voice_name = "zh-CN-XiaoxiaoNeural"

# audio_config = speechsdk.audio.AudioOutputConfig(filename="outputaudio.wav")


# https://speech.azure.cn/portal/voicegallery
# text = "你好，这是晓晓。"

ssmlText = '''
<speak xmlns="http://www.w3.org/2001/10/synthesis" xmlns:mstts="http://www.w3.org/2001/mstts"
    xmlns:emo="http://www.w3.org/2009/10/emotionml" version="1.0" xml:lang="zh-CN">
    <voice name="zh-CN-XiaoxiaoNeural">
        <s />
        <mstts:express-as style="excited">我试玩了最近讨论度很高的王国之歌。</mstts:express-as>
        <s />
    </voice>
</speak>
'''

# use the default speaker as audio output.
speech_synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config, audio_config=None)

result = speech_synthesizer.speak_ssml_async(ssmlText).get()
# Check result
if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
    # print("Speech synthesized for text [{}]".format(text))
    stream = speechsdk.AudioDataStream(result)
    stream.save_to_wav_file("outputaudio.wav")
    print("Audio data for text [{}] was saved to outputaudio.wav".format(ssmlText))
elif result.reason == speechsdk.ResultReason.Canceled:
    cancellation_details = result.cancellation_details
    print("Speech synthesis canceled: {}".format(cancellation_details.reason))
    if cancellation_details.reason == speechsdk.CancellationReason.Error:
        print("Error details: {}".format(cancellation_details.error_details))

