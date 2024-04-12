import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import azure.cognitiveservices.speech as speechsdk
from config import SPEECH_KEY, SPEECH_REGION

def msTTS(text, outFileName, speechSynthesisVoiceName='zh-CN-XiaoxiaoNeural', style="excited", speed=1.0):
    speech_key = SPEECH_KEY
    service_region = SPEECH_REGION

    speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=service_region)
    speech_config.speech_synthesis_voice_name = speechSynthesisVoiceName

    ssmlText = '''
    <speak xmlns="http://www.w3.org/2001/10/synthesis" xmlns:mstts="http://www.w3.org/2001/mstts"
        xmlns:emo="http://www.w3.org/2009/10/emotionml" version="1.0" xml:lang="zh-CN">
        <voice name="{}">
            <s />
            <mstts:express-as style="{}">'''.format(speechSynthesisVoiceName, style)
    if speed != 1.0:
        if speed > 1.0:
            ssmlText += '<prosody rate="+{:.2f}%">'.format((speed - 1.0) * 100) + text + '</prosody>'
        else:
            ssmlText += '<prosody rate="{:.2f}%">'.format((speed - 1.0) * 100) + text + '</prosody>'
    else:
        ssmlText += text
    ssmlText += '''
            </mstts:express-as>
            <s />
        </voice>
    </speak>
    '''

    speech_synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config, audio_config=None)

    result = speech_synthesizer.speak_ssml_async(ssmlText).get()
    if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
        stream = speechsdk.AudioDataStream(result)
        stream.save_to_wav_file(outFileName)
        # print("Audio data for text [{}] was saved to {}".format(text, outFileName))
        # print(ssmlText)
    elif result.reason == speechsdk.ResultReason.Canceled:
        cancellation_details = result.cancellation_details
        print("Speech synthesis canceled: {}".format(cancellation_details.reason))
        if cancellation_details.reason == speechsdk.CancellationReason.Error:
            print("Error details: {}".format(cancellation_details.error_details))

if __name__ == '__main__':
    msTTS("我试玩了最近讨论度很高的王国之歌。", "outputaudio.wav",speed=1.5)