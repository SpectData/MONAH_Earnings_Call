import os
from pydub import AudioSegment, effects
import Python.Data_Preprocessing.config.dir_config as prs

def convert_to_normalized_wav(video_1, parallel_run_settings):
    # subfolder = video_1.split('_')[1] + '_' + video_1.split('_')[0]
    src = os.path.join(parallel_run_settings['wav_path'], video_1 + '.mp3')
    dst = os.path.join(parallel_run_settings['wav_path'], video_1 + '.wav')
    rawsound = AudioSegment.from_file(src, "mp3")
    normalizedsound = effects.normalize(rawsound)
    normalizedsound.export(dst, format="wav")

if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')
    convert_to_normalized_wav(video_1="BMI_20161018_f000005102",
                              parallel_run_settings=parallel_run_settings)
