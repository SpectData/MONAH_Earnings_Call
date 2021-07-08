'''
Main manager for formulating vpa text_blob
'''
import os
import pandas as pd
from datetime import datetime

import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.copy_mp4_files as cmf
# import Python.Data_Preprocessing.Stage_2.Actions.talkturn_family_actions as tfa
import Python.Data_Preprocessing.Stage_2.Prosody.talkturn_family_prosody as tfp
# import Python.Data_Preprocessing.Stage_2.Prosody.talkturn_pitch_vol as tpv
import Python.Data_Preprocessing.Stage_3.narrative_fine as atb
import Python.Data_Preprocessing.config.dir_config as prs
# import Python.Data_Preprocessing.Stage_1.OpenFace.execute_open_face_to_dataset as opf
import Python.Data_Preprocessing.Stage_1.Vokaturi.execute_vokaturi as exv
import Python.Data_Preprocessing.Stage_1.Audio_files_manipulation.convert_mp3_to_wav as cmw
import Python.Data_Preprocessing.Stage_1.Google_speech_to_text.execute_google_speech_to_text as gst
import Python.Data_Preprocessing.Stage_2.Verbatim.execute_weaving_talkturn as wvt


def weave_vpa(video_1, video_2, delay, tone, speech_rate, au_action, posiface, smile,
              headnod, leanforward, parallel_run_settings):
    '''
    Weave text blob for vpa family
    :param video_name_1: name of first video file
    :param video_name_2: name of second video file
    :return: none
    '''
    overall_start = datetime.now()
    start = datetime.now()

    # Stage 1 runs - transcripts
    cmf.run_creating_directories(video_1, video_2, parallel_run_settings)
    # TODO: write if statements to detect if we can skip some steps
    # exa.run_extracting_audio(parallel_run_settings)
    cmw.convert_to_normalized_wav(video_1, parallel_run_settings)
    gst.run_google_speech_to_text(video_1, video_2, parallel_run_settings)
    word_transcript = pd.read_csv(os.path.join(parallel_run_settings['csv_path'], 'Stage_1', 'word_transcripts.csv'))
    # opf.run_open_face(video_1, video_2, parallel_run_settings)
    if word_transcript.shape[0] == 0:
        print('audio file is unrecognizable.')
    else:
        wvt.run_weaving_talkturn(video_1, video_2, parallel_run_settings,
                                 input_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                             '',
                                                             'Stage_1',
                                                             "word_transcripts.csv"),
                                 output_filepath=os.path.join(parallel_run_settings['csv_path'],
                                                              '',
                                                              'Stage_2',
                                                              'weaved talkturns.csv'))
        exv.run_vokaturi(video_1, video_2, parallel_run_settings)
        print("Done data processing - Stage 1")

        print('Stage 1 Time: ', datetime.now() - start)
        start = datetime.now()

        # Stage 2 runs - processed tables
        # About 19 seconds
        # tpv.create_talkturn_pitch_vol(video_1, video_2, parallel_run_settings, require_pitch_vol=True)

        # TODO: to resume here, all downstream reference to 'weaved talkturn.csv' should change
        # to 'talkturn_pitch_vol.csv'

        # tfp.combine_prosody_features(video_1, video_2, parallel_run_settings)
        # tfa.combine_actions_features(video_1, video_2, parallel_run_settings)

    # print("Done data processing - Stage 2")

    # print('Stage 2 Time: ', datetime.now() - start)
    # start = datetime.now()
    '''
    # Stage 3 - runs - text blob narratives
    atb.weave_narrative(video_1, video_2,
                        delay, tone, speech_rate,
                        au_action, posiface, smile,
                        headnod, leanforward,
                        parallel_run_settings)
    print("Done data processing - Stage 3")

    print('Stage 3 Time: ', datetime.now() - start)
    print('All Stages Run Time: ', datetime.now() - overall_start)
    '''


if __name__ == '__main__':
    parallel_run_settings = prs.get_parallel_run_settings('marriane_win')

    mp3_list = []
    for file in os.listdir(parallel_run_settings['wav_path']):
        if file.endswith('.mp3'):
            mp3_list.append(file)

    for file in mp3_list:
        weave_vpa(video_1=file[:-4],
                  video_2=file[:-4],
                  delay=0,
                  tone=1,
                  speech_rate=1,
                  au_action=0,
                  posiface=0,
                  smile=0,
                  headnod=0,
                  leanforward=0,
                  parallel_run_settings=parallel_run_settings)

    tfp.combine_prosody_features(parallel_run_settings)

    print("Done data processing - Stage 2")

    # Stage 3 - runs - text blob narratives
    atb.weave_narrative(delay=0,
                        tone=1,
                        speech_rate=1,
                        au_action=0,
                        posiface=0,
                        smile=0,
                        headnod=0,
                        leanforward=0,
                        parallel_run_settings=parallel_run_settings)
    print("Done data processing - Stage 3")