from multiprocessing import Pool
import os
import time

import pandas as pd


class ListenHistoryPipeline(object):

    def __init__(self):
        self.DATA_PATH = "./data"
        self.RAW_GPLAY_HISTORY = '/'.join([self.DATA_PATH,
                                           's1',
                                           'takeout-20200209T182806Z-001',
                                           'Takeout',
                                           'Google Play Music',
                                           'Tracks'])
        self.RAW_YOUTUBE_HISTORY = '/'.join([self.DATA_PATH,
                                             's1',
                                             'takeout-20200209T235558Z-001',
                                             'Takeout',
                                             'YouTube',
                                             'history',
                                             'watch-history.json'])
        self.GPLAY_HISTORY_PATH = '/'.join([self.DATA_PATH,
                                            's2',
                                            'gplay_history.csv'])
        self.YT_HISTORY_PATH = '/'.join([self.DATA_PATH,
                                         's2',
                                         'yt_history.csv'])
        self.YT_DATA_PATH = '/'.join([self.DATA_PATH,
                                      's1',
                                      'youtube'])
        self.YTM_DF_PATH = '/'.join([self.DATA_PATH,
                                     's2',
                                     'youtube_music.csv'])
        self.HISTORY_DF_PATH = '/'.join([self.DATA_PATH,
                                         's3',
                                         'history.csv'])

    def read_file(self, f):
        return pd.read_csv('/'.join([self.RAW_GPLAY_HISTORY, f]))

    def process_gplay_history(self):
        gplay_history_files = os.listdir(self.RAW_GPLAY_HISTORY)
        with Pool(12) as p:
            play_history = pd.concat(
                p.map(self.read_file, gplay_history_files)
            )
        play_history.to_csv(self.GPLAY_HISTORY_PATH, index=False)

    def read_yt_history(self):
        yt_history = pd.read_json(self.RAW_YOUTUBE_HISTORY)
        yt_history.to_csv(self.YT_HISTORY_PATH, index=False)

    def process_s1(self):
        start = time.time()
        self.process_gplay_history()
        self.read_yt_history()
        s1_runtime = time.time()-start
        print(f"S1 Processed in {s1_runtime}")
