import json
from multiprocessing import Pool
import os
import requests
import time

import pandas as pd
from tqdm import tqdm


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
        with open("./.secrets/youtube.json") as f:
            self.youtube_key = json.loads(f.read())

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

    def request_yt_data(self, id):
        '''
        YouTube Music is the worst, so we need to make API calls to YT to
        get some details on some of the videos.

        We will extract the video name (hopefully a the song name), channel
        title
        '''
        response = requests.get(f'https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics&id={id}&key={self.youtube_key}')
        return response.text

    def process_yt_response(self, response):
        meta = ['title', 'channelTitle']
        # Extract title and channel name from YT API response
        try:
            return {data: json.loads(response)["items"][0]['snippet'][data]
                    for data in meta}
        except KeyError:
            return {data: '' for data in meta}

    def get_meta(self, id):
        # Check if data has already been pulled
        if (f'{id}.json' in os.listdir(self.YT_DATA_PATH)):
            with open('/'.join([self.YT_DATA_PATH, f'{id}.json'])) as f:
                return json.loads(f.read())
        else:
            response = self.request_yt_data(id)
            processed = self.process_yt_response(response)
            with open('/'.join([self.YT_DATA_PATH, f'{id}.json']), 'w+') as f:
                f.write(json.dumps(processed))
            return processed

    def clean_up_empties(self):
        killcount = 0
        for data in os.listdir(self.YT_DATA_PATH):
            joint_path = '/'.join([self.YT_DATA_PATH, data])
            if os.path.getsize(joint_path) == 33:
                os.remove(joint_path)
                killcount += 1
        print(f"Cleaned up {killcount} files")

    def process_s2(self):
        start = time.time()
        yt_history = pd.read_csv(self.YT_HISTORY_PATH)

        # Filter YouTube history to YouTube Music
        ytm = yt_history[yt_history.header == 'YouTube Music']

        # Create id column
        ytm['id'] = ytm.titleUrl.str.rsplit("=", expand=True).loc[:, 1]

        # Pull meta data from YouTube Data API
        metadata = [self.get_meta(id) for id in tqdm(ytm['id'])]

        # Extract data of interest from response
        ytm['title'] = [data['title'] for data in metadata]
        ytm['channelTitle'] = [data['channelTitle'] for data in metadata]

        # Clean up responses which weren't found
        self.clean_up_empties()

        # Write result to build
        ytm.to_csv(self.YTM_DF_PATH, index=False)

        s2_runtime = time.time()-start
        print(f"S2 Processed in {s2_runtime}")

    def clean_VEVO(self, artist_col):
        """ Remove VEVO from artist name if it appears"""
        results = []
        for artist in artist_col:
            if artist.endswith("VEVO"):
                results.append(artist[-4])
            else:
                results.append(artist)
        return results

    def process_s3(self):
        start = time.time()
        # Read in S2 data
        gplay_history = pd.read_csv(self.GPLAY_HISTORY_PATH)
        ytm_history = pd.read_csv(self.YTM_DF_PATH)

        # Clean Google Play DF
        gplay_history.rename(
            {"Title": "title", "Artist": "artist", "Play Count": "listens"}, 
            axis=1,
            inplace=True
        )

        # Clean Youtube Music History
        ytm_history['artist'] = ytm_history.channelTitle.str.split(" - ", expand=True).loc[:, 0]
        ytm_history['artist'] = self.clean_VEVO(ytm_history.artist)
        ytm_history = (
            ytm_history
                .dropna(subset=['title', 'artist'])
                .groupby(['title', 'artist'])
                .agg({'id':'count'})
                .reset_index()
                .rename({'id': "listens"}, axis=1)
        )
        cols = ['title', 'artist', 'listens']
        gp = gplay_history[cols]
        yt = ytm_history[cols]
        history = pd.concat([gp, yt])
        combined = (
            history
                .groupby(['title', 'artist'])
                .agg({'listens':'sum'})
                .reset_index()
        )
        combined.to_csv(self.HISTORY_DF_PATH, index=False)
        
        s3_runtime = time.time()-start
        print(f"S3 Processed in {s3_runtime}")
