# from sqlalchemy import create_engine

# db_string = "postgres://data:data@0.0.0.0:5433/data"

# db = create_engine(db_string)

# # Read
# result_set = db.execute("SELECT * FROM pg_catalog.pg_tables")  
# for r in result_set:  
#     print(r)

import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

with open(".secrets/spotify.json") as f:
    credential = json.loads(f.read())

if __name__ == "__main__":
    client_credentials_manager = SpotifyClientCredentials(**credential['spotify'])
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlists = sp.user_playlists('spotify')
    while playlists:
        for i, playlist in enumerate(playlists['items']):
            print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))
        if playlists['next']:
            playlists = sp.next(playlists)
        else:
            playlists = None