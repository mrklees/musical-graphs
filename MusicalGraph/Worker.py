import json
from spotipy import Spotify, util
from spotipy.oauth2 import SpotifyClientCredentials
from tqdm import tqdm


class Worker(object):
    """ The Worker does work on the Musical Graph

    This design allows for the separation of the graph itself and the
    code that processes it.
    """
    def __init__(self, graph):
        self.graph = graph

    def add_artists(self, artists):
        assert type(artists) == list
        """Add artists to graph"""
        for artist in tqdm(artists):
            self._add_artist(artist)
        self.graph.save()

    def _add_artist(self, artist):
        # We'll only add an artist if they return a record in the
        # Spotify API
        raw = self._search_for_artist(artist)
        try:
            processed = self._get_artist_meta(raw)
            self.graph.G.add_node(processed['name'], type='artist')
            for genre in processed['genres']:
                self.graph.G.add_node(genre, type="genre")
                self.graph.G.add_edge(artist, genre)
        except TypeError:
            pass
        self.graph.save()

    def _search_for_artist(self, artist):
        """Searches for the artist and returns the data from the first result
        """
        token = self._generate_token()
        if token:
            sp = Spotify(client_credentials_manager=token)
            search_results = sp.search(q=artist, type='artist')
            try:
                first_result = search_results['artists']['items'][0]
                return first_result
            except IndexError:
                pass

    def _get_artist_meta(self, response):
        """Processes the json response for the desired fields"""
        fields = ['genres', 'id', 'name']
        if response is not None:
            return {field: response[field] for field in fields}

    def _generate_token(self):
        with open('.secrets/spotify.json') as f:
            credential = json.loads(f.read())
        client_manager = SpotifyClientCredentials(**credential['spotify'])
        return client_manager
