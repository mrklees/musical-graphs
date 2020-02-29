import json
from spotipy import Spotify, util


class Worker(object):
    def __init__(self, graph):
        # Generate a token so that the handoff is taken careoff
        _ = self.generate_token()
        self.graph = graph
    
    def add_genres(self, artists):
        """Add genres of each artist to graph"""
        for artist in artists:
            raw = self.search_for_artist(artist)
            try:
                processed = self.get_artist_meta(raw)
                for genre in processed['genres']:
                    self.graph.G.add_node(genre, type='genre')
                    self.graph.G.add_edge(artist, genre)
            except TypeError:
                pass
        self.graph.save()

    def add_artists(self, artists):
        """Add artists to graph"""
        self.graph.G.add_nodes_from(artists, type='artist')
        self.graph.save()

    def search_for_artist(self, artist):
        """Searches for the artist and returns the data from the first result"""
        token = self.generate_token()
        if token:
            sp = Spotify(auth=token)
            search_results = sp.search(q=artist, type='artist')
            try:
                first_result = search_results['artists']['items'][0]
                return first_result
            except IndexError:
                pass

    def get_artist_meta(self, response):
        """Processes the json response for the desired fields"""
        fields = ['genres', 'id', 'name']
        if response is not None:
            return {field: response[field] for field in fields}

    def generate_token(self):
        scope = 'user-library-read'
        token = util.prompt_for_user_token("vpwyi3535uqqz9rd2tenemfed",
                                scope,
                                client_id='14c8f3b01396406499fdc2b2471cd8e2',
                                client_secret='3c12cef4352b4dcd8b9e48a9481115ad',
                                redirect_uri='http://localhost/')
        return token