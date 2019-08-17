#from pyechonest import artist
#from pyechonest import config
from pymongo import MongoClient
import datetime
import json
from urllib.request import urlopen

__author__ = 'rossetti'
__license__ = "GPL"
__email__ = "giulio.rossetti@gmail.com"


class DataGathering(object):

    def __init__(self, lastfm_api, echonest_api, db_config):
        """
        Constructor

        :param echonest_api:
        :param lastfm_api:
        :return:
        """
        self.lfm_apikey = lastfm_api
        #config.ECHO_NEST_API_KEY = echonest_api
        self.db_config = db_config

    def get_db_connection(self):
        client = MongoClient(self.db_config['host'], self.db_config['port'])
        if self.db_config['username'] != "":
            client.admin.authenticate('admin', 'crepuscolo84', 'admin')
            uri = "mongodb://%s:%s@%s/?authSource=%s" % (self.db_config['username'], self.db_config['password'],
                                                         self.db_config['host'], self.db_config['authsorce'])
            return MongoClient(uri)
        else:
            return client

    def get_user_detail(self, username):
        details = {'gender': None, 'age': None, 'country': None, 'playcount': None, 'registered_on': None}
        try:
            url = "http://ws.audioscrobbler.com/2.0/?method=user.getinfo&format=json&user=%s&api_key=%s" % \
                (username, self.lfm_apikey)
            response = urlopen(url)
            data = json.loads(response.read())['user']
            details['user_id'] = username
            details['crawled'] = True
            details['gender'] = data['gender']
            age = int(data['age'])
            details['age'] = None if age == 0 else age
            details['country'] = data['country']
            details['playcount'] = data['playcount']
            details['registered_on'] = datetime.datetime.fromtimestamp(data['registered']['unixtime'])
        except:
            pass

        return details

    def get_range_listening(self, username, start_date, end_date):
        listening = []
        page = 1

        while True:
            url = "http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=%s&api_key=%s&limit=200&page=%s&from=%s&to=%s&format=json" % \
                  (username, self.lfm_apikey, page, start_date.strftime("%s"), end_date.strftime("%s"))
            response = urlopen(url)

            try:
                data = json.loads(response.read())['recenttracks']['track']
            except:
                data = {}

            if len(data) == 0:
                break
            else:
                page += 1
                for l in data:
                    res = (l['name'].replace(".", ""), l['mbid'], l['artist']['#text'].replace(".", ""),
                           l['artist']['mbid'], l['album']['#text'].replace(".", ""), l['album']['mbid'],
                           datetime.datetime.fromtimestamp(int(l['date']['uts'])))
                    listening.append(res)

        for r in listening:
            db = self.get_db_connection()
            try:
                db.Lastfm_Profiles.listenings.insert_one({'user_id': username, 'track': r[0],
                                                       'artist': r[2],
                                                       'album': r[4],
                                                       'date': r[-1]})
            except:
                pass
            db.close()

        return listening

    def get_user_listening(self, username, hundreds_of_listenings=1):
        listening = []
        page = 1

        while page <= hundreds_of_listenings:
            url = "http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=%s&api_key=%s&limit=100&page=%s&format=json" % \
                  (username, self.lfm_apikey, page)
            response = urlopen(url)
            try:
                data = json.loads(response.read())['recenttracks']['track']
            except:
                data = {}

            if len(data) == 0:
                break
            else:
                page += 1
                for l in data:
                    res = (
                    l['name'].replace(".", ""), l['mbid'], l['artist']['#text'].replace(".", ""), l['artist']['mbid'],
                    l['album']['#text'].replace(".", ""), l['album']['mbid'],
                    datetime.datetime.fromtimestamp(int(l['date']['uts'])))
                    listening.append(res)

        for r in listening:
            db = self.get_db_connection()
            try:
                db.Lastfm_Profiles.listenings.insert_one({'user_id': username, 'track': r[0],
                                                       'artist': r[2],
                                                       'album': r[4],
                                                       'date': r[-1]})
            except:
                pass
            db.close()

        return listening

    def get_network(self, username):
        page = 1
        friends = []
        while True:
            url = "http://ws.audioscrobbler.com/2.0/?method=user.getfriends&user=%s&api_key=%s&page=%d&limit=100&format=json" % \
              (username, self.lfm_apikey, page)
            response = urlopen(url)
            print(url)
            try:
                data = json.loads(response.read())
                print(data)
            except:
                data = {}
            if len(data) == 0:
                break

            if len(data['friends']['user']) == 0:
                break

            for k in data['friends']['user']:
                friends.append(k['name'])
                details = {'gender': None, 'age': None, 'country': None, 'playcount': None, 'registered_on': None}
                try:
                    details['user_id'] = k['name']
                    details['crawled'] = False
                    details['gender'] = k['gender']
                    age = int(k['age'])
                    details['age'] = None if age == 0 else age
                    details['country'] = k['country']
                    details['playcount'] = k['playcount']
                    details['registered_on'] = datetime.datetime.fromtimestamp(k['registered']['unixtime'])
                    db = self.get_db_connection()
                    if db.Lastfm_Profiles.user_info.find_one({'user_id': k['name']}) is None:
                        db.Lastfm_Profiles.user_info.insert(details)
                    db.close()
                except:
                    pass
            page += 1
        return friends


    @staticmethod
    def get_genre(artist_name):
        """
        Echonest genre information gathering

        :param artist_name:
        :return:
        """

        # echonest way
        #try:
        #    art = artist.Artist(artist_name)
        #    for term in art.terms:
        #        res = (term['name'], term['frequency'])
        #        genres.append(res)
        #except:
        #    pass

        # Spotify way
        url = "https://api.spotify.com/v1/search?q=%s&type=artist&limit=1" % artist_name
        print(url)
        response = urlopen(url)

        try:
            data = json.loads(response.read())['artists']
            if len(data['items']) == 0:
                return {}
            else:
                genres_profile = {
                    'genres': data['items'][0]['genres'],
                    'spotify_id': data['items'][0]['id'],
                    'popularity': data['items'][0]['popularity']
                }
                return genres_profile
        except:
            return {}

    def get_artist_info(self, artist_name):
        url = "http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=%s&api_key=%s&format=json" % \
              (artist_name, self.lfm_apikey)

        response = urlopen(url)
        try:
            data = json.loads(response.read())
            if len(data) > 0 and 'artist' in data:
                artist_profile = {
                    "name": data['artist']['name'],
                    "stats": data['artist']['stats'],
                    "similar": [data['artist']['similar']['artist'][x]['name'] for x in range(0, len(data['artist']['similar']['artist']))],
                    "tags": [data['artist']['tags']['tag'][x]['name'] for x in range(0, len(data['artist']['tags']['tag']))],
                    "spotify_genres": {}
                }
                return artist_profile
            else:
                return {}
        except:
            return {}


if __name__ == "__main__":

    echonest_api = ""
    lastfm_api = ''

    db = {}
    db['host'] = 'localhost'
    db['port'] = 27017
    db['username'] = ''
    dg = DataGathering(lastfm_api, echonest_api, db)

    # res = dg.get_range_listening("giuliorossetti", datetime.datetime(2014,1,1), datetime.datetime.today())

    # res = dg.get_artist_info("Cher")
    # print(res)
    res = dg.get_network("giuliorossetti")
    print(res)

    # exit()
    user_listenings = dg.get_user_listening("giuliorossetti", hundreds_of_listenings=1)
    analyzed_artists = {}

    for l in user_listenings:
        art = l[2]
        if art not in analyzed_artists:
            analyzed_artists[art] = None
            # gn = dg.get_genre(art)
            print(art)#, gn)
