from bottle import route, run, request
from spotipy import oauth2
from pymongo import MongoClient
import urllib, os
import json

PORT_NUMBER = 8080
SPOTIPY_CLIENT_ID = 'bc548f85cdf24d8582dffcd8f829ca5c'
SPOTIPY_CLIENT_SECRET = 'ce8b1ac7f3e643b89d51c865639fc61a'
SPOTIPY_REDIRECT_URI = 'http://localhost:8080/callback'
SCOPE = 'user-library-read'
CACHE = '.spotipyoauthcache'

sp_oauth = oauth2.SpotifyOAuth(SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET,
                               SPOTIPY_REDIRECT_URI, scope=SCOPE, cache_path=CACHE)


@route('/')
def index():
    try:
        access_token = ""
        token_info = sp_oauth.get_cached_token()

        if token_info:
            print "Found cached token!"
            access_token = token_info['access_token']
        else:
            url = request.url
            code = sp_oauth.parse_response_code(url)
            if code:
                print "Found Spotify auth code in Request URL! Trying to get valid access token..."
                token_info = sp_oauth.get_access_token(code)
                access_token = token_info['access_token']

        if access_token:
            tracks_details(access_token)

        else:
            return htmlForLoginButton()
    except:
        callback()

@route('/callback')
def callback():
    try:
        access_token = ""

        token_info = sp_oauth.get_cached_token()

        if token_info:
            print "Found cached token!"
            access_token = token_info['access_token']
            print access_token
        else:
            url = request.url
            code = sp_oauth.parse_response_code(url)
            if code:
                print "Found Spotify auth code in Request URL! Trying to get valid access token..."
                token_info = sp_oauth.get_access_token(code)
                access_token = token_info['access_token']
                print access_token

        if access_token:
            tracks_details(access_token)

        else:
            return htmlForLoginButton()
    except:
        index()


def htmlForLoginButton():
    auth_url = getSPOauthURI()
    htmlLoginButton = "<a href='" + auth_url + "'>Login to Spotify</a>"
    return htmlLoginButton


def getSPOauthURI():
    auth_url = sp_oauth.get_authorize_url()
    return auth_url


def tracks_details(token):
    db = get_db_connection()
    dba = get_db_connection()
    try:
        res = dba.Lastfm_Profiles.listenings.find({'crawled':{'$exists': False}})

        for r in res:
            dba.Lastfm_Profiles.listenings.update_one({'date': r['date'], 'user_id': r['user_id']}, {"$set": {"crawled": True}})
            track = r['track']
            url = "https://api.spotify.com/v1/search?q=%s&type=track&" % track
            response = urllib.urlopen(url.encode("utf8"))
            res = response.read()
            data = json.loads(res)
            for i in data['tracks']['items']:

                track = {
                    'tid': i['id'],
                    'artist': i['artists'],
                    'album': i['album']['name'],
                    'album_id': i['album']['id'],
                    'explicit': i['explicit'],
                    'name': i['name'],
                    'popularity': i['popularity'],
                    'track_number': i['track_number'],
                    'uri': i['uri']
                }
                tid = i["id"]

                url_info = 'curl https://api.spotify.com/v1/audio-features/%s -H "Authorization: Bearer %s"' % (tid, token)
                info = os.popen(url_info).read()
                tdata = json.loads(info)
                del tdata['id']
                del tdata['uri']
                del tdata['track_href']
                del tdata['analysis_url']
                track['audio_features'] = tdata

#                db = get_db_connection()
                r = db.Lastfm_Profiles.tracks_info.find_one({'tid': tid})
                if r is None:
                    db.Lastfm_Profiles.tracks_info.insert(track)
 #               db.close()
    except:
        print "token expired"
        dba.close()
        db.close()
        index()


def get_db_connection():
    client = MongoClient('localhost', 27017)
    client.borders.authenticate('admin', 'crepuscolo84', 'admin')
    uri = "mongodb://%s:%s@%s/?authSource=%s" % ('admin', 'crepuscolo84', 'localhost', 'admin')
    return MongoClient(uri)


run(host='localhost', port=8080)