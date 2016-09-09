import urllib2
import socket
import json
from pymongo import MongoClient
from BeautifulSoup import BeautifulSoup

__author__ = 'rossetti'
__license__ = "GPL"
__email__ = "giulio.rossetti@gmail.com"


# https://genius.com/api-clients
# client id: fUdz2EeQ6P7VqhVP8_9qRBA2KSeZwpnhmJqmrRNoGBNd4erbsu2L601oJ17JhPC0
# secret: u7FWbGaClyefpv8Wd4FQwKKrzmlJt2zfKnWisj7VXdt6Ty7-64OBjhuOQjIY9-bQG7TEz6WfEbeHfxjOPy75KQ


class Genius_Lyrics(object):
    def __init__(self, token, db_conf=None):

        self.access_token = token
        self.db_conf = db_conf
        if db_conf is None:
            self.db = MongoClient("localhost", 27017)
            self.db.admin.authenticate("admin", "crepuscolo84", "admin")
        else:
            self.db = MongoClient(self.db_conf['host'], self.db_conf['port'])
            self.db.admin.authenticate(self.db_conf['user'], self.db_conf['pwd'], self.db['auth'])
#        self.db.Track_Lyric.createIndex({"spotify_id": 1})

    # noinspection PyBroadException
    def __search_song(self, song, artist, max_page=50):

        page = 1
        while True:
            try:
                querystring = "http://api.genius.com/search?q=" + urllib2.quote(song) + "&page=" + str(page)
                request = urllib2.Request(querystring.decode("utf8").encode("utf8"))
                request.add_header("Authorization", "Bearer " + self.access_token)
                request.add_header("User-Agent",
                                   "curl/7.9.8 (i686-pc-linux-gnu) libcurl 7.9.8 (OpenSSL 0.9.6b) (ipv6 enabled)")

                raw = ""
                while True:
                    try:
                        response = urllib2.urlopen(request, timeout=4)
                        raw = response.read()
                    except socket.timeout:
                        print("Timeout raised and caught")
                        continue
                    break

                json_obj = json.loads(raw)['response']['hits']
                for h in json_obj:
                    t = h['result']['title']
                    a = h['result']['primary_artist']['name']
                    if t.lower() == song.lower() and a.lower() == artist.lower():
                        return h

                if page >= max_page:
                    return None
                page += 1
            except Exception:
                return None

    def crawl_lyric_endpoint(self, max_page_to_crawl=10):

        tot = self.db.Lastfm_Profiles.tracks_info.count({'crawled_lyrics': {'$exists': False}})
        count, identified = 0, 0
        while True:
            a_tot = self.db.Lastfm_Profiles.tracks_info.count({'crawled_lyrics': {'$exists': False}})
            if a_tot == 0:
                return
            res = self.db.Lastfm_Profiles.tracks_info.find({'crawled_lyrics': {'$exists': False}}).limit(10)
            #crs = []
            for r in res:
                artist = r['artist'][0]['name']
                song = r['name'].split("-")[0]
                res_s = self.__search_song(song, artist, max_page_to_crawl)
            #    crs.append(r['_id'])
                if res_s is not None:
                    info = {
                        'artist': artist,
                        'song': song,
                        'url': res_s['result']['url'],
                        'spotify_id': r['tid']
                    }
                    ie = self.db.Lastfm_Profiles.Track_Lyric.find_one({"spotify_id": r['tid']})
                    if ie is None:
                        self.db.Lastfm_Profiles.Track_Lyric.insert(info)
                        self.db.Lastfm_Profiles.tracks_info.update_one({'_id': r['_id']}, {"$set": {"identified": True}})
                        identified += 1
                    else:
                        self.db.Lastfm_Profiles.tracks_info.update_one({'_id': r['_id']}, {"$set": {"crawled_lyrics": True}})
                else:
                    self.db.Lastfm_Profiles.tracks_info.update_one({'_id': r['_id']}, {"$set": {"crawled_lyrics": True}})
                count += 1
            #for c in crs:
            #    self.db.MusicaItalia.Track.update_one({'_id': c}, {"$set": {"crawled_lyrics": True}})

                print "%s/%s (Identified: %s)" % (count, tot, identified)

    @staticmethod
    def __strip_tags(html):

        html = str(html).replace("<br />", "").replace("<p>", "").replace("</p>", "")
        return html

    def crawl_lyric(self):

        while True:
            lyric_list = self.db.Lastfm_Profiles.Track_Lyric.find({'lyric': {'$exists': False}}).limit(10)

            if lyric_list is None:
                return

            for l in lyric_list:
                text = self.__get_lyric(l['url'])
                self.db.Lastfm_Profiles.Track_Lyric.update_one({'_id': l['_id']}, {"$set": {"lyric": text}})

    def __get_lyric(self, url):

        request = urllib2.Request(url)
        request.add_header("Authorization", "Bearer " + self.access_token)
        request.add_header("User-Agent",
                           "curl/7.9.8 (i686-pc-linux-gnu) libcurl 7.9.8 (OpenSSL 0.9.6b) (ipv6 enabled)")
        raw = ""
        while True:
            try:
                response = urllib2.urlopen(request, timeout=4)
                raw = response.read()
            except socket.timeout:
                print("Timeout raised and caught")
                continue
            break

        soup = BeautifulSoup(raw)
        divs = soup.findAll("p")
        div = divs[0]
        text = self.__strip_tags(div)
        return text


if __name__ == "__main__":
    access_token = 'mf4yxk8wm7DcRbxKisJgmwy_PpcFHsJI_O1TyVJGO3jqW8O4nwaopvs3bbseyvzV'
    gl = Genius_Lyrics(access_token)
    #gl.crawl_lyric_endpoint(2)
    gl.crawl_lyric()
