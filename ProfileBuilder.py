import time
import operator
import os
import numpy as np
import math
import timeout_decorator
import datetime
import json
from pymining import seqmining
from bson.json_util import dumps
from multiprocessing import Pool
import DataGathering as d

__author__ = 'rossetti'
__license__ = "GPL"
__email__ = "giulio.rossetti@gmail.com"


def timeit(method):
    """
    Decorator: Compute the execution time of a function
    :param method: the function
    :return: the method runtime
    """

    def timed(*arguments, **kw):
        ts = time.time()
        result = method(*arguments, **kw)
        te = time.time()

        print 'Time:  %r %2.2f sec' % (method.__name__.strip("_"), te - ts)
        return result

    return timed


class ProfileBuilder(object):
    def __init__(self, lastfm_api, echonest_api, db_config):
        self.profile = {}
        self.artist_to_genre = {}
        self.artist_to_id = {}
        self.genre_to_id = {}
        self.db_config = db_config

        self.dg = d.DataGathering(lastfm_api, echonest_api, self.db_config)
        self.db = self.dg.get_db_connection()

        # db = self.dg.get_db_connection()
        # cursor = db.Lastfm_Profiles.genres.find()
        # for c in cursor:
        #    self.artist_to_genre[c['name'].encode('utf-8')] = c['genre'].encode('utf-8')
        # db.close()

    def close(self):
        self.db.close()

    def build_profile_series(self, username, start_date, end_date, days_per_snapshot=30):
        self.days_per_snapshot = days_per_snapshot
        # user's details
        details = self.dg.get_user_detail(username)
        #db = self.dg.get_db_connection()
        try:
            r = self.db.Lastfm_Profiles.user_info.find_one({'user_id': username})
            if r is None:
                self.db.Lastfm_Profiles.user_info.insert(details)
            elif not r['crawled']:
                self.db.Lastfm_Profiles.user_info.update_one({'user_id': username}, {"$set": {"crawled": True}})
        except:
            #db.close()
            pass
        #db.close()

        delta = datetime.timedelta(days=days_per_snapshot)
        profiles = 0
        actual_end = start_date + delta
        while actual_end < end_date:
            r = self.build_profile(username, nlistenings=0, start_date=start_date, end_date=actual_end)
            if r:
                profiles += 1
            start_date = actual_end + datetime.timedelta(seconds=1)
            actual_end = start_date + delta
        return "%s:%d" % (username, profiles)

    def build_profile(self, username, nlistenings, start_date=None, end_date=None):
        try:
            #db = self.dg.get_db_connection()

            self.profile = {}
            res = self.db.Lastfm_Profiles.profile.find({'user_id': username})
            for l in res:
                if l is not None and l['snapshot_end'] == end_date:
                    return l
            #db.close()
            self.profile['user_id'] = username

            # gather data
            # try:
            if start_date is not None and end_date is not None:
                user_listenings = self.dg.get_range_listening(username, start_date=start_date, end_date=end_date)
                self.profile['snapshot_end'] = end_date
                self.profile['snapshot_start'] = start_date
            else:
                user_listenings = self.dg.get_user_listening(username, hundreds_of_listenings=nlistenings)
                self.profile['most_recent_listening'] = datetime.datetime.fromtimestamp(
                    long(user_listenings[0][-1].strftime("%s")))
                self.profile['least_recent_listening'] = datetime.datetime.fromtimestamp(
                    long(user_listenings[-1][-1].strftime("%s")))

            # listenings stats
            # print "OK 1"
            self.__listenings_stats(user_listenings)
            self.__temporal_stats(user_listenings)

            # artist stats
            # print "OK 2"
            self.__artist_stats(user_listenings)

            # genre stats
            # print "OK 3"
            self.__genre_stats(user_listenings)

            # seq. patterns
            # print "OK 3.5"
            # @todo: rivedere
            # self.__seq_patterns_stats(user_listenings)

            # network
            # print "OK 4"
            #db = self.dg.get_db_connection()
            res = self.db.Lastfm_Profiles.network_stars.find_one({'user_id': username})
            if res is None:
                self.__get_network(username)
            #db.close()

            # composite indexes
            # print "OK 5"
            self.profile['theta_tl'] = 0 if self.profile['nlistening'] == 0 else float(self.profile['ntracks']) / \
                                                                                 self.profile['nlistening']
            self.profile['theta_al'] = 0 if self.profile['nlistening'] == 0 else float(self.profile['nartists']) / \
                                                                                 self.profile['nlistening']
            self.profile['theta_bl'] = 0 if self.profile['nlistening'] == 0 else float(self.profile['nalbum']) / \
                                                                                 self.profile['nlistening']
            self.profile['theta_gl'] = 0 if self.profile['nlistening'] == 0 else float(self.profile['ngenre']) / \
                                                                                 self.profile['nlistening']
            self.profile['theta_ga'] = 0 if self.profile['nartists'] == 0 else float(self.profile['ngenre']) / self.profile[
                'nartists']
            self.profile['theta_bt'] = 0 if self.profile['ntracks'] == 0 else float(self.profile['nalbum']) / self.profile[
                'ntracks']

            # print "OK 6"
            self.__save_profile()
            # except:
            #    return False
        except:
            pass

        # self.db.close()
        return True

    @staticmethod
    def __entropy(x, classes=None):
        val_entropy = 0
        n = np.sum(x)
        for freq in x:
            if freq == 0:
                continue
            p = 1.0 * freq / n
            val_entropy -= p * np.log2(p)

        if classes is not None and classes>1:
            val_entropy /= np.log2(classes)
        return val_entropy

    @staticmethod
    def __hour_to_timeslot(h):
        if 2 < h <= 8:  # early morning 6h
            return 0
        if 8 < h <= 12:  # late morning 4h
            return 1
        if 12 < h <= 15:  # early afternoon 3h
            return 2
        if 15 < h <= 18:  # late afternoon 3h
            return 3
        if 18 < h <= 22:  # early night 4h
            return 4
        if 22 < h or h <= 2:  # late night 4h
            return 5

    @staticmethod
    def __seq_pattern(freq_seqs):
        closed_freq_seq = dict()
        for item in sorted(freq_seqs, key=lambda x: len(x[0]), reverse=True):
            seq = item[0]
            sup = item[1]

            is_closed = True
            for closed_seq, closed_sup in closed_freq_seq.iteritems():
                if set(seq) <= set(closed_seq) and closed_sup >= sup:
                    is_closed = False
                    break

            if is_closed:
                closed_freq_seq[seq] = sup
        cls = [[x, y] for x, y in closed_freq_seq.iteritems() if len(x) > 1]

        return cls

    def __seq_patterns_stats(self, user_listenings):

        # print "Seq Patterns"
        # separate by day
        # genre_patterns, genre_pattern = [], []
        artist_patterns, artist_pattern = [], []

        # misup base
        total_listening = len(user_listenings)
        avg_daily_listening = float(total_listening) / self.days_per_snapshot
        min_sup = int(math.sqrt(avg_daily_listening))

        # @todo: modify genre handling (multigenre)
        last_day = -1
        for l in user_listenings:
            day = l[-1].day
            if day != last_day:
                last_day = day
                if len(artist_patterns) > 0:
                    #    genre_patterns.append(genre_pattern)
                    artist_patterns.append(artist_pattern)
                # if self.artist_to_genre[l[2]] is not None:
                #     genre_pattern = [self.artist_to_genre[l[2]]]
                artist_pattern = [l[2]]
            else:
                # if self.artist_to_genre[l[2]] is not None:
                #     genre = self.artist_to_genre[l[2]]
                #     if len(genre_pattern) > 0:
                #         genre_pattern.append(genre)
                if len(artist_patterns) > 0:
                    artist_pattern.append(l[2])
        if len(artist_patterns) > 0:
            # genre_patterns.append(genre_pattern)
            artist_patterns.append(artist_pattern)

        # done = False

        # min_size = max([3, min_sup])
        # while not done:
        #    try:
        #        self.__genre_freq_seq(genre_patterns, min_size)
        #        done = True
        #    except:
        #        min_size += min_sup

        done = False
        min_size = max([3, min_sup])
        # print "start artist patterns"
        while not done:
            try:
                self.__artist_freq_seq(artist_patterns, min_size)
                done = True
            except:
                min_size += min_sup
                # print "update", min_size

    # @timeout_decorator.timeout(15)
    # def __genre_freq_seq(self, genre_patterns, min_size):
    #    freq_genre = seqmining.freq_seq_enum(genre_patterns, min_size)
    #    self.profile['genre_pattern'] = self.__seq_pattern(freq_genre)

    @timeout_decorator.timeout(15)
    def __artist_freq_seq(self, artist_patterns, min_size):
        freq_artist = seqmining.freq_seq_enum(artist_patterns, min_size)
        self.profile['artist_pattern'] = self.__seq_pattern(freq_artist)

    def __get_network(self, username):
        friends = self.dg.get_network(username)
        self.profile['nfriends'] = len(friends)
        dt = datetime.datetime.now()
        #db = self.dg.get_db_connection()
        self.db.Lastfm_Profiles.network_stars.insert({'user_id': username, 'neighbors': friends,
                                                  'crawl_date': dt})
        #db.close()

    def __listenings_stats(self, listenings):
        self.profile['nlistening'] = len(listenings)

        artist_to_listenings = {}
        track_to_listenings = {}
        album_to_listenings = {}

        for l in listenings:

            # tracks
            track = l[0]
            if track in track_to_listenings:
                track_to_listenings[track] += 1
            else:
                track_to_listenings[track] = 1

            # artists
            art = l[2]
            if art in artist_to_listenings:
                artist_to_listenings[art] += 1
            else:
                artist_to_listenings[art] = 1

            # albums
            album = l[4]
            if album in album_to_listenings:
                album_to_listenings[album] += 1
            else:
                album_to_listenings[album] = 1

        # cleaning frequency vectors
        # artist_to_listenings = self.__knee(artist_to_listenings)
        # track_to_listenings = self.__knee(track_to_listenings)
        # album_to_listenings = self.__knee(album_to_listenings)

        self.profile['nartists'] = len(artist_to_listenings)
        self.profile['ntracks'] = len(track_to_listenings)
        self.profile['nalbum'] = len(album_to_listenings)

        self.profile['ml_artist'] = None if len(artist_to_listenings) == 0 else \
        max(artist_to_listenings.iteritems(), key=operator.itemgetter(1))[0]
        self.profile['ml_track'] = None if len(track_to_listenings) == 0 else \
        max(track_to_listenings.iteritems(), key=operator.itemgetter(1))[0]
        self.profile['ml_album'] = None if len(album_to_listenings) == 0 else \
        max(album_to_listenings.iteritems(), key=operator.itemgetter(1))[0]

        self.profile['entropy_artist'] = 0 if len(artist_to_listenings) == 1 else \
            self.__entropy(artist_to_listenings.values(), classes=len(artist_to_listenings))
        self.profile['entropy_track'] = 0 if len(track_to_listenings) == 1 else \
            self.__entropy(track_to_listenings.values(), classes=len(track_to_listenings))
        self.profile['entropy_album'] = 0 if len(album_to_listenings) == 1 else \
            self.__entropy(album_to_listenings.values(), classes=len(album_to_listenings))

        self.profile['freq_artist'] = artist_to_listenings
        self.profile['freq_track'] = track_to_listenings
        self.profile['freq_album'] = album_to_listenings

    def __artist_stats(self, listening):
        for l in listening:
            art = l[2]
            #db = self.dg.get_db_connection()
            if self.db.Lastfm_Profiles.artists.find_one({'name': art}) is None:
                info = self.dg.get_artist_info(art)
                if len(info) > 0:
                    self.db.Lastfm_Profiles.artists.insert(info)
            #db.close()

    def __genre_stats(self, listenings):
        aid = 0
        genre_to_count = {}
        tested_artists = {}
        for l in listenings:
            art = l[2]
            if art not in tested_artists:
                self.artist_to_id[art] = aid
                aid += 1

                tested_artists[art] = None
                # genre = [] # None
                if art not in self.artist_to_genre:
                    genres = self.dg.get_genre(art)

                    # echonest version
                    # if genres is not None and len(genres) > 0:
                    #    dt = genres[0]  # the most reliable genre assigned to the given artist
                    #    g = dt[0]
                    #    genre.append(g)
                    #    self.artist_to_genre[art] = genre
                    # save genre
                    #    db = self.dg.get_db_connection()
                    #    db.Lastfm_Profiles.genres.insert({'name': art, 'genre': genre})
                    #    db.close()
                    # else:
                    #    self.artist_to_genre[art] = None

                    # spotify version
                    if genres != {} and len(genres['genres']) > 0:
                        #db = self.dg.get_db_connection()
                        self.db.Lastfm_Profiles.artists.update_one({'name': art}, {"$set": {"spotify_genres": genres}})
                        self.artist_to_genre[art] = genres['genres']
                        # db.Lastfm_Profiles.genres.insert({'name': art, 'genre': genre})
                        #db.close()
                    else:
                        self.artist_to_genre[art] = []

                genre = self.artist_to_genre[art]

                # @todo: aggiornare il calcolo della frequenza dei generi per spotify

                if len(genre) > 0:  # if genre is not None:
                    for g in genre:
                        if g in genre_to_count:
                            genre_to_count[g] += 1
                        else:
                            genre_to_count[g] = 1

        # compute stats
        self.profile['ngenre'] = len(genre_to_count)
        self.profile['freq_genre'] = genre_to_count
        self.profile['ml_genere'] = None if len(genre_to_count) == 0 else \
        max(genre_to_count.iteritems(), key=operator.itemgetter(1))[0]
        self.profile['entropy_genre'] = 0 if len(genre_to_count) <= 1 else \
            self.__entropy(genre_to_count.values(), classes=len(genre_to_count))
        # print "CAAA"

    def __temporal_stats(self, listenings):

        day_to_count = {'0': 0, '1': 0, '2': 0, '3': 0, '4': 0, '5': 0, '6': 0}
        time_slot_to_count = {'0': 0, '1': 0, '2': 0, '3': 0, '4': 0, '5': 0}
        time_listenings = [0] * 42
        for l in listenings:
            dt = l[-1]
            day = dt.weekday()
            day_to_count[str(day)] += 1
            time_slot = self.__hour_to_timeslot(dt.hour)
            time_slot_to_count[str(time_slot)] += 1
            slot = time_slot + (day * 6)
            time_listenings[slot] += 1

        self.profile['freq_tod'] = time_slot_to_count
        self.profile['freq_dow'] = day_to_count
        self.profile['entropy_dow'] = self.__entropy(day_to_count.values(), len(day_to_count))
        self.profile['entropy_tod'] = self.__entropy(time_slot_to_count.values(), len(time_slot_to_count))
        self.profile["time_listening"] = time_listenings

    def dump_profile_on_file(self, filename):
        res = dumps(self.profile)
        f = open(filename, "w")
        f.write(res.encode('utf-8'))
        f.flush()
        f.close()

    def __save_profile(self):
        if self.profile['nlistening'] == 0:
            return
        #db = self.dg.get_db_connection()
        try:
            self.db.Lastfm_Profiles.profile.insert(self.profile)
        except:
            d = os.path.dirname("profile_error_dumps")
            if not os.path.exists(d):
                os.makedirs(d)
            self.dump_profile_on_file("profile_error_dumps/%s.json" % self.profile['user_id'])
        #db.close()

    @staticmethod
    def __closest_point_on_segment(a, b, p):
        sx1 = a[0]
        sx2 = b[0]
        sy1 = a[1]
        sy2 = b[1]
        px = p[0]
        py = p[1]

        x_delta = sx2 - sx1
        y_delta = sy2 - sy1

        if x_delta == 0 and y_delta == 0:
            return p

        u = ((px - sx1) * x_delta + (py - sy1) * y_delta) / (x_delta * x_delta + y_delta * y_delta)
        if u < 0:
            closest_point = a
        elif u > 1:
            closest_point = b
        else:
            cp_x = sx1 + u * x_delta
            cp_y = sy1 + u * y_delta
            closest_point = [cp_x, cp_y]

        return closest_point

    def __get_change_point(self, x, y):

        max_d = -float('infinity')
        index = 0

        for i in range(0, len(x)):
            c = self.__closest_point_on_segment(a=[x[0], y[0]], b=[x[len(x) - 1], y[len(y) - 1]], p=[x[i], y[i]])
            d = math.sqrt((c[0] - x[i]) ** 2 + (c[1] - y[i]) ** 2)
            if d > max_d:
                max_d = d
                index = i

        if len(y) >= 2:
            return min(index + 1, len(x) - 1), y[min(index + 1, len(x) - 1) - 1]
        else:
            return min(index + 1, len(x) - 1), y[min(index + 1, len(x) - 1)]

    def __knee(self, frequencies):
        if len(frequencies.keys()) == 0:
            return frequencies
        a = frequencies.values()
        a = sorted(a)

        index, v_prev = self.__get_change_point(range(0, len(a)), a)
        new_freq = dict()
        for k, v in frequencies.iteritems():
            if v >= a[index]:
                new_freq[k] = frequencies[k]

        if len(new_freq) == 1 and len(a) >= 2:
            new_freq = dict()
            for k, v in frequencies.iteritems():
                if v >= v_prev:
                    new_freq[k] = frequencies[k]

        return new_freq


def read_next_pool(f_open, size, db):
    names = []
    users = db.Lastfm_Profiles.user_info.find({"crawled": False}, {'user_id': 1}).limit(size)
    for l in users:
        if "user_id" in l:
            names.append(l['user_id'])
    return names


def gater_data(infos):
    inputs = []
    try:
        for i in infos:
            inputs.append(i)
        pb = ProfileBuilder(inputs[-3], inputs[-2], inputs[-1])
        result = pb.build_profile_series(inputs[0], inputs[1], inputs[2], inputs[3])
        pb.close()
    except:
        print "Errore"

    #inputs[-1].close()
    print "Terminated. Connection closed"

    return result


def read_config():
    cfg = json.loads(open("config/cfg.json").read())
    start_date = datetime.datetime.strptime(cfg['start_date'], "%Y-%m-%d")
    end_date = datetime.datetime.strptime(cfg['end_date'], "%Y-%m-%d")
    lastfm_apis = cfg['lastfm']
    echonest_apis = cfg['echonest']
    snapshot_days = cfg['snapshot_days']
    user_list = cfg['user_list']
    mongo_conf = cfg['mongodb']
    return lastfm_apis, echonest_apis, start_date, end_date, snapshot_days, user_list, mongo_conf


if __name__ == "__main__":
    import itertools

    settings = read_config()
    start_date = settings[2]
    end_date = settings[3]
    time_window = settings[4]
    db_config = settings[6]
    dg = d.DataGathering('', '', db_config)
    db = dg.get_db_connection()
    f = open(settings[5])
    names = read_next_pool(f, len(settings[0]), db)
    while len(names) > 0:
        try:
            pool = Pool(len(names))
            print "Starting: %s" % names
            # pb = ProfileBuilder("b0dd3e3357ccba7d85b0bfa1f7390142", "", db_config)
            # result = pb.build_profile_series("diegopennac", start_date, end_date, 30)
            # exit()

            res = pool.map(gater_data, itertools.izip(names, itertools.repeat(start_date),
                                                  itertools.repeat(end_date), itertools.repeat(time_window),
                                                  settings[0], itertools.repeat(settings[1][0]),
                                                  itertools.repeat(db_config)))
            pool.close()
            pool.join()
            print "Completed: %s" % res
            names = read_next_pool(f, len(settings[0]), db)
        except: # OSError as e
            #print e
            names = read_next_pool(f, len(settings[0]), db)
