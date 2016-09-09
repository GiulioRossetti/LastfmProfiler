from lastfm import api
from pymongo import MongoClient

__author__ = 'rossetti'
__license__ = "GPL"
__email__ = "giulio.rossetti@gmail.com"


class UpdateNet(object):

    def __init__(self, lastfm_api):
        """
        Constructor

        :param echonest_api:
        :param lastfm_api:
        :return:
        """
        self.lastfm_api = api.Api(lastfm_api)

    def __get_network(self, username):
        user = self.lastfm_api.get_user(username)
        ff = user.get_friends()
        return [f.name for f in ff]

    def update(self, username):
        db = MongoClient('localhost', 27017)
        us = db.lastfm_profiling.profile.find_one({'user_id': username})

        if us is not None and us['nfriends'] == 50:
            friends = self.__get_network(username)
            print len(friends)
            db = MongoClient('localhost', 27017)
            db.lastfm_profiling.profile.update_one({'user_id': username},
                                                              {
                                                                  '$set': {
                                                                      'nfriends': len(friends)
                                                                  }
                                                              })
            db.lastfm_profiling.network_stars.update_one({'user_id': username},
                                                   {
                                                       '$set': {
                                                           'neighbors': friends
                                                       }
                                                   }
                                                   )
        db.close()


if __name__ == "__main__":
    lastfm_api = '7f5b87ada5f5e2bbea281ce00757cad0'
    echonest_api = "CPNNPRGPTHKVC1XSW"
    pb = UpdateNet(lastfm_api)
    f = open("users_list")
    count = 1
    for l in f:
        l = l.rstrip()
        try:
            pb.update(l)
        except:
            print "error"
            pass
        print "Updated: %s (%d)" % (l, count)
        count += 1
