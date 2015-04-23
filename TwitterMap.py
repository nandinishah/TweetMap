'''
Created on Feb 27, 2015

@author: nandinishah
'''
import tweepy
import json
import mysql.connector
#import re
import datetime

# Authentication details. To  obtain these visit dev.twitter.com
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

# This is the listener, responsible for receiving data
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)
        f = open('workfile','a')
        #location = re.escape(decoded['user']['location'])
        try:
            coordinates = decoded['coordinates']
            # Also, we convert UTF-8 to ASCII ignoring all bad characters sent by users
            print ''
            try:
                if coordinates:
                    print "Geo location obtained!"
                    tweetid = decoded['id_str']
                    print tweetid
                    screenName = decoded['user']['screen_name']
                    message = decoded['text'].encode('ascii', 'ignore')
                    print '@%s: %s' % (screenName, message)
                    print 'coordinates: %s' % (coordinates) 
                    longitude = coordinates['coordinates'][0]
                    latitude = coordinates['coordinates'][1]
                    print 'latitude: %s, longitude: %s' % (latitude,longitude)
                    timing = str(datetime.datetime.now())
                    print timing
                    
                    #obtaining key word
                    if "fashion" in message:
                        keyword = 'fashion'
                    if "food" in message:
                        keyword = 'food'
                    if "USA" in message:
                        keyword = 'USA'
                    if "cricket" in message:
                        keyword = 'cricket'
                    if "big data" in message:
                        keyword = 'bigdata'
                    if "mathematics" in message:
                        keyword = 'mathematics'
                    print 'keyword: %s' % keyword    
                    
                    # WRITING TO A FILE FOR BACKUP
                    f.write(data)
                    f.write("\n")
                    f.write(keyword)
                    f.write("\n")
                    f.write(screenName)
                    f.write(message)
                    f.write("\n")
                    f.write(str(latitude))
                    f.write("\t")
                    f.write(str(longitude))
                    f.write("\n")
                    f.close()
                    
                    # WRITING TO THE DATABASE
                    #print 'boo1'
                    cursor = cnx.cursor()
                    #print 'boo2'
                    add_TweetsDB = ("INSERT INTO tweetmap_tweetsdbc "
                   "(TweetID, KeyWord, ScreenName, Message, Latitude, Longitude, timestamp) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                    #print 'boo3'
                    data_TweetsDB = (tweetid, keyword, screenName, message, latitude, longitude, timing)
                    #print 'boo4'
                    cursor.execute(add_TweetsDB,data_TweetsDB)
                    #print 'boo5'
                    cnx.commit()
                    print 'saved'
                    #print 'boo6'
                    
            except:
                print "No geolocation"
                f.close()  
                return True
        except:
            print "No coordinates fields"
            f.close()  
            return True
        #cnx.close()
        f.close()
        return True

    def on_error(self, status):
        print status
        cnx.close()
        return True
    
if __name__ == '__main__':
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    print "Showing all new tweets for #fashion #food #USA #cricket #bigdata #mathematics:"
    cnx = mysql.connector.connect(user='nss2158', password='',host='comse6998.c5nefszkvtc0.us-west-2.rds.amazonaws.com', database='comse6998')
    stream = tweepy.Stream(auth, l)
    stream.filter(track=['fashion','food','USA','cricket','big data', 'mathematics'])