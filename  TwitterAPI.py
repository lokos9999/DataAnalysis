
# coding: utf-8

# In[ ]:

import twitter
import urlparse # python 2.7
import logging
import time
import sys
from datetime import datetime
import os
import io
import json
from pprint import pprint as pp
import csv
from collections import namedtuple
import time

class TwitterAPI(object):
    """
    TwitterAPI class allows the Connection to Twitter via OAuth
    once you have registered with Twitter and receive the 
    necessary credentials 
    """
    def __init__(self): 
        consumer_key = 'U0MiUHjZWvtCUmd53OduJpqLO'
        consumer_secret = 'q4YtC79OCrO7hiV08dpasL9o04xMMAfm8f35f91iCvDUdPnRZK'
        access_token = '3594247278-rwwUg3H7Uv68KInZ3m42JbeAVmLgtgwTXodeIHe'
        access_secret = 'oJA4Jwwxc5IPQwPVQ74zOjMYcXV7Zz16XyAdMas5VAeYu'
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        self.retries = 3
        self.auth = twitter.oauth.OAuth(access_token, access_secret, consumer_key, consumer_secret)
        self.api = twitter.Twitter(auth=self.auth)
        
        # logger initialisation
        appName = 'whotest9999'
        self.logger = logging.getLogger(appName)
        #self.logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        logPath = '/home/ubuntu/anaconda2'
        fileName = appName
        fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fileHandler.setFormatter(formatter)
        self.logger.addHandler(fileHandler) 
        self.logger.setLevel(logging.DEBUG)
        
        # Save to JSON file initialisation
        jsonFpath = '/home/ubuntu/anaconda2'
        jsonFname = 'M10307916'
        self.jsonSaver = IO_json(jsonFpath, jsonFname)
        
        # Save to CSV file initialisation
        csvFpath = '/home/ubuntu/anaconda2'
        csvFname = 'M10307916'
        self.csvSaver = IO_csv(csvFpath, csvFname)
      
    def searchTwitter(self, q, max_res=10,**kwargs):
        search_results = self.api.search.tweets(q=q, count=10, **kwargs)
        statuses = search_results['statuses']
        #screen_name = statuses['screen_name']
        #screen_names = self.api.statuses.user_timeline(screen_name=screen_name)
        # max_results = min(1000, max_res) 
        max_results =10000 
        for _ in range(5000):
            try:
                next_results = search_results['search_metadata']['next_results']
                # self.logger.info('info in searchTwitter - next_results:%s'% next_results[1:])
            except KeyError as e:
            	#self.logger.error('error in searchTwitter: %s', %(e))
                break
            
            next_results = urlparse.parse_qsl(next_results[1:]) # python 2.7
            #next_results = urllib.parse.parse_qsl(next_results[1:])
            # self.logger.info('info in searchTwitter - next_results[max_id]:', next_results[0:])
            kwargs = dict(next_results)
            # self.logger.info('info in searchTwitter - next_results[max_id]:%s'% kwargs['max_id'])
            search_results = self.api.search.tweets(**kwargs)
            statuses += search_results['statuses']
            self.saveTweets(search_results['statuses'])
            
            if len(statuses) > max_results:
                self.logger.info('info in searchTwitter - got %i tweets - max: %i' %(len(statuses), max_results))
                break
        return statuses

    def saveTweets(self, statuses):
        # Saving to JSON File
        self.jsonSaver.save(statuses)
        #self.csvSaver.save(statuses)
      
    def parseTweets(self, statuses):
        return [ (status['id'], 
                  status['created_at'], 
                  status['user']['id'],
                  status['user']['name'], 
                  status['text'], 
                  url['expanded_url']) 
                        for status in statuses 
                            for url in status['entities']['urls'] ]

    def getTweets(self, q,  max_res=10):
        """
        Make a Twitter API call whilst managing rate limit and errors.
        """
        def handleError(e, wait_period=2, sleep_when_rate_limited=True):

            if wait_period > 3600: # Seconds
                #self.logger.error('Too many retries in getTweets: %s', %(e))
                self.logger.info('3600')
                raise e
            if e.e.code == 401:
                #self.logger.error('error 401 * Not Authorised * in getTweets: %s', %(e))
                return None
            elif e.e.code == 404:
                #self.logger.error('error 404 * Not Found * in getTweets: %s', %(e))
                return None
            elif e.e.code == 429: 
                #self.logger.error('error 429 * API Rate Limit Exceeded * in getTweets: %s', %(e))
                self.logger.info('429')
                if sleep_when_rate_limited:
                    #self.logger.error('error 429 * Retrying in 15 minutes * in getTweets: %s', %(e))
                    sys.stderr.flush()
                    time.sleep(60*15 + 5)
                    #self.logger.info('error 429 * Retrying now * in getTweets: %s', %(e))
                    return 2
                    #return None
                else:
                    raise e # Caller must handle the rate limiting issue
            elif e.e.code in (500, 502, 503, 504):
                self.logger.info('Encountered %i Error. Retrying in %i seconds' % (e.e.code, wait_period))
                time.sleep(wait_period)
                wait_period *= 1.5
                return wait_period
            else:
                #self.logger.error('Exit - aborting - %s', %(e))
                raise e
        
        while True:
            try:
                self.searchTwitter( q, max_res=10)
            except twitter.api.TwitterHTTPError as e:
                self.logger.info('Exception')
                error_count = 0
                wait_period  = 2
                wait_period = handleError(e, wait_period)               
                if wait_period is None:
                    return

class IO_csv(object):
    def __init__(self, filepath, filename, filesuffix='csv'):
        self.filepath = filepath       # /path/to/file  without the '/' at the end
        self.filename = filename       # FILE_NAME
        self.filesuffix = filesuffix
        # self.file_io = os.path.join(dir_name, '.'.join((base_filename, filename_suffix)))

    def save(self, data, NTname, fields):
        # NTname = Name of the NamedTuple
        # fields = header of CSV - list of the fields name
        NTuple = namedtuple(NTname, fields)
        
        if os.path.isfile('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix)):
            # Append existing file
            with open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'ab') as f:
                writer = csv.writer(f)
                # writer.writerow(fields) # fields = header of CSV
                writer.writerows([row for row in map(NTuple._make, data)])
                # list comprehension using map on the NamedTuple._make() iterable and the data file to be saved
                # Notice writer.writerows and not writer.writerow (i.e. list of multiple rows sent to csv file
        else:
            # Create new file
            with open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'wb') as f:
                writer = csv.writer(f)
                writer.writerow(fields) # fields = header of CSV - list of the fields name
                writer.writerows([row for row in map(NTuple._make, data)])
                #  list comprehension using map on the NamedTuple._make() iterable and the data file to be saved
                # Notice writer.writerows and not writer.writerow (i.e. list of multiple rows sent to csv file
            
    def load(self, NTname, fields):
        # NTname = Name of the NamedTuple
        # fields = header of CSV - list of the fields name
        NTuple = namedtuple(NTname, fields)
        with open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix),'rU') as f:
            reader = csv.reader(f)
            for row in map(NTuple._make, reader):
                # Using map on the NamedTuple._make() iterable and the reader file to be loaded
                yield row


class IO_json(object):
    def __init__(self, filepath, filename, filesuffix='json'):
        self.filepath = filepath        # /path/to/file  without the '/' at the end
        self.filename = filename        # FILE_NAME
        self.filesuffix = filesuffix
        # self.file_io = os.path.join(dir_name, '.'.join((base_filename, filename_suffix)))

    def save(self, data):
        if os.path.isfile('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix)):
            # Append existing file
            with io.open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'a', encoding='utf-8') as f:
                f.write(unicode(json.dumps(data, ensure_ascii= False))) # In python 3, there is no "unicode" function 
                # f.write(json.dumps(data, ensure_ascii= False)) # create a \" escape char for " in the saved file        
        else:
            # Create new file
            with io.open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), 'w', encoding='utf-8') as f:
                f.write(unicode(json.dumps(data, ensure_ascii= False)))
                # f.write(json.dumps(data, ensure_ascii= False))    

    def load(self):
        with io.open('{0}/{1}.{2}'.format(self.filepath, self.filename, self.filesuffix), encoding='utf-8') as f:
            return f.read()

class IO_mongo(object):
    conn={'host':'localhost','ip':'27017'}

    def __init__(self, db='twtr_db', coll='twtr_coll', **conn):
        self.client=MCLi(**conn)
        self.db=self.client[db]
        self.coll=self.db[coll]

    def save(self,data):
        return self.coll.insert(data)

    def load(self, return_cursor=False,criteria=None,projection=None):
        if criteria is None:
            criteria={}

        if Projection is None:
            cursor=self.coll.find(criteria)
        else:
            cursor=self.coll.find(criteria,projection)

        if return_cursor:
            return cursor
        else:
            return [iterm for item in cursor]
        
t=TwitterAPI()
q="nba"
t.getTweets( q, max_res=10)

