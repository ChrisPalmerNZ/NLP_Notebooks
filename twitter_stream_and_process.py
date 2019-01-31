# -*- coding: utf-8 -*-
# To run this code, first edit config.py with your configuration
# 
# consumer_key = 'your consumer key'
# consumer_secret = 'your consumer secret'
# access_token = 'your access token'
# access_secret = 'your access secret'
# 
# then:
#
# mkdir data
#
# minimal example
# python twitter_stream_and_process.py -q apple -d data
# It will produce a list of tweets key fields in CSV form for the query "apple"
# in the file data/stream_apple.csv
#
# full example:
# python twitter_stream_and_process.py -q "vaccination,vaccinations,vaccine,vaccines,vax,vaxx,vaxine,vaccinated,vacinated,flushot,flu shot" -d data -f vaccine_flu -s Y -t Y
#
# wrapped:
# python twitter_stream_and_process.py 
#       -q "vaccination,vaccinations,vaccine,vaccines,vax,vaxx,vaxine,vaccinated,vacinated,flushot,flu shot"
#       -d data 
#       -f vaccine_flu 
#       -s Y 
#       -t Y
#
# The comma delimited list inserts a logical OR between each word, 
# the two words "flu shot" looks for flu AND shot
#
# Since it uses override file name (-f parameter) and specifies a suffix (-s), 
# it will produce a file for the query like this (for 8th Feb 2018):
# data/stream_vaccine_twitter_20180208.csv
# as the date changes to the next day on the user's PC, it will begin writing to 
# data/stream_vaccine_twitter_20180209.csv   
# the -t parameter means it will only report on tweets (not retweets or mentions)
#
# if tweepy not installed:
# conda install -c conda-forge tweepy
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
#from unidecode import unidecode
import argparse
import string
import config
import json
import csv
import re
import os.path

# enable logging
# import logging
# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
#                     datefmt="%Y-%m-%d %H:%M:%S")
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)


def get_parser():
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(description="Twitter Downloader")
    parser.add_argument("-q",
                        "--query",
                        dest="query",
                        help="Query/Filter",
                        default='-')
    parser.add_argument("-d",
                        "--data-dir",
                        dest="data_dir",
                        help="Output/Data Directory")
    parser.add_argument("-f",
                        "--file-name",
                        dest="file_name",
                        help="Override default filename")
    parser.add_argument("-s",
                        "--date-suffix",
                        dest="date_suffix",
                        help="(Y or 1 for Yes. Add date suffix to file name which changes daily")
    parser.add_argument("-t",
                        "--tweets-only",
                        dest="tweets_only",
                        help="(Y or 1 for Yes. Only process tweets (not retweets or mentions)")
    return parser


class MyListener(StreamListener):
    """Custom StreamListener for streaming data."""

    def __init__(self, data_dir, query, userfname, datesuffix, tweets_only):
        self.datacount = 0
        self.data_dir = data_dir
        self.query = query
        self.userfname = userfname
        self.datesuffix = datesuffix
        self.tweets_only = tweets_only
        
        query_fname = format_filename(self.query, self.userfname, self.datesuffix)
        self.outfile = "%s/stream_%s.csv" % (self.data_dir, query_fname)

        #self.text_list = []
        #self.text_dict = {}
        self.text_set = set()

        # set dictionary of already fetched data if file exists
        #"""
        if os.path.isfile(self.outfile):
            of = open(self.outfile, "r")
            olines = csv.DictReader(of, delimiter=',', quotechar='"',
                                    fieldnames = ['id','userid','screen_name','location',
                                                  'utc_offset','time_zone','CREATED_AT',
                                                  'TWEET_TYPE','TEXTXURLS','URLS','TEXT','place']
                                    )

            for row in olines:
                #self.text_list.append(row['TEXTXURLS'])
                #self.text_dict.update({row['TEXTXURLS']:row['id']})
                self.text_set.add(row['TEXTXURLS'])
                
            of.close()
        #"""
        
    def on_data(self, data):

        # Create a new file if date has changed
        if self.datesuffix is not None and self.datesuffix != time.strftime("%Y%m%d"):
            self.datesuffix = time.strftime("%Y%m%d")
            query_fname = format_filename(self.query, self.userfname, self.datesuffix)
            self.outfile = "%s/stream_%s.csv" % (self.data_dir, query_fname)
            #self.text_list = []
            #self.text_dict.clear()
            self.text_set.clear()

        try:
            tweet = parse_tweet(data, self.tweets_only)
            
            if tweet != None:
                content = extract_content(tweet)
                
                # just insert the new text into the set - if it doesn't yet exist
                # then the set will grow, and therefore add the tweet to our output
                print(".....")
                xlen = len(self.text_set)
                self.text_set.add(tweet['TEXTXURLS'])
                ylen = len(self.text_set)
                
                if len(tweet['TEXTXURLS']) > 10 and ylen > xlen:
                    #and tweet['TEXTXURLS'] not in self.text_dict #self.text_list
                    
                    #self.text_list.append(tweet['TEXTXURLS'])
                    #self.text_dict.update({tweet['TEXTXURLS']:tweet['id']})
                    
                    with open(self.outfile, 'a', newline='') as f:
                        writer = csv.writer(f, quotechar = '"')
                        writer.writerow(content)
                        print(content)
                        return True
    
        except BaseException as e:
#             logger.warning(e)
            print("Error on_data: %s" % str(e))
            time.sleep(5)
        return True

    def on_error(self, status):
        print(status)
        # See https://developer.twitter.com/en/docs/basics/response-codes
        disconnect_errors = (420, 429)
        if status in disconnect_errors:
            #returning False in on_data disconnects the stream
            return False
        else:
            return True


def parse_tweet(data, tweets_only):
    # load JSON item into a dict
    tweet = json.loads(data)

    # check if tweet is valid
    if 'user' in tweet.keys():

        # classify tweet type based on metadata
        if 'retweeted_status' in tweet:
            tweet['TWEET_TYPE'] = 'retweet'
        elif len(tweet['entities']['user_mentions']) > 0:
            tweet['TWEET_TYPE'] = 'mention'
        else:
            tweet['TWEET_TYPE'] = 'tweet'
        
        # Remove any new line characters in the location
        if tweet['user']['location'] is not None:
            tweet['user']['location'] = tweet['user']['location'].replace("\n", " ").rstrip()

        if (tweets_only != True or tweet['TWEET_TYPE'] == 'tweet'): 
            # parse date
            tweet['CREATED_AT'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))

            tweet['FULLTEXT'] = tweet['text']

            if 'extended_tweet' in tweet:
                if 'full_text' in tweet['extended_tweet']:
                    tweet['FULLTEXT'] = tweet['extended_tweet']['full_text']

#            # Remove unsuitable characters for model training - i.e nothing understood as ascii (prev used 'unicode_escape') 
#            tweet['TEXT'] = tweet['FULLTEXT'].encode('unicode_escape').decode('utf-8').rstrip().lstrip()

# Using this if .encode('unicode_escape') has been used above
#            tweet['TEXT'] = (tweet['TEXT'].replace("\\n", ". ")
#                            .replace("\\u2018", "'")
#                            .replace("\\u2019", "'")
#                            .replace("\\u201C", '"')
#                            .replace("\\u201c", '"')
#                            .replace("\\u201D", '"')
#                            .replace("\\u201d", '"')
#                            .replace("\\u2026", "...")
#                            .replace("&amp;", "&")
#                            .replace("&lt;", "<")
#                            .replace("&gt;", ">")
#                            )
                    
            # Replace all values needed to be carried into ascii text regardless of encoding strategies
            tweet['TEXT'] = (tweet['FULLTEXT'].replace("\n", " ")
                            .replace("\u2018", "'")
                            .replace("\u2019", "'")
                            .replace("\u201C", '"')
                            .replace("\u201c", '"')
                            .replace("\u201D", '"')
                            .replace("\u201d", '"')
                            .replace("\u2026", "...")
                            .replace("&amp;", "&")
                            .replace("&lt;", "<")
                            .replace("&gt;", ">")
                            .replace("&#39;", "'")
                            )

            # URL and special character-less version
            tweet['TEXTXURLS'] = tweet['TEXT'].encode('ascii','ignore').decode('utf-8').rstrip().lstrip()
            tweet['TEXTXURLS'] = re.sub(r"http\S+", "", tweet['TEXTXURLS']).rstrip().lstrip()
            
            # Just the URLs - add a space to the end for display - some create a link that includes 
            # the start of the next word, ignoring the comma between
            tweet['URLS'] = ' '.join(re.findall(r"http\S+", tweet['TEXT'])) + ' '
            
            # Encode main text only after using it as the basis for a URL and special character-less version above
            tweet['TEXT'] = tweet['TEXT'].encode('unicode_escape').decode('utf-8').rstrip().lstrip()
            
            return tweet
        else: 
            return None
    else:
        print("Imcomplete tweet: %s" % tweet)
        return None
#        logger.warning("Imcomplete tweet: %s", tweet)


# extract relevant data to write to CSV
def extract_content(tweet):

    content = [tweet['id'],
               tweet['user']['id'],
               tweet['user']['screen_name'],
               tweet['user']['location'],
               tweet['user']['utc_offset'],
               tweet['user']['time_zone'],
               tweet['CREATED_AT'],
               tweet['TWEET_TYPE'],
               tweet['TEXTXURLS'],
               tweet['URLS'], 
               tweet['TEXT'],
               tweet['place']
               ]


    return content

def format_filename(fname, uname, dsuffix):
    """Convert file name into a safe string.

    Arguments:
        fname -- the file name to convert
        uname -- alternative user defiend file name
        dsuffix -- date suffix if required (change of date drives regeneration of new date-suffixed file name)
    Return:
        String -- converted file name
    """
    filename = ""
    if uname is not None and len(uname) > 0:
        filename = ''.join(convert_valid(one_char) for one_char in uname)
    else:
        filename = ''.join(convert_valid(one_char) for one_char in fname)

    if dsuffix is not None:
        filename = filename + "_" + dsuffix

    return filename


def convert_valid(one_char):
    """Convert a character into '_' if invalid.

    Arguments:
        one_char -- the char to convert
    Return:
        Character -- converted char
    """
    valid_chars = "-_.%s%s" % (string.ascii_letters, string.digits)
    if one_char in valid_chars:
        return one_char
    else:
        return '_'

@classmethod
def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret)
    api = tweepy.API(auth)

    datesuffix = None
    if (args.date_suffix is not None and args.date_suffix in ["Y","y","1"]):
        datesuffix = time.strftime("%Y%m%d")

    userfname = args.file_name
    tweets_only = (args.tweets_only is not None and args.tweets_only in ["Y","y","1"])
    
    twitter_stream = Stream(auth, MyListener(args.data_dir, args.query, args.file_name, datesuffix, tweets_only))
    twitter_stream.filter(track=[args.query], languages=["en"])
