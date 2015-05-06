# -*- coding: utf-8 -*-
"""
Based largely on Yelp API v2.0 code sample. Link to their GitHub:
https://github.com/Yelp/yelp-api/tree/master/v2/python

Refer to http://www.yelp.com/developers/documentation for the API documentation.
"""
import argparse
import json
import pprint
import sys
import urllib
import urllib2
import oauth2
import pandas as pd 

API_HOST = 'api.yelp.com'
DEFAULT_TERM = 'restaurant'
DEFAULT_LOCATION = 'new york'
SEARCH_LIMIT = 20
SEARCH_PATH = '/v2/search/'
BUSINESS_PATH = '/v2/business/'
SORT = 1
RADIUS_FILTER = 33000
offset = 0

APIKEYS = pd.read_json('APIKeys.json')

CONSUMER_KEY = (APIKEYS['CONSUMER_KEY'].values)[0]
CONSUMER_SECRET = (APIKEYS['CONSUMER_SECRET'].values)[0]
TOKEN = (APIKEYS['TOKEN'].values)[0]
TOKEN_SECRET = (APIKEYS['TOKEN_SECRET'].values)[0]

#I pulled the full list of NYC neighborhoods that Yelp recognizes
#from their site, and stored into a file to read in
locations = pd.read_json('YelpNeighborhoodlistNY.json')

#Also pulled the full list of activity, restaurant type, bar type
#and arts/entertainment keywords that Yelp recognizes.
activities = pd.read_json('SimpleYelpActivityKeywords.json')

business_id = []
name = []
latitude = []
longitude = []
zipcode = []
city = []
neighborhoods = []
categories = []
review_count = []
rating = []

df = pd.DataFrame(data={'business_id' : business_id, 'name' : name, 
    'latitude' : latitude, 'longitude' : longitude, 'zipcode' : zipcode, 
    'city' : city, 'neighborhoods' : neighborhoods, 'categories' : categories, 
    'review_count' : review_count, 'rating':rating})

df.to_csv('yelpAPIresults1.csv', index_label='index')

def request(host, path, url_params=None):
    # Prepares OAuth authentication and sends the request to the API. Args:
    #     host (str): The domain host of the API.
    #     path (str): The path of the API after the domain.
    #     url_params (dict): An optional set of query parameters in the request.
    # Returns: dict: The JSON response from the request.
    # Raises: urllib2.HTTPError: An error occurs from the HTTP request.
    
    url_params = url_params or {}
    url = 'http://{0}{1}?'.format(host, urllib.quote(path.encode('utf8')))

    consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
    oauth_request = oauth2.Request(method="GET", url=url, parameters=url_params)

    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': TOKEN,
            'oauth_consumer_key': CONSUMER_KEY
        }
    )
    token = oauth2.Token(TOKEN, TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()
    
    print u'Querying {0} ...'.format(url)

    conn = urllib2.urlopen(signed_url, None)
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()

    return response

# def search(term, location):
# def search(term, location, offset):
def search(term, location):
    # Query the Search API by a search term and location. Args:
    #     term (str): The search term passed to the API.
    #     location (str): The search location passed to the API.
    # Returns dict: The JSON response from the request.
    
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        # 'sort': SORT,
        # 'offset': offset,
        # 'radius_filter' : RADIUS_FILTER
    }
    return request(API_HOST, SEARCH_PATH, url_params=url_params)


for i in activities['Entertainment']:
    print i
    for k in locations['Neighborhood']:
        print k
        # total_businesses = 0
        # offset = 0
        # while offset <= total_businesses:
        try:
            response = search(i, "nyc" + k)
        except:
            continue
        else:
            # total_businesses = response['total'] - 1
            # print "total number of businesses for " + i + " in " + k + " is " + str(total_businesses)
            businesses = response.get('businesses')
            # print businesses[0]
            if len(businesses) == 0:
                total_businesses = -1
                continue
            else: 
                for j in range(len(businesses)):
                # print businesses[j]['name']
                    business_id = []
                    name = []
                    latitude = []
                    longitude = []
                    zipcode = []
                    city = []
                    neighborhoods = []
                    categories = []
                    review_count = []
                    rating = []
                    try:
                        #if any of these are not included in the response, skip them.
                        business_id.append(businesses[j]['id'])
                        name.append(businesses[j]['name'])
                        latitude.append(businesses[j]['location']['coordinate']['latitude'])
                        longitude.append(businesses[j]['location']['coordinate']['longitude'])
                        zipcode.append(businesses[j]['location']['postal_code'])
                        city.append(businesses[j]['location']['city'])
                        neighborhoods.append(businesses[j]['location']['neighborhoods'])
                        categories.append(businesses[j]['categories'])
                        review_count.append(businesses[j]['review_count'])
                        rating.append(businesses[j]['rating'])
                        print businesses[j]['name']

                        df = pd.DataFrame(data={'business_id' : business_id, 'name' : name, 
                            'latitude' : latitude, 'longitude' : longitude, 'zipcode' : zipcode, 
                            'city' : city, 'neighborhoods' : neighborhoods, 'categories' : categories, 
                            'review_count' : review_count, 'rating':rating})                        
                        
                        with open('yelpAPIresults1.csv', 'a') as f:
                            df.to_csv(f, header=False, encoding='utf-8')
                    except:
                        continue
                    # offset = offset+20

        # else:
        #     offset = 0
        #     total_businesses = 0
        #     continue


# yelpbizdf = pd.DataFrame(data={'business_id' : business_id, 'name' : name, 
#     'latitude' : latitude, 'longitude' : longitude, 'zipcode' : zipcode, 
#     'city' : city, 'neighborhoods' : neighborhoods, 'categories' : categories, 
#     'review_count' : review_count, 'rating':rating})

# yelpbizdf.to_csv('yelpAPIresults.csv', index_label='index', encoding='utf-8')















