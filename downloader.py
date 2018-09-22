#!/usr/bin/python3

# Python 3 Script for  Parallel download of files. This script has Support for S3, HTTP and HTTP protocols.

import urllib.request, urllib.parse, urllib.error, boto3, botocore, sys, os
from urllib.parse import urlparse
from botocore.exceptions import ClientError
import threading

def url_http_https(uri):
    url = (uri)
    try:
        urllib.request.urlretrieve(url, (os.path.basename(uri)))
        print("{} Download Complete".format(uri))
    except urllib.error.URLError as e:
        ResponseData = e.reason
        print("{} {}".format(ResponseData, uri))


def url_s3(bkt):
    o2 = urlparse(bkt)
    bkt_name = (o2.netloc)
    file_name = (os.path.basename(bkt))
    s3_client = boto3.client('s3')

    try:
       s3_client.download_file((bkt_name), (file_name), (file_name))
       print('{} Download Complete'.format(bkt))

    except botocore.exceptions.ClientError as e:
       error_code = int(e.response['Error']['Code'])
       print('ERROR {} for URL {}'.format(error_code, bkt))

		  
		  
print('Number of URLS provided for download are: {}'.format((len(sys.argv)-1)))

for i in sys.argv:
    if i == 'downloader.py' or i == './downloader.py':
       continue
    o = urlparse(i)
    if (o.scheme) == 's3':
       thread = threading.Thread(target = url_s3, args=(i,))
       thread.start()
       print('Launched thread {} '.format(str(i)))
    elif (o.scheme) == 'https' or (o.scheme) == 'http':
       thread = threading.Thread(target = url_http_https, args=(i,))
       thread.start()
       print('Launched thread {} '.format(str(i)))
    else:
       print('{} is not a valid url'.format(i))
