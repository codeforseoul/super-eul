#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib
import socket
import csv
import pprint
import datetime
import sys
import httplib
import urlparse
import argparse
import time
import xml.etree.ElementTree as ET


'''
낙찰정보

from https://www.data.go.kr/subMain.jsp#/L3B1YnIvdXNlL3ByaS9Jcm9zT3BlbkFwaURldGFpbC9vcGVuQXBpTGlzdFBhZ2UkQF4wMTJtMSRAXnB1YmxpY0RhdGFQaz0xNTAwMDU5MSRAXmJybUNkPU9DMDAwMyRAXnJlcXVlc3RDb3VudD0xNTckQF5vcmdJbmRleD1PUEVOQVBJ

- 공고일 대신 개찰일을 기준으로 수집합니다.
'''

DATA_TYPE = dict(
    thing="getScsbidListSttusThng",
    foreign="getScsbidListSttusFrgcpt",
    service="getScsbidListSttusServc",
)


################################################################################
# options
parser = argparse.ArgumentParser(description='get subscribe data from data.go.kr')
parser.add_argument('--service-key', help='service key for data.go.kr', required=True)
parser.add_argument('-d', dest='debug', action='store_true', default=False, help='debug')
parser.add_argument('--number-of-result', type=int, help='number of result', default=20000)
parser.add_argument('--page', type=int, help='page number to start', default=1)
parser.add_argument('--start-date', help='start date of data, YYYYMMDD', required=True)
parser.add_argument('--end-date', help='end date of data, YYYYMMDD')
parser.add_argument('--type', choices=DATA_TYPE.keys(), required=True, help='종류, ex) thing : 물품, foreign: 외자, service: 용역')

options = parser.parse_args()

default_query = dict(
    ServiceKey=urllib.unquote(options.service_key),
    sDate=options.start_date,
    eDate=options.end_date if options.end_date else datetime.datetime.now().strftime('%Y%m%d'),
    numOfRows=options.number_of_result,
    pageNo=options.page,
    type=options.type,
)

pprint.pprint(default_query)


url = 'http://openapi.g2b.go.kr/openapi/service/rest/ScsbidInfoService/%s' % DATA_TYPE.get(options.type)


def request(url, query=None):
    _parsed_url = urlparse.urlparse(url)
    if options.debug:
        print '[debug] parsed url: ', _parsed_url

    if _parsed_url.scheme in ('https',):
        _connection = httplib.HTTPSConnection
    else:
        _connection = httplib.HTTPConnection

    _conn = _connection(_parsed_url.netloc)

    _query = _parsed_url.query
    if query is not None:
        _query += ('&' if _query else '') + urllib.urlencode(query)

    _conn.request('GET', _parsed_url.path + '?' + _query)
    _response = _conn.getresponse()
    if _response.status not in (200,):
        print '[debug] ', _response.reason
        return None

    _content = _response.read()
    _conn.close()

    return _content


def get_data(page=1):
    query = default_query.copy()
    query['pageNo'] = page

    try:
        document = request(url, query=query)
    except socket.error:
        return None

    if document is None:
        return None

    root = ET.fromstring(document)
    result_code = root.find('.//resultCode')
    if result_code is None:
        return None

    if result_code.text not in ('00',):
        print '[error] %s' % root.find('.//resultMsg').text
        return None

    items = list()
    for i in root.findall('.//item'):
        item = dict()
        for j in i:
            item[j.tag] = j.text

        items.append(item)

    return dict(
        total=int(root.find('.//totalCount').text),
        number_of_result=int(root.find('.//numOfRows').text),
        page=int(root.find('.//pageNo').text),
        items=items,
    )


number_of_items = 0
page = default_query.get('pageNo')
header = None

csv_file_name = 'ScsbidInfoService-%(type)s-%(sDate)s-%(eDate)s.csv' % default_query

with open(csv_file_name, 'ab') as f:
    writer = csv.writer(f)
    while True:
        print '[ii] get data: page=%s' % (page,)
        s = time.time()
        result = get_data(page)
        print 'got data: %f' % (time.time() - s, )
        if result is None:
            print '[ee] failed to get data'
            break
            continue

        for item in result.get('items'):
            if not header:
                header = item.keys()
                writer.writerow(
                    map(
                        lambda x: x.encode('utf-8'),
                        header,
                    )
                )

            writer.writerow(
                map(
                    lambda x: item.get(x, '').encode('utf-8'),
                    header,
                )
            )

        number_of_items += len(result.get('items'))
        if number_of_items >= result.get('total'):
            print '[ii] got final data: number of item=%s' % (number_of_items,)
            break

        page += 1

        f.flush()

sys.exit(0)
