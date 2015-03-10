#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
>>> def do (s):
...     for i in split(s):
...         print i

>>> do('개발용역(국방부 합동참모본부) 하하하')
합동참모본부
국방부
개발용역
하하하
>>> do('개발용역(국방부,합동참모본부) 하하하')
합동참모본부
국방부
개발용역
하하하
>>> do('선반, NC')
NC
선반
>>> do('유압 프레스 선반')
유압
프레스
선반
>>> do('"중국 근현대 강역 문제 연구" 한글 번역 용역')
문제
강역
한글
중국 근현대 강역 문제 연구
근현대
용역
번역
연구
중국
>>> do('\\'중국 근현대 강역 문제 연구\\' 한글 번역 용역')
문제
강역
한글
중국 근현대 강역 문제 연구
근현대
용역
번역
연구
중국
>>> do('\\'04년도 주전산기체계 및 EDMS 리스등 10종 보험')
리스등
10종
04년도
EDMS
주전산기체계
보험
>>> do('\\'08-해군 인터넷 홈페이지 개선 사업')
해군
08
개선
08-해군 인터넷 홈페이지 개선 사업
인터넷
사업
홈페이지
>>> do('\\'09년 전산장비(PC) 정비용역')
PC
09년
전산장비
정비용역
>>> do('\\'10~\\'12년 컴퓨터체계 통합유지보수')
10
12
컴퓨터체계
년
12년 컴퓨터체계 통합유지보수
통합유지보수
>>> do('\\'10년 오수처리시설 본체 설계(1공구)(2010-2035)')
1
공구
1공구
10년
설계
본체
오수처리시설
2035
2010-2035
2010
>>> do('(2014-05)&#39;국가비축 항바이러스제 사용기간 연장 프로그램 연구&#39;용역 계약요청')
2014-05
2014
05
항바이러스제
계약요청
프로그램
사용기간
연장
용역
연구
국가비축
>>> do('00부대 유류지원대 토양오염정화사업 건설폐기물 용역')
토양오염정화사업
유류지원대
건설폐기물
부대
용역
>>> do('120다산콜센터')
120
120다산콜센터
다산콜센터
>>> do('1·2·3호선 냉난방·위생·오폐수설비 유지관리 용역(1차)')
1
1차
차
호선
위생
용역
1
3
2
유지관리
냉난방
오폐수설비
3호선 냉난방
>>> do('2012학년도 1학년 수련회 위탁용역')
1학년
2012학년도 1학년 수련회 위탁용역
학년도
위탁용역
수련회
2012
>>> do('[긴급]공주월송 국민임대주택단지 조성사업 사후환경영향조사 용역')
사후환경영향조사
용역
조성사업
공주월송
국민임대주택단지
긴급
>>> do('0 /9.0ha @ 1')
1
0
0 /9.0ha @ 1
9.0ha
'''


import argparse
import string
import re
import csv
import sys
import HTMLParser
import dbm


parser = argparse.ArgumentParser(description='fetch keywords from contract csv data from data.go.kr')
parser.add_argument('--field', type=int, help='field number of keywords', required=True)
parser.add_argument('--dry-run', action='store_true', help='don\'t fetch keyword, just print field string')
parser.add_argument('--db-to-csv', action='store_true', help='print db data to csv')
parser.add_argument('--test', action='store_true', help='run test')
parser.add_argument('-d', dest='debug', action='store_true', default=False, help='debug')
parser.add_argument('filename', help='csv file')


html_parser = HTMLParser.HTMLParser()


def html_escape(s):
    if type(s) in (str,):
        s = s.decode('utf-8')

    return html_parser.unescape(s).encode('utf-8')


def strip_list(l):
    return filter(bool, map(string.strip, l))


def index(s, k):
    try:
        return s.index(k)
    except ValueError:
        return -1


def filter_keyword(s):
    if not s.strip():
        return False

    if len(s) > 50:
        return False

    return True


RE_QUOTE = re.compile('([\'\"].*[\'\"])')
RE_PUNC_S = re.compile('^[%s]' % (''.join(map(re.escape, string.punctuation)), ))
RE_PUNC_E = re.compile('[%s]$' % (''.join(map(re.escape, string.punctuation)), ))
RE_HTML_ENTITIES = re.compile('\&#[\d]+\;', re.I)
RE_00_PREFIX = re.compile('^(00)([\s\w])', re.U)
RE_DIGIT = re.compile('^([\d][\d]*)')
RE_NONE_DIGIT = re.compile('^[^\d]')
RE_BRACKET = re.compile('[\[\]]')

STOP_WORDS = (
    '및',
)


def split(s, **kw):
    if not s:
        return list()

    if RE_HTML_ENTITIES.search(s):
        s = html_escape(s)

    if RE_00_PREFIX.search(s.strip()):
        s = s.strip()[2:].strip()

    if index(s, '·') > -1:
        s = s.replace('·', ',')

    if RE_BRACKET.search(s):
        s = RE_BRACKET.sub(' ', s)

    sl = list()

    # brace must be one word
    bs = index(s, '(')
    be = index(s, ')')

    if bs > -1 and be > bs:
        sl.extend(split(s[bs + 1:be]))
        sl.extend(split(s[:bs]))
        sl.extend(split(s[be + 1:]))

        s = ''

    if not s:
        return sl

    rs = RE_QUOTE.search(s)
    if rs:
        rs_s, rs_e = rs.span()
        sl.extend(split(s[:rs_s]))
        sl.append(s[rs_s + 1:rs_e - 1])
        sl.extend(split(s[rs_s + 1:rs_e - 1]))
        sl.extend(split(s[rs_e:]))
    elif index(s, '-') > -1:
        sl.append(s)
        for i in s.split('-'):
            sl.extend(split(i))
    elif index(s, ',') > -1:
        for i in strip_list(s.split(',')):
            sl.extend(split(i))
    elif kw.get('not_split_blank'):
        sl.append(s.strip())
    elif RE_DIGIT.search(s):
        a = RE_DIGIT.sub('', s)
        if RE_NONE_DIGIT.search(a.strip()):
            sl.append(RE_DIGIT.search(s).groups()[0])
            sl.append(s)
            sl.extend(split(a))
        else:
            sl.extend(strip_list(s.split()))
    else:
        sl.extend(strip_list(s.split()))

    # escape html entities
    return filter(filter_keyword, list(set(filter(
        lambda y: y not in STOP_WORDS,
        map(lambda x: RE_PUNC_E.sub('', x), map(lambda x: RE_PUNC_S.sub('', x), sl))
    ))))


def main():
    keywords = dict()

    n = 0
    for i in csv.reader(file(options.filename), dialect='excel'):
        s = i[options.field]
        if options.dry_run:
            print s
            continue

        for j in split(s):
            if type(j) in (unicode,):
                j = j.strip().decode('utf-8')

            j = j.strip()

            keywords.setdefault(j, 0)
            keywords[j] += 1

        if (n % 1000) == 0:
            print '> fetch %d rows, %d keywords' % (n, len(keywords),)

        n += 1

    print '< found %d keywords' % len(keywords)
    print '> store in db'
    if not options.dry_run:
        db = dbm.open('keywords', 'c')
        for k, v in keywords.iteritems():
            if k not in db:
                db[k] = str(0)

            db[k] = str(int(db[k]) + v)

    db.close()
    print '< done'

    return


def print_db_to_csv():
    db = dbm.open('keywords', 'c')
    for i in db.keys():
        print '"%s",%s' % (i, db[i], )

    return


if __name__ == '__main__':
    options = parser.parse_args()

    if options.test:
        import doctest
        doctest.testmod()
        sys.exit(0)

    if options.db_to_csv:
        print_db_to_csv()
    else:
        main()

    sys.exit(0)
