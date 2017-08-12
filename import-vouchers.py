#!/usr/bin/env python3
import pandas as pd
import elasticsearch
import elasticsearch.helpers
from datetime import datetime

df = pd.read_excel('march2017.xlsx')

df['Voucher Number'] = df['Voucher Number'].str.replace(r'[^0-9A-Z]', '')

df['Description'] = df['Description'].str.replace(r'\xa0+', ';')
df['Description'] = df['Description'].str.replace(r'\s+', ' ')
df['Description'] = df['Description'].str.upper()
df['Description'] = df['Description'].str.strip()

# extract parent and child names
m1 = df['Description'].str.extract(r'^[0-9A-Z]+ [0-9\-]+ (?P<Parent>[A-Z\- ]+) (?P<DateFor>\d+\-\d+) ;', expand=False)
m2 = df['Description'].str.extract(r'QLTY \S+ (?P<Children>[A-Z\- ]+)', expand=False)

df = pd.concat([df, m1, m2], axis=1)


def cleanup_name(s):
    return ' '.join(map(str.capitalize, s.split()))


def actions():
    for _, row in df.iterrows():
        # issue_date = row['Issue Date']
        issue_date = datetime.strptime(row['DateFor'], '%m-%y')
        voucher_number = row['Voucher Number'].strip()

        # doc = {
        #     '_index': 'cases',
        #     '_type': 'case',
        #     '_id': voucher_number,
        #     'parent': cleanup_name(row['Parent']),
        # }
        #
        # try:
        #     doc['children'] = cleanup_name(row['Children'])
        # except:
        #     pass
        #
        # print(doc)
        #
        # yield doc

        doc = {
            '_index': 'rainbow',
            '_type': 'voucher',
            '_id': issue_date.strftime('%Y%m%d{}'.format(voucher_number)),
            'issue_date': issue_date.strftime('%Y/%m/%d'),
            'voucher_number': voucher_number,
            'parent': cleanup_name(row['Parent']),
            'check_number': row['Warrant/EFT#'],
            'payment_amount': row['Payment Amount'],
        }

        try:
            doc['children'] = cleanup_name(row['Children'])
        except:
            pass

        yield doc


es = elasticsearch.Elasticsearch()
elasticsearch.helpers.bulk(es, actions())
