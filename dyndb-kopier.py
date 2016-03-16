#!/usr/bin/python2
from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from time import sleep
import sys
import os

# Taken and modified from:
# https://github.com/techgaun/dynamodb-copy-table
# Usage: python dyndb-kopier.py src_region src_table dst_region dst_table

if len(sys.argv) != 5:
    print 'Usage: %s <source_region> <source_table_name>' \
          ' <destination_region> <destination_table_name>' % sys.argv[0]
    sys.exit(1)

src_region = sys.argv[1]
src_table = sys.argv[2]
dst_region = sys.argv[3]
dst_table = sys.argv[4]

source_hostname = 'dynamodb.{}.amazonaws.com'.format(src_region)
destination_hostname = 'dynamodb.{}.amazonaws.com'.format(dst_region)
source_ddbc = DynamoDBConnection(is_secure=False, region=src_region, host=source_hostname)
destination_ddbc = DynamoDBConnection(is_secure=False, region=dst_region, host=destination_hostname)

# 1. Try to connect to the source table
try:
    src_logs = Table(src_table, connection=source_ddbc)
    print "Connected to source table %s in region %s" % (src_table, src_region)
except JSONResponseError:
    print "Source table %s in region %s does not exist" % (src_table, src_region)
    sys.exit(1)(src_table, src_region)

print 'Reading source key schema from %s in %s' % (src_table, src_region)
src_table_describe = source_ddbc.describe_table(src_table)['Table']
src_hash_key = ''
src_range_key = ''
for schema in src_table_describe['KeySchema']:
    attr_name = schema['AttributeName']
    key_type = schema['KeyType']
    if key_type == 'HASH':
        src_hash_key = attr_name
    elif key_type == 'RANGE':
        src_range_key = attr_name
print 'Found hash_key = %s, and range_key = %s' % (src_hash_key, src_range_key or 'None')

# 2. Try to connect to the destination table
try:
    dst_logs = Table(dst_table, connection=destination_ddbc)
    print "Connected to destination table %s in region %s" % (dst_table, dst_region)
except JSONResponseError:
    print "Destination Table %s in region %s does not exist" % (dst_table, dst_region)
    sys.exit(1)

print 'Reading destination key schema from %s in %s' % (dst_table, dst_region)
dst_table_describe = destination_ddbc.describe_table(dst_table)['Table']
dst_hash_key = ''
dst_range_key = ''
for schema in dst_table_describe['KeySchema']:
    attr_name = schema['AttributeName']
    key_type = schema['KeyType']
    if key_type == 'HASH':
        dst_hash_key = attr_name
    elif key_type == 'RANGE':
        dst_range_key = attr_name
print 'Found hash_key = %s, and range_key = %s' % (dst_hash_key, dst_range_key or 'None')

# 3. Compare hash key and range key
if src_hash_key != dst_hash_key or src_range_key != dst_range_key:
    print 'Source table schema is not the same as destination table schema'
    sys.exit(1)

# 4. Copy the data from source table to destination able
print 'Starting to copy from %s in %s to %s in %s' % (src_table, src_region, dst_table, dst_region)
for item in src_logs.scan():
    new_item = {}
    new_item[src_hash_key] = item[src_hash_key]
    if src_range_key != '':
        new_item[src_range_key] = item[src_range_key]
    for f in item.keys():
        if f in [src_hash_key, src_range_key]:
            continue
        new_item[f] = item[f]
    try:
        dst_logs.put_item(new_item, overwrite=True)
    except ValidationException:
        print dst_table, new_item
    except JSONResponseError:
        print destination_ddbc.describe_table(dst_table)['Table']['TableStatus']

print 'We are done. Exiting...'