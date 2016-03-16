# dyndb-kopier
A simple python script to copy dynamodb table.
Taken from https://github.com/techgaun/dynamodb-copy-table and renamed it to a shorter name.

The aim of this script is only to copy content from one dynamodb table to another on in another region.
It's to make migration easier for smaller table, althought it can also be used for medium sized ones.

---

### Requirements

- Python 2.x
- boto (`pip install boto`)

### Usage

A simple usage example:

```shell
$ python dyndb-kopier.py src_region src_table dst_region dst_table
```

This script version doesn't create the destination table for you. It just assumes it already exists.

### References

- [Import and Export DynamoDB Data using AWS Data Pipeline](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb.html)
- [Original script](https://gist.github.com/iomz/9774415) - had to modify and add support for tables with only hash key
- [Techgaun script](https://github.com/techgaun/dynamodb-copy-table) - modify to add support for copying across region and dropped support for table creation
