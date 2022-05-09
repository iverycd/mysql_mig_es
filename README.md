# INTRODUCE
A TOOL YOU CAN MIGRATE MySQL ALL TABLES INTO ELASTICSEARCH ONLINE

`SUPPORT`
`MySQL`: 5.7 AND 8
`ELASTICSEARCH`: 8.1.0

# HOW TO USE
## 1. ADD CONNECTIONS IN config.ini

```bash
[mysql]
host = 192.168.218.6
port = 3306
user = root
passwd = Gepoint
database = yst
dbchar = utf8mb4



[elastic]
host = http://192.168.218.6:9200

[option]
page_size=20000
es_bulk_chunk_size=1000
es_thread_count=4
bulk_split_thread=2
mysql_fenye_parallel_run=2
```

`NOTE`

- page_size:MYSQL FETCH SIZE LIMIT,FOR EXAMPLE:SLECT * FROM TEST LIMT 20000;
- es_bulk_chunk_size:PYTHON ELASTICSEARCH API parallel_bulk WRITE CHUNK SIZE
- es_thread_count:PYTHON ELASTICSEARCH API parallel_bulk WRITE THREAD COUNT
- bulk_split_thread:SPLIT MYSQL RESULT ROW SHARD
- mysql_fenye_parallel_run:MULTI PROCESS READ MYSQL TABLE DATA

## 2. ADD SOURCE TABLE AND TARGET INDEX IN data_dict_map.txt

```json
{"index": "YOUR TARGET INDEX NAME", "table_name": "YOUR SOURCE TABLE NAME"}
eg:
{"index": "sales_info", "table_name": "sales_info"}
```

## 3. START DATA TRANSFER

```python
python myBulkES.py
```
