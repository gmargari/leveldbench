leveldbench
===========

Multi-threaded benchmark for LevelDB.

```
syntax: ./bench [options]

GENERAL OPTIONS
 -m, --memstore-size VALUE         memstore size, in MB (program may use up to 2x of this) [100]
 -C, --compress                    compress database files [false]

PUT OPTIONS
 -i, --insert-bytes VALUE          number of bytes to insert in MB [1000]
 -n, --num-keys VALUE              number of KVs to insert [953250]
 -k, --key-size VALUE              size of keys, in bytes [100]
 -v, --value-size VALUE            size of values, in bytes [1000]
 -u, --unique-keys                 create unique keys [false]
 -z, --zipf-keys VALUE             create zipfian keys, with given distribution parameter [false]
 -o, --ordered-keys VALUE          keys created are ordered, with VALUE probability being random [false]
 -P, --put-throughput VALUE        put requests per sec (0: unlimited) [0]

GET OPTIONS
 -g, --get-threads VALUE           number of get threads [0]
 -G, --get-throughput VALUE        get requests per sec per thread (0: unlimited) [10]
 -R, --range-get-size VALUE        max number of KVs to read (0: point get) [10]

VARIOUS OPTIONS
 -e, --print-kvs-to-stdout         print KVs that would be inserted and exit
 -s, --read-kvs-from-stdin         read KVs from stdin
 -t, --print-periodic-stats        print stats on stderr every 5 sec
 -D, --db-dir VALUE                where to store the db files [/tmp/testdb]
 -F, --flush-db                    flush db in stdout a human-readable form
 -E, --erase-db                    erase any existing files in database directory
 -h, --help                        print this help message and exit
```
