# Requires snappy [de]compression library: https://github.com/google/snappy
# On ubuntu, just: apt-get install libsnappy-dev

all: bench

bench: bench.c leveldb/libleveldb.a
	g++ -o $@ $< -g -Wall -Wno-sign-compare -lpthread -pthread -Ileveldb//include/ leveldb/libleveldb.a -lsnappy

leveldb/libleveldb.a:
	[ -d leveldb ] || git clone https://github.com/gmargari/leveldb
	cd leveldb; make

clean:
	rm -f bench
