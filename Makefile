all: bench

bench: bench.c leveldb/libleveldb.a
	g++ -o $@ $< -g -Wall -Wno-sign-compare -lpthread -pthread -Ileveldb/include/ leveldb/libleveldb.a

leveldb/libleveldb.a:
	[ -d leveldb ] || git clone https://github.com/gmargari/leveldb
	cd leveldb; make

clean:
	rm -f bench
