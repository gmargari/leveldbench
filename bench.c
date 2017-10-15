#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <getopt.h>
#include <sys/time.h>
#include <signal.h>
#include <limits.h>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <map>
#include <vector>

// LevelDB includes
#if defined(ROCKSDB_COMPILE) || defined(TRIAD_COMPILE)
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#elif defined(HYPERLEVELDB_COMPILE)
#include "hyperleveldb/db.h"
#include "hyperleveldb/env.h"
#else
#include "leveldb/db.h"
#include "leveldb/env.h"    // for NowMicros()
#endif

#include "RequestThrottle.h"

#if defined(ROCKSDB_COMPILE) || defined(TRIAD_COMPILE)
namespace leveldb = rocksdb;
#elif defined(FLODB_COMPILE)
namespace leveldb {
	size_t kBigValueSize = 256;
	int kNumThreads = 4;
}
#endif

using std::cout;
using std::cerr;
using std::endl;
using std::setw;
using std::right;
using std::flush;

#define b2mb(b) ((b)/(1024.0*1024.0))
#define mb2b(mb) ((uint64_t)((mb)*(1024*1024)))
#define max(a,b) ((a) > (b) ? (a) : (b))
#define min(a,b) ((a) < (b) ? (a) : (b))
#define TRUE_FALSE(b) ((b) == true ? "true" : "false")
#define ISDEFAULT(flag) (flag == 0 ? "(default)" : "")

void     *put_routine(void *args);
void     *get_routine(void *args);
void      update_put_stats(uint32_t latency);
void      update_get_stats(int tid, uint32_t latency);
void      print_put_get_stats(int signum);
void      randstr_r(char *s, const int len, uint32_t *seed);
void      zipfstr_r(char *s, const int len, double zipf_param, uint32_t *seed);
void      orderedstr_r(char *s, const int len, uint32_t *seed);
int       zipf_r(double zipf_param, uint32_t *seed);
void      check_duplicate_arg_and_set(int *flag, int opt);
int       numdigits(uint64_t num);
double    round(double r);

// default values
const char     *DEFAULT_DB_DIR =     "/tmp/testdb";
const uint64_t  DEFAULT_MEMSTORE_SIZE =  104857600;
const uint32_t  MAX_KEY_SIZE =                1024;
const uint32_t  MAX_VALUE_SIZE =            102400;
const uint64_t  DEFAULT_INSERTBYTES = 1048576000LL;  // 1GB
const uint32_t  DEFAULT_KEY_SIZE =             100;  // 100 bytes
const uint32_t  DEFAULT_VALUE_SIZE =          1000;  // 1000 bytes
const bool      DEFAULT_UNIQUE_KEYS =        false;
const bool      DEFAULT_ZIPF_KEYS =          false;
const bool      DEFAULT_ORDERED_KEYS =       false;
const int       DEFAULT_NUM_GET_THREADS =        0;
const int       DEFAULT_PUT_THRPUT =             0;  // req per sec, 0: disable
const int       DEFAULT_GET_THRPUT =            10;  // req per sec, 0: disable
const int       DEFAULT_RANGE_GET_SIZE =        10;  // get 10 KVs per range get
const bool      DEFAULT_FLUSH_PCACHE =       false;
const bool      DEFAULT_STATS_PRINT =        false;
const int       DEFAULT_STATS_PRINT_INTERVAL =   5;  // print stats every 5 sec
const int32_t   ZIPF_MAX_NUM =             1000000;
const uint64_t  DEFAULT_INSERTKEYS = DEFAULT_INSERTBYTES / (DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE);
const bool      DEFAULT_COMPRESS_DB =        false;

struct thread_args {
  int tid;
  int sflag;
  int uflag;
  int zipf_keys;
  double zipf_param;
  int ordered_keys;
  double ordered_prob;
  int print_kv_and_continue;
  uint64_t num_keys_to_insert;
  uint32_t keysize;
  uint32_t valuesize;
  int put_thrput;
  int get_thrput;
  int range_get_size;
  bool collect_stats;
  leveldb::Options options;
  leveldb::WriteOptions write_options;
  leveldb::ReadOptions read_options;
};

bool put_thread_finished = false;
int num_get_threads = 0;
std::map<uint32_t, uint32_t> put_latency;                  // put latency stats
pthread_mutex_t put_latency_mutex;
std::vector<std::map<uint32_t, uint32_t> > get_latency;    // get latency stats
std::vector<pthread_mutex_t> get_latency_mutex;
uint64_t bytes_inserted = 0;

leveldb::DB *db;  // multiple get threads and 1 put thread can use it concurrently

/*============================================================================
 *                               print_syntax
 *============================================================================*/
void print_syntax(char *progname) {
  cout << "syntax: " << progname << " [options]" << endl;
  cout << endl;
  cout << "GENERAL OPTIONS" << endl;
  cout << " -m, --memstore-size VALUE         memstore size, in MB (program may use up to 2x of this) [" << b2mb(DEFAULT_MEMSTORE_SIZE) << "]" << endl;
  cout << " -C, --compress                    compress database files [" << TRUE_FALSE(DEFAULT_COMPRESS_DB) << "]" << endl;
  cout << endl;
  cout << "PUT OPTIONS" << endl;
  cout << " -i, --insert-bytes VALUE          number of bytes to insert in MB [" << b2mb(DEFAULT_INSERTBYTES) << "]" << endl;
  cout << " -n, --num-keys VALUE              number of KVs to insert [" << DEFAULT_INSERTKEYS << "]" << endl;
  cout << " -k, --key-size VALUE              size of keys, in bytes [" << DEFAULT_KEY_SIZE << "]" << endl;
  cout << " -v, --value-size VALUE            size of values, in bytes [" << DEFAULT_VALUE_SIZE << "]" << endl;
  cout << " -u, --unique-keys                 create unique keys [" << TRUE_FALSE(DEFAULT_UNIQUE_KEYS) << "]" << endl;
  cout << " -z, --zipf-keys VALUE             create zipfian keys, with given distribution parameter [" << TRUE_FALSE(DEFAULT_ZIPF_KEYS) << "]" << endl;
  cout << " -o, --ordered-keys VALUE          keys created are ordered, with VALUE probability being random [" << TRUE_FALSE(DEFAULT_ORDERED_KEYS) << "]" << endl;
  cout << " -P, --put-throughput VALUE        put requests per sec (0: unlimited) [" << DEFAULT_PUT_THRPUT << "]" << endl;
  cout << endl;
  cout << "GET OPTIONS" << endl;
  cout << " -g, --get-threads VALUE           number of get threads [" << DEFAULT_NUM_GET_THREADS << "]" << endl;
  cout << " -G, --get-throughput VALUE        get requests per sec per thread (0: unlimited) [" << DEFAULT_GET_THRPUT << "]" << endl;
  cout << " -R, --range-get-size VALUE        max number of KVs to read (0: point get) [" << DEFAULT_RANGE_GET_SIZE << "]" << endl;
  cout << endl;
  cout << "VARIOUS OPTIONS" << endl;
  cout << " -e, --print-kvs-to-stdout         print KVs that would be inserted and exit" << endl;
  cout << " -s, --read-kvs-from-stdin         read KVs from stdin" << endl;
  cout << " -t, --print-periodic-stats        print stats on stderr every " << DEFAULT_STATS_PRINT_INTERVAL << " sec" << endl;
  cout << " -D, --db-dir VALUE                where to store the db files [" << DEFAULT_DB_DIR << "]" << endl;
  cout << " -F, --flush-db                    flush db in stdout a human-readable form" << endl;
  cout << " -E, --erase-db                    erase any existing files in database directory" << endl;
  cout << " -h, --help                        print this help message and exit" << endl;
}

/*============================================================================
 *                                   main
 *============================================================================*/
int main(int argc, char **argv) {
  const char short_args[] = "eg:hi:k:m:n:o:stuv:xz:CD:EFG:P:R:";
  const struct option long_opts[] = {
       {"memstore-size",          required_argument,  0, 'm'},
       {"compress",               no_argument,        0, 'C'},
       {"insert-bytes",           required_argument,  0, 'i'},
       {"num-keys",               required_argument,  0, 'n'},
       {"key-size",               required_argument,  0, 'k'},
       {"value-size",             required_argument,  0, 'v'},
       {"unique-keys",            no_argument,        0, 'u'},
       {"zipf-keys",              required_argument,  0, 'z'},
       {"ordered-keys",           required_argument,  0, 'o'},
       {"put-throughput",         required_argument,  0, 'P'},
       {"get-threads",            required_argument,  0, 'g'},
       {"get-throughput",         required_argument,  0, 'G'},
       {"range-get-size",         required_argument,  0, 'R'},
       {"print-kvs-to-stdout",    no_argument,        0, 'e'},
       {"read-kvs-from-stdin",    no_argument,        0, 's'},
       {"print-periodic-stats",   no_argument,        0, 't'},
       {"db-dir",                 required_argument,  0, 'D'},
       {"flush-db",               no_argument,        0, 'F'},
       {"erase-db",               no_argument,        0, 'E'},
       {"help",                   no_argument,        0, 'h'},
       {0, 0, 0, 0}
  };
  int      mflag = 0,
           Cflag = 0,
           iflag = 0,
           nflag = 0,
           kflag = 0,
           vflag = 0,
           uflag = 0,
           Pflag = 0,
           zflag = 0,
           oflag = 0,
           gflag = 0,
           Gflag = 0,
           Rflag = 0,
           eflag = 0,
           sflag = 0,
           tflag = 0,
           Dflag = 0,
           Fflag = 0,
           Eflag = 0,
           myopt,
           i,
           retval,
           indexptr,
           put_thrput,
           get_thrput,
           range_get_size;
  uint64_t memstore_size,
           insertbytes,
           num_keys_to_insert;
  uint32_t keysize,
           valuesize;
  double   zipf_param,
           ordered_prob;
  char    *key = NULL,
          *value = NULL,
          *end_key = NULL,
          db_dir[1000];
  bool    unique_keys,
          zipf_keys,
          ordered_keys,
          print_kv_and_continue = false,
          print_periodic_stats,
          compress_db,
          flush_db = false,
          erase_db = false;
  leveldb::Options options;
  leveldb::WriteOptions write_options;
  leveldb::ReadOptions read_options;
  std::vector<struct thread_args> targs;
  pthread_t *thread;
  struct tm *current;
  time_t now;

  //--------------------------------------------------------------------------
  // get arguments
  //--------------------------------------------------------------------------
  while ((myopt = getopt_long(argc, argv, short_args, long_opts, &indexptr))
        != -1) {
    switch (myopt)  {
      case 'h':
        print_syntax(argv[0]);
        exit(EXIT_SUCCESS);

      case 'm':
        check_duplicate_arg_and_set(&mflag, myopt);
        memstore_size = mb2b(atoll(optarg));
        break;

      case 'C':
        check_duplicate_arg_and_set(&Cflag, myopt);
        compress_db = true;
        break;

      case 'i':
        check_duplicate_arg_and_set(&iflag, myopt);
        insertbytes = mb2b(atof(optarg));
        break;

      case 'n':
        check_duplicate_arg_and_set(&nflag, myopt);
        num_keys_to_insert = atoll(optarg);
        break;

      case 'k':
        check_duplicate_arg_and_set(&kflag, myopt);
        keysize = atoi(optarg);
        break;

      case 'v':
        check_duplicate_arg_and_set(&vflag, myopt);
        valuesize = atoi(optarg);
        break;

      case 'u':
        check_duplicate_arg_and_set(&uflag, myopt);
        unique_keys = true;
        break;

      case 'z':
        check_duplicate_arg_and_set(&zflag, myopt);
        zipf_keys = true;
        zipf_param = atof(optarg);
        break;

      case 'o':
        check_duplicate_arg_and_set(&oflag, myopt);
        ordered_keys = true;
        ordered_prob = atof(optarg);
        break;

      case 'P':
        check_duplicate_arg_and_set(&Pflag, myopt);
        put_thrput = atoi(optarg);
        break;

      case 'g':
        check_duplicate_arg_and_set(&gflag, myopt);
        num_get_threads = atoi(optarg);
        break;

      case 'G':
        check_duplicate_arg_and_set(&Gflag, myopt);
        get_thrput = atoi(optarg);
        break;

      case 'R':
        check_duplicate_arg_and_set(&Rflag, myopt);
        range_get_size = atoi(optarg);
        break;

      case 'e':
        check_duplicate_arg_and_set(&eflag, myopt);
        print_kv_and_continue = true;
        break;

      case 's':
        check_duplicate_arg_and_set(&sflag, myopt);
        break;

      case 't':
        check_duplicate_arg_and_set(&tflag, myopt);
        print_periodic_stats = true;
        break;

      case 'D':
        check_duplicate_arg_and_set(&Dflag, myopt);
        strcpy(db_dir, optarg);
        break;

      case 'F':
        check_duplicate_arg_and_set(&Fflag, myopt);
        flush_db = true;
        break;

      case 'E':
        check_duplicate_arg_and_set(&Eflag, myopt);
        erase_db = true;
        break;

      case '?':
        exit(EXIT_FAILURE);

      default:
        abort();
    }
  }

  for (i = optind; i < argc; i++) {
    cerr << "Error: non-option argument: '" << argv[i] << "'" << endl;
    exit(EXIT_FAILURE);
  }

  //--------------------------------------------------------------------------
  // set default values
  //--------------------------------------------------------------------------
  if (mflag == 0) {
    memstore_size = DEFAULT_MEMSTORE_SIZE;
  }
  if (Cflag == 0) {
    compress_db = DEFAULT_COMPRESS_DB;
  }
  if (kflag == 0) {
    keysize = DEFAULT_KEY_SIZE;
  }
  if (vflag == 0) {
    valuesize = DEFAULT_VALUE_SIZE;
  }
  if (uflag == 0) {
    unique_keys = DEFAULT_UNIQUE_KEYS;
  }
  if (zflag == 0) {
    zipf_keys = DEFAULT_ZIPF_KEYS;
  }
  if (oflag == 0) {
    ordered_keys = DEFAULT_ORDERED_KEYS;
  }
  if (Pflag == 0) {
    put_thrput = DEFAULT_PUT_THRPUT;
  }
  if (iflag == 0) {
    if (nflag == 0) {
      insertbytes = DEFAULT_INSERTBYTES;
    } else {
      insertbytes = num_keys_to_insert * (keysize + valuesize);
    }
  }
  if (nflag == 0) {
    num_keys_to_insert = insertbytes / (keysize + valuesize);
  }
  if (gflag == 0) {
    num_get_threads = DEFAULT_NUM_GET_THREADS;
  }
  if (Gflag == 0) {
    get_thrput = DEFAULT_GET_THRPUT;
  }
  if (Rflag == 0) {
    range_get_size = DEFAULT_RANGE_GET_SIZE;
  }
  if (tflag == 0) {
    print_periodic_stats = DEFAULT_STATS_PRINT;
  }
  if (Dflag == 0) {
    strcpy(db_dir, DEFAULT_DB_DIR);
  }
  //--------------------------------------------------------------------------
  // check values
  //--------------------------------------------------------------------------
  if (kflag && keysize > MAX_KEY_SIZE) {
    cerr << "Error: 'keysize' cannot be bigger than " << MAX_KEY_SIZE << endl;
    exit(EXIT_FAILURE);
  }
  if (vflag && valuesize > MAX_VALUE_SIZE) {
    cerr << "Error: 'valuesize' cannot be bigger than " << MAX_VALUE_SIZE << endl;
    exit(EXIT_FAILURE);
  }
  if (zflag && zipf_param < 0) {
    cerr << "Error: zipf parameter must be >= 0" << endl;
    exit(EXIT_FAILURE);
  }
  if (oflag && (ordered_prob < 0 || ordered_prob > 1)) {
    cerr << "Error: probability for ordered keys must be in [0, 1]" << endl;
    exit(EXIT_FAILURE);
  }
  if (nflag && iflag) {
    cerr << "Error: you cannot set both 'insertbytes' and 'numkeystoinsert' parameters" << endl;
    exit(EXIT_FAILURE);
  }
  if (sflag) {
    if (kflag) {
      cerr << "Ignoring '-k' flag (keysize): keys will be read from stdin" << endl;
      kflag = 0;
      keysize = DEFAULT_KEY_SIZE;
    }
    if (vflag) {
      cerr << "Ignoring '-v' flag (valuesize): values will be read from stdin" << endl;
      vflag = 0;
      valuesize = DEFAULT_VALUE_SIZE;
    }
    if (uflag) {
      cerr << "Ignoring '-u' flag (unique keys): keys will be read from stdin" << endl;
      uflag = 0;
      unique_keys = DEFAULT_UNIQUE_KEYS;
    }
    if (zflag) {
      cerr << "Ignoring '-z' flag (zipf keys): keys will be read from stdin" << endl;
      zflag = 0;
      zipf_keys = DEFAULT_ZIPF_KEYS;
    }
  }
  if (Fflag && Eflag) {
    cerr << "Ignoring '-E' flag (erase db): a flush of db was asked" << endl;
    Eflag = 0;
    erase_db = false;
  }

  options.write_buffer_size = memstore_size;
  options.create_if_missing = true;
  options.error_if_exists = false;
  // These should be also possible to change via command line arguments
  options.paranoid_checks = false;
  write_options.sync = false;
  if (compress_db) {
    options.compression = leveldb::kSnappyCompression;
  } else {
    options.compression = leveldb::kNoCompression;
  }

  if (flush_db) {
    options.create_if_missing = false;
  }

  if (erase_db) {
    char cmd[1000];
    sprintf(cmd, "rm -rf %s/*", db_dir);
    system(cmd);
  }

  // create db
  leveldb::Status status = leveldb::DB::Open(options, db_dir, &db);
  if(!status.ok()) {
    cerr << "Error opening database: " << db_dir << endl;
    cerr << status.ToString() << endl;
    exit(EXIT_FAILURE);
  }

  if (flush_db) {
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
    }
    assert(it->status().ok());
    delete it;
    delete db;
    return EXIT_SUCCESS;
  }

  //--------------------------------------------------------------------------
  // print values of parameters
  //--------------------------------------------------------------------------
  cerr << "# memstore_size:       " << setw(15) << b2mb(memstore_size) << " MB  " << ISDEFAULT(mflag) << endl;
  if (sflag) {
    cerr << "# insert_bytes:        " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
    cerr << "# key_size:            " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
    cerr << "# value_size:          " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
    cerr << "# keys_to_insert:      " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
    cerr << "# unique_keys:         " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
    cerr << "# zipf_keys:           " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
    cerr << "# ordered_keys:        " << setw(15) << "?     (keys and values will be read from stdin)" << endl;
  } else {
    cerr << "# insert_bytes:        " << setw(15) << b2mb(insertbytes) << " MB  " << ISDEFAULT(iflag) << endl;
    cerr << "# key_size:            " << setw(15) << keysize << " B     " << ISDEFAULT(kflag) << endl;
    cerr << "# value_size:          " << setw(15) << valuesize << " B     " << ISDEFAULT(vflag) << endl;
    cerr << "# keys_to_insert:      " << setw(15) << num_keys_to_insert << endl;
    cerr << "# unique_keys:         " << setw(15) << TRUE_FALSE(unique_keys) << "       " << ISDEFAULT(uflag) << endl;
    cerr << "# zipf_keys:           " << setw(15) << TRUE_FALSE(zipf_keys) << "       " << ISDEFAULT(zflag) << endl;
    if (zipf_keys) {
      cerr << "# zipf_parameter:      " << setw(15) << zipf_param << endl;
    }
    cerr << "# ordered_keys:        " << setw(15) << TRUE_FALSE(ordered_keys) << "       " << ISDEFAULT(oflag) << endl;
    if (ordered_keys) {
      cerr << "# ordered_prob:      " << setw(15) << ordered_prob << endl;
    }
  }
  cerr << "# put_throughput:      " << setw(15) << put_thrput << " req/s " << ISDEFAULT(Pflag) << endl;
  cerr << "# get_threads:         " << setw(15) << num_get_threads << "       " << ISDEFAULT(gflag) << endl;
  cerr << "# get_throughput:      " << setw(15) << get_thrput << " req/s " << ISDEFAULT(Gflag) << endl;
  cerr << "# get_type:            " << setw(15) << (range_get_size ? "range" : "point") << endl;
  if (range_get_size) {
    cerr << "# range_get_size:      " << setw(15) << range_get_size << " keys  " << ISDEFAULT(Rflag) << endl;
  }
  cerr << "# compress_db:         " << setw(15) << TRUE_FALSE(compress_db) << "       " << ISDEFAULT(Cflag) << endl;
  cerr << "# read_from_stdin:     " << setw(15) << TRUE_FALSE(sflag) << "       " << ISDEFAULT(sflag) << endl;
  cerr << "# print_periodic_stats:" << setw(15) << TRUE_FALSE(print_periodic_stats) << "       " << ISDEFAULT(tflag) << endl;
  cerr << "# index_dir:           " << setw(15) << db_dir << endl;

  time(&now);
  current = localtime(&now);
  cerr << "[DATE]    " << current->tm_mday << "/" << current->tm_mon + 1 << "/" << current->tm_year + 1900 << endl;
  cerr << "[TIME]    " << current->tm_hour << ":" << current->tm_min << ":" << current->tm_sec << endl;

  fflush(stdout);

  //--------------------------------------------------------------------------
  // initialize variables
  //--------------------------------------------------------------------------
  key = (char *)malloc(MAX_KEY_SIZE + 1);
  end_key = (char *)malloc(MAX_KEY_SIZE + 1);
  value = (char *)malloc(MAX_VALUE_SIZE + 1);

  //--------------------------------------------------------------------------
  // fill-in arguments of put thread and get threads
  //--------------------------------------------------------------------------
  targs.resize(1 + num_get_threads);
  get_latency_mutex.resize(num_get_threads);
  get_latency.resize(num_get_threads);
  for (i = 0; i < 1 + num_get_threads; i++) {
    targs[i].tid = i;
    targs[i].uflag = uflag;
    targs[i].sflag = sflag;
    targs[i].zipf_keys = zipf_keys;
    targs[i].zipf_param = zipf_param;
    targs[i].ordered_keys = ordered_keys;
    targs[i].ordered_prob = ordered_prob;
    targs[i].print_kv_and_continue = print_kv_and_continue;
    targs[i].num_keys_to_insert = num_keys_to_insert;
    targs[i].keysize = keysize,
    targs[i].valuesize = valuesize;
    targs[i].put_thrput = put_thrput;
    targs[i].get_thrput = get_thrput;
    targs[i].range_get_size = range_get_size;
    targs[i].collect_stats = print_periodic_stats;
    targs[i].options = options;
    targs[i].read_options = read_options;
    targs[i].write_options = write_options;
  }

  //--------------------------------------------------------------------------
  // set signal handler and timer for periodic printing of get latency/thrput
  //--------------------------------------------------------------------------
  if (print_periodic_stats) {
    struct itimerval timer;
    if (signal(SIGALRM, print_put_get_stats) == SIG_ERR) {
      perror("Could not set signal handler");
      exit(EXIT_FAILURE);
    }

    timer.it_interval.tv_sec = DEFAULT_STATS_PRINT_INTERVAL;
    timer.it_interval.tv_usec = 0;
    timer.it_value.tv_sec = timer.it_interval.tv_sec;
    timer.it_value.tv_usec = timer.it_interval.tv_usec;
    if (setitimer(ITIMER_REAL, &timer, NULL) == -1) {
      perror("Could not set timer");
      exit(EXIT_FAILURE);
    }
  }

  if (print_periodic_stats) {
    print_put_get_stats(0);   // print time database was opened
  }

  //--------------------------------------------------------------------------
  // create put thread and get threads
  //--------------------------------------------------------------------------
  thread = (pthread_t *)malloc((1 + num_get_threads) * sizeof(pthread_t));
  for (i = 0; i < 1 + num_get_threads; i++) {
    if (i == 0) {
      retval = pthread_create(&thread[i], NULL, put_routine, (void *)&targs[i]);
    } else {
      retval = pthread_create(&thread[i], NULL, get_routine, (void *)&targs[i]);
    }
    if (retval) {
      perror("pthread_create");
      exit(EXIT_FAILURE);
    }
  }

  //--------------------------------------------------------------------------
  // wait for threads to finish
  //--------------------------------------------------------------------------
  for (i = 0; i < 1 + num_get_threads; i++) {
    pthread_join(thread[i], NULL);
  }

  if (print_periodic_stats) {
    // print one more last time
    print_put_get_stats(0);

    // disable periodic stats print
    struct itimerval timer;
    timer.it_interval.tv_sec = 0;
    timer.it_interval.tv_usec = 0;
    timer.it_value.tv_sec = 0;
    timer.it_value.tv_usec = 0;
    if (setitimer(ITIMER_REAL, &timer, NULL) == -1) {
      perror("Could not set timer");
      exit(EXIT_FAILURE);
    }
  }

  time(&now);
  current = localtime(&now);
  cerr << "[DATE]    " << current->tm_mday << "/" << current->tm_mon + 1 << "/" << current->tm_year + 1900 << endl;
  cerr << "[TIME]    " << current->tm_hour << ":" << current->tm_min << ":" << current->tm_sec << endl;

  free(key);
  free(end_key);
  free(value);
  free(thread);
  delete db;

  return EXIT_SUCCESS;
}

/*============================================================================
 *                                 put_routine
 *============================================================================*/
void *put_routine(void *args) {
  struct thread_args *targs = (struct thread_args *)args;
  int      uflag = targs->uflag,
           sflag = targs->sflag,
           zipf_keys = targs->zipf_keys,
           ordered_keys = targs->ordered_keys,
           print_kv_and_continue = targs->print_kv_and_continue;
  double   zipf_param = targs->zipf_param,
           ordered_prob = targs->ordered_prob;
  bool     print_periodic_stats = targs->collect_stats;
  uint64_t num_keys_to_insert = targs->num_keys_to_insert;
  uint32_t keysize = targs->keysize,
           valuesize = targs->valuesize;
  uint32_t kseed = targs->tid,  // kseed = getpid() + time(NULL);
           vseed = kseed + 1,
           sseed = 0,
           pseed = 0;
  char    *key = NULL,
          *value = NULL;
  uint32_t keylen, valuelen;
  RequestThrottle throttler(targs->put_thrput);
  struct timeval start, end;

  key = (char *)malloc(MAX_KEY_SIZE + 1);
  value = (char *)malloc(MAX_VALUE_SIZE + 1);

  // if we read keys and values from stdin set num_keys_to_insert to infinity
  if (sflag) {
    num_keys_to_insert = -1;  // ('num_keys_to_insert' is uint64_t)
  }

  //--------------------------------------------------------------------------
  // until we have inserted all keys
  //--------------------------------------------------------------------------
  for (uint64_t i = 0; i < num_keys_to_insert; i++) {

    //--------------------------------------------------------------
    // throttle request rate
    //--------------------------------------------------------------
    throttler.throttle();

    if (sflag) {
      //------------------------------------------------------------------
      // read key and value from stdin
      //------------------------------------------------------------------
      if (scanf("%s %s", key, value) != 2) {
        break;
      }
      keylen = strlen(key);
      valuelen = strlen(value);
    } else {
      //------------------------------------------------------------------
      // create a random key and a random value
      //------------------------------------------------------------------
      if (zipf_keys) {
        zipfstr_r(key, keysize, zipf_param, &kseed);
        keylen = keysize;
      } else if (ordered_keys) {
        // with probability 'ordered_prob', create a random key. else,
        // create an ordered key
        if ((float)rand_r(&pseed) / (float)RAND_MAX < ordered_prob) {
          randstr_r(key, keysize, &kseed);
        } else {
          orderedstr_r(key, keysize, &sseed);
        }
        keylen = keysize;
      } else {
        randstr_r(key, keysize, &kseed);
        keylen = keysize;
      }
      if (uflag) {
        // TODO: sprintf(key, "%s", key) -> undefined behaviour!
        sprintf(key, "%s.%llu", key, (long long)i);  // make unique
        keylen += 1 + numdigits(i);
      }
      randstr_r(value, valuesize, &vseed);
      valuelen = valuesize;
    }

    //----------------------------------------------------------------------
    // just print <key, value> to stdout, do not insert into db
    //----------------------------------------------------------------------
    if (print_kv_and_continue) {
      cout << key << " " << value << endl;
      continue;
    }

    //----------------------------------------------------------------------
    // insert <key, value> into db
    //----------------------------------------------------------------------
    if (print_periodic_stats) {
      gettimeofday(&start, NULL);
    }

    leveldb::Status s = db->Put(targs->write_options, key, value);
    bytes_inserted += keylen + valuelen;
    if (!s.ok()) {
      printf("Error in Put(%s): %s\n", key, s.ToString().c_str());
    }

    if (print_periodic_stats) {
      uint32_t latency;
      gettimeofday(&end, NULL);
      latency = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
      update_put_stats(latency);
    }
  }

  put_thread_finished = true;

  free(key);
  free(value);
  pthread_exit(NULL);
}

/*============================================================================
 *                                get_routine
 *============================================================================*/
void *get_routine(void *args) {
  struct thread_args *targs = (struct thread_args *)args;
  int    uflag = targs->uflag,
       zipf_keys = targs->zipf_keys,
       range_get_size = targs->range_get_size;
  double   zipf_param = targs->zipf_param;
  bool   print_periodic_stats = targs->collect_stats;
  uint32_t keysize = targs->keysize, keylen;
  uint32_t kseed = targs->tid;  // kseed = getpid() + time(NULL);
  char   key[MAX_KEY_SIZE + 1];
  int    i = -1;
  RequestThrottle throttler(targs->get_thrput);
  struct timeval start, end;

  while (!put_thread_finished) {

    //--------------------------------------------------------------
    // throttle request rate
    //--------------------------------------------------------------
    throttler.throttle();

    //--------------------------------------------------------------
    // create a random key
    //--------------------------------------------------------------
    if (zipf_keys) {
      zipfstr_r(key, keysize, zipf_param, &kseed);
      keylen = keysize;
    } else {
      randstr_r(key, keysize, &kseed);
      keylen = keysize;
    }
    if (uflag) {
      // TODO: sprintf(key, "%s", key) -> undefined behaviour!
      sprintf(key, "%s#%d", key, ++i);
      keylen += 1 + numdigits(i);
    }

    //--------------------------------------------------------------
    // execute range get() or point get()
    //--------------------------------------------------------------
    if (print_periodic_stats) {
      gettimeofday(&start, NULL);
    }
    if (range_get_size == 0) {
      //--------------------------------------------------------------
      // point get()
      //--------------------------------------------------------------
      std::string value;
      leveldb::Status s = db->Get(targs->read_options, key, &value);
      if (!s.ok() && !s.IsNotFound()) {
        printf("Error in Get(%s): %s\n", key, s.ToString().c_str());
      }
    } else {
      //--------------------------------------------------------------
      // range get()
      //--------------------------------------------------------------
      leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
      int i = 0;
      for (it->Seek(key); it->Valid() && i < range_get_size; it->Next()) {
        i++;
      }
      assert(it->status().ok());
      delete it;
    }

    if (print_periodic_stats) {
      uint32_t latency;
      gettimeofday(&end, NULL);
      latency = (end.tv_sec - start.tv_sec)*1000000 + (end.tv_usec - start.tv_usec);
      update_get_stats(targs->tid, latency);
    }
  }

  pthread_exit(NULL);
}

/*============================================================================
 *                            update_put_stats
 *============================================================================*/
void update_put_stats(uint32_t latency) {
  pthread_mutex_lock(&put_latency_mutex);
  put_latency[latency]++;
  pthread_mutex_unlock(&put_latency_mutex);
}

/*============================================================================
 *                            update_get_stats
 *============================================================================*/
void update_get_stats(int tid, uint32_t latency) {
  pthread_mutex_lock(&get_latency_mutex[tid - 1]);
  get_latency[tid - 1][latency]++;
  pthread_mutex_unlock(&get_latency_mutex[tid - 1]);
}

/*============================================================================
 *                            print_put_get_stats
 *============================================================================*/
void print_put_get_stats(int signum) {
  static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  static uint64_t old_time = 0;
  static int first_time = 1;
  uint64_t cur_time;
  uint32_t psum, pcount, pmax, pmin;
  uint32_t gsum, gcount, gmax, gmin;
  float pavg, gavg, pstd, gstd;
  float sec_lapsed;
  uint32_t tmp_count;
  std::map<uint32_t, uint32_t>::iterator it;
  std::map<uint32_t, uint32_t> pperc, gperc;
  std::ostringstream buf;

  // needed in some architectures
  if (signal(SIGALRM, print_put_get_stats) == SIG_ERR) {
    perror("Could not set signal handler");
    exit(EXIT_FAILURE);
  }

  // if mutex locked, signal was caught while last signal handling is not
  // completed: return immediately (we'll catch next signal)
  if (pthread_mutex_trylock(&mutex) != 0) {
    return;
  }

  //----------------------------------------------------------------------------
  // lock in order to exclusively access stats
  //----------------------------------------------------------------------------
  pthread_mutex_lock(&put_latency_mutex);
  for (int i = 0; i < num_get_threads; i++) {
    pthread_mutex_lock(&get_latency_mutex[i]);
  }

  //----------------------------------------------------------------------------
  // collect/calculate get stats
  //----------------------------------------------------------------------------
  gsum = gcount = gavg = 0;
  gmin = put_latency.begin()->first;
  gmax = put_latency.begin()->first;

  // sum, count, min, max
  for (int i = 0; i < num_get_threads; i++) {
    for (it = get_latency[i].begin(); it != get_latency[i].end(); ++it) {
      uint32_t latency = it->first, count = it->second;
      gsum += latency * count;
      gcount += count;
      gmin = min(gmin, latency);
      gmax = max(gmax, latency);
    }
  }

  // avg
  if (gcount) {
    gavg = gsum / (float)gcount;
  }

  // merge all maps in a single map
  std::map<uint32_t, uint32_t> tmp_map;
  for (int i = 0; i < num_get_threads; i++) {
    for (it = get_latency[i].begin(); it != get_latency[i].end(); ++it) {
      tmp_map[it->first] += it->second;
    }
  }

  // median, 90% perc, 95%, 99% perc, 99.9% perc, std
  tmp_count = 0;
  gstd = 0;
  for (it = tmp_map.begin(); it != tmp_map.end(); ++it) {
    uint32_t latency = it->first, count = it->second;
    tmp_count += count;
    // (a-x)^2 + ... + (a-x)^2 = n * (a-x)^2
    gstd += count * powl(latency - gavg, 2);
    if (gperc.find(500) == gperc.end() && tmp_count >= 0.500 * gcount) { gperc[500] = latency; }
    if (gperc.find(900) == gperc.end() && tmp_count >= 0.900 * gcount) { gperc[900] = latency; }
    if (gperc.find(950) == gperc.end() && tmp_count >= 0.950 * gcount) { gperc[950] = latency; }
    if (gperc.find(990) == gperc.end() && tmp_count >= 0.990 * gcount) { gperc[990] = latency; }
    if (gperc.find(999) == gperc.end() && tmp_count >= 0.999 * gcount) { gperc[999] = latency; }
  }
  if (gcount) {
    gstd = sqrt(gstd / gcount);
  }

  //----------------------------------------------------------------------------
  // reset get stats
  //----------------------------------------------------------------------------
  for (int i = 0; i < num_get_threads; i++) {
    get_latency[i].clear();
  }

  //----------------------------------------------------------------------------
  // collect/calculate put stats
  //----------------------------------------------------------------------------
  psum = pcount = pavg = 0;
  pmin = put_latency.begin()->first;
  pmax = put_latency.begin()->first;

  // sum, count, min, max
  for (it = put_latency.begin(); it != put_latency.end(); ++it) {
    uint32_t latency = it->first, count = it->second;
    psum += latency * count;
    pcount += count;
    pmin = min(pmin, latency);
    pmax = max(pmax, latency);
  }

  // avg
  if (pcount) {
    pavg = psum / (float)pcount;
  }

  // median, 90% perc, 95% perc, 99% perc, 99.9% perc, std
  tmp_count = 0;
  pstd = 0;
  for (it = put_latency.begin(); it != put_latency.end(); ++it) {
    uint32_t latency = it->first, count = it->second;
    tmp_count += count;
    // (a-x)^2 + ... + (a-x)^2 = n * (a-x)^2
    pstd += count * powl(latency - pavg, 2);
    if (pperc.find(500) == pperc.end() && tmp_count >= 0.500 * pcount) { pperc[500] = latency; }
    if (pperc.find(900) == pperc.end() && tmp_count >= 0.900 * pcount) { pperc[900] = latency; }
    if (pperc.find(950) == pperc.end() && tmp_count >= 0.950 * pcount) { pperc[950] = latency; }
    if (pperc.find(990) == pperc.end() && tmp_count >= 0.990 * pcount) { pperc[990] = latency; }
    if (pperc.find(999) == pperc.end() && tmp_count >= 0.999 * pcount) { pperc[999] = latency; }
  }
  if (pcount) {
    pstd = sqrt(pstd / pcount);
  }

  //----------------------------------------------------------------------------
  // reset put stats
  //----------------------------------------------------------------------------
  put_latency.clear();

  //----------------------------------------------------------------------------
  // calculate time passed since last call
  //----------------------------------------------------------------------------
  cur_time = leveldb::Env::Default()->NowMicros() / 1000;
  if (old_time == 0) {
    sec_lapsed = 0; //DEFAULT_STATS_PRINT_INTERVAL;
  } else {
    sec_lapsed = (cur_time - old_time) / 1000.0;
  }
  old_time = cur_time;

  //----------------------------------------------------------------------------
  // print stats to stderr
  //----------------------------------------------------------------------------
  buf.str("");
  if (first_time) {
    first_time = 0;
    if (num_get_threads) {
      buf << "[GET_HEADER] (timestamp) (sec_lapsed) (count) (sum) (avg) (min) (med) (90p) (95p) (99p) (99.9p) (max) (std)" << endl;
    }
    buf << "[PUT_HEADER] (timestamp) (sec_lapsed) (count) (sum) (avg) (min) (med) (90p) (95p) (99p) (99.9p) (max) (std) (bytes_ins)" << endl;
  }
  if (num_get_threads) {
    buf << "[GET_STATS] " << cur_time << " " << sec_lapsed << " " << gcount << " " << gsum << " " << (int)gavg << " "
        << gmin << " " << gperc[500] << " " << gperc[900] << " " << gperc[950] << " " << gperc[990] << " " << gperc[999] << " " << gmax << " " << gstd << endl;
  }
  buf << "[PUT_STATS] " << cur_time << " " << sec_lapsed << " " << pcount << " " << psum << " " << (int)pavg << " "
      << pmin << " " << pperc[500] << " " << pperc[900] << " " << pperc[950] << " " << pperc[990] << " " << pperc[999] << " " << pmax << " " << pstd << " "
      << bytes_inserted << endl;

  cerr << buf.str() << flush;

  //----------------------------------------------------------------------------
  // resume threads
  //----------------------------------------------------------------------------
  pthread_mutex_unlock(&put_latency_mutex);
  for (int i = 0; i < num_get_threads; i++) {
    pthread_mutex_unlock(&get_latency_mutex[i]);
  }

  pthread_mutex_unlock(&mutex);
}

/*============================================================================
 *                               randstr_r
 *============================================================================*/
void randstr_r(char *s, const int len, uint32_t *seed) {
  static const char alphanum[] =
//     "0123456789"
//    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
  int size = sizeof(alphanum);

  assert(len >= 0);

  for (int i = 0; i < len; i++) {
    s[i] = alphanum[rand_r(seed) % (size - 1)];
  }

  s[len] = '\0';
}

/*============================================================================
 *                               zipfstr_r
 *============================================================================*/
void zipfstr_r(char *s, const int len, double zipf_param, uint32_t *seed) {
  static int num_digits = (int)log10(ZIPF_MAX_NUM) + 1;
  static char key_prefix[MAX_KEY_SIZE + 1];
  static bool first = true;

  if (first) {
    first = false;
    // key prefix must be common for all keys to follow zipf distribution
    randstr_r(key_prefix, len - num_digits, seed);
  }

  sprintf(s, "%s%0*d", key_prefix, num_digits, zipf_r(zipf_param, seed));
}

/*============================================================================
 *                               orderedstr_r
 *============================================================================*/
void orderedstr_r(char *s, const int len, uint32_t *seed) {
  static int num_digits = (int)log10(ULONG_MAX) + 1;
  static char key_prefix[MAX_KEY_SIZE + 1];
  static bool first = true;

  if (first) {
    first = false;
    // key prefix must be common for all keys
    randstr_r(key_prefix, len - num_digits, seed);
  }

  sprintf(s, "%s%0*u", key_prefix, num_digits, (*seed)++);
}

/*
 * code below from:
 *  http://www.csee.usf.edu/~christen/tools/toolpage.html
 * code was modified to precompute sum of probabilities and use integers
 * instead of doubles
 */

/*============================================================================
 *                                 zipf_r
 *============================================================================*/
int zipf_r(double zipf_param, uint32_t *seed) {
  static int *sum_prob = NULL;  // sum of probabilities
  int z,            // uniform random number (0 <= z <= RAND_MAX)
    zipf_value,         // computed exponential value to be returned
    i,
    first, last, mid;     // for binary search

  // compute sum of probabilities on first call only
  if (sum_prob == NULL) {
    double *sum_prob_f;
    double c = 0;       // normalization constant

    for (i = 1; i <= ZIPF_MAX_NUM; i++) {
      c = c + (1.0 / pow((double) i, zipf_param));
    }
    c = 1.0 / c;

    // precompute sum of probabilities
    sum_prob_f = (double *)malloc((ZIPF_MAX_NUM + 1) * sizeof(*sum_prob_f));
    sum_prob_f[0] = 0;
    for (i = 1; i <= ZIPF_MAX_NUM; i++) {
      sum_prob_f[i] = sum_prob_f[i-1] + c / pow((double) i, zipf_param);
    }

    // from array of doubles sum_prob_f[] that contains values in range
    // [0,1], compute array of integers sum_prob_i[] that contains values
    // in range [0,RAND_MAX]
    sum_prob = (int *)malloc((ZIPF_MAX_NUM + 1) * sizeof(*sum_prob));
    for (i = 0; i <= ZIPF_MAX_NUM; i++) {
      sum_prob[i] = (int)(sum_prob_f[i] * RAND_MAX);
    }
  }

  // pull a uniform random number (0 <= z <= RAND_MAX)
  z = rand_r(seed);

  // map z to the value (find the first 'i' for which sum_prob[i] >= z)
  first = 1;
  last = ZIPF_MAX_NUM;
  while (first <= last) {
    mid = (last - first)/2 + first;  // avoid overflow
    if (z > sum_prob[mid]) {
      first = mid + 1;
    } else if (z < sum_prob[mid]) {
      last = mid - 1;
    } else {
      break;
    }
  }

  if (sum_prob[mid] >= z) {
    zipf_value = mid;
  } else {
    zipf_value = mid + 1;
  }

  // assert that zipf_value is between 1 and N
  assert((zipf_value >= 1) && (zipf_value <= ZIPF_MAX_NUM));

  return (zipf_value);
}

/*============================================================================
 *                     check_duplicate_arg_and_set
 *============================================================================*/
void check_duplicate_arg_and_set(int *flag, int opt) {
  if (*flag) {
    cerr << "Error: you have already set '-" << (char)opt << "' argument" << endl;
    exit(EXIT_FAILURE);
  }
  *flag = 1;
}

/*============================================================================
 *                              numdigits
 *============================================================================*/
int numdigits(uint64_t num) {
  int digits = 0;

  if (num < 10) return 1;
  if (num < 100) return 2;
  if (num < 1000) return 3;
  if (num < 10000) return 4;
  if (num < 100000) return 5;
  if (num < 1000000) return 6;
  if (num < 10000000) return 7;
  if (num < 100000000) return 8;
  if (num < 1000000000) return 9;

  do {
    num /= 10;
    ++digits;
  } while (num > 0);

  return digits;
}

/*============================================================================
 *                                round
 *============================================================================*/
double round(double r) {
  return (r > 0.0) ? floor(r + 0.5) : ceil(r - 0.5);
}
