
#ifndef CEPH_FILECONTAINEROBJECTSTORE_H
#define CEPH_FILECONTAINEROBJECTSTORE_H


#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "common/RWLock.h"
//#include "BuddyStore.h"
#include "include/compat.h"
#include "buddy_types.h"


class FileContainer{
public:
  Finisher* finisher;
  CephContext *cct;

  //--------- write thread  -----------// 
  class ContainerWriteThread : public Thread {
	FileContainer *fc;
	public:
	explicit ContainerWriteThread(FileContainer *fc_) : fc(fc_) {}
	void *entry() override {
	  fc->write_thread_entry();
	  return 0;
	}
  } write_thread;


private:
  KeyValueDB* free_extent_map;
  KeyValueDB* object_extent_map; // ooff, <foff, bytes> 
  map<string, int> file_map; // fname, fd

  //--------- writeq -----------// 
  struct completion_item {
    uint64_t seq;
    Context *finish;
    utime_t start;
    TrackedOpRef tracked_op;
    completion_item(uint64_t o, Context *c, utime_t s,
		    TrackedOpRef opref)
      : seq(o), finish(c), start(s), tracked_op(opref) {}
    completion_item() : seq(0), finish(0), start(0) {}
  };

  struct write_item {
    uint64_t seq;
	vector<buddy_iov_t> iov;
    //bufferlist bl;
    //uint32_t orig_len;
    TrackedOpRef tracked_op;


    write_item(uint64_t s, bufferlist& b, int ol, TrackedOpRef opref) :
      seq(s), orig_len(ol), tracked_op(opref) {
      bl.claim(b, buffer::list::CLAIM_ALLOW_NONSHAREABLE); // potential zero-copy
    }
    write_item() : seq(0), orig_len(0) {}
  };

  //--------- writeq -----------// 
  Mutex writeq_lock;
  Cond writeq_cond;
  //list<OpSequencer*> writeq;
  list<write_item> writeq;


  bool writeq_empty() {
	Mutex::Locker l(writeq_lock);
	return writeq.empty();
  }

  //void queue_writeq(OpSequencer* osr) {
  void queue_writeq(write_item* it) {
	Mutex::Locker l(writeq_lock);
	writeq.push_back(it);
  }

  //OpSequencer* &peek_writeq(){ // front 
  write_item* &peek_writeq(){ // front 
	Mutex::Locker l(writeq_lock);
	assert(!writeq.empty());
	return writeq.front();
  }

  void pop_writeq(){ // front 
	Mutex::Locker l(writeq_lock);
	assert(!writeq.empty());
	writeq.pop_front();
  }
  void batch_pop_writeq(list<write_item*> &items){// 전체 다 가져오기 
	Mutex::Locker l(writeq_lock); 
	writeq.swap(items); 
  }
  void batch_unpop_writeq(list<write_item*> &items){
	Mutex::Locker l(writeq_lock);
	writeq.splice(writeq.begin(), items);
  }

  //--------- completionq -----------// 
  Mutex completions_lock;
  list<completion_item> completions;
  bool completions_empty() {
    Mutex::Locker l(completions_lock);
    return completions.empty();
  }
  void batch_pop_completions(list<completion_item> &items) {
    Mutex::Locker l(completions_lock);
    completions.swap(items);
  }
  void batch_unpop_completions(list<completion_item> &items) {
    Mutex::Locker l(completions_lock);
    completions.splice(completions.begin(), items);
  }
  completion_item completion_peek_front() {
    Mutex::Locker l(completions_lock);
    assert(!completions.empty());
    return completions.front();
  }
  void completion_pop_front() {
    Mutex::Locker l(completions_lock);
    assert(!completions.empty());
    completions.pop_front();
  }

  //--------- finisher -----------// 
  Mutex finisher_lock;
  Cond finisher_cond;
  //  uint64_t journaled_seq;
  //  bool plug_journal_completions;



  public:
	int prepare_write(vector<Transaction> &tls, vector<buddy_iov_t>& iov);
	int transaction_update(vector<Transaction> &tls, vector<buddy_iov_t>& iov); // add map info to tr.
	int submit_entry(vector<buddy_iov_t>& iov, Context *onwrite, TrackedOpRef osd_op = TrackedOpRef());
	int sync();

	virtual int op_container_write(); // 여기에서 submit entry 불러줘야 하는데 이건 virtual 





	FileContainer() {}
	~FileContainer() {}

} file_container;




class FileContainerObjectStore {
  string fc_path;

#if 0
  class FreeSpaceManager {
	// yuil 

  } free_space_manager;

  int _read_fd(int fd, uint64_t foff, size_t bytes, bufferlist& bl) {return 0;}
 
#endif

public:
  FileContainerObject (string _path) : fc_path(_path) {}
  fc_submit_entry();
  


#if 0

  ///void _op_journal_transactions(bufferlist& tls, uint32_t orig_len, uint64_t op,
	//			Context *onjournal, TrackedOpRef osd_op);

  int read_object (const ghobject_t& oid, const off_t ooff, const ssize_t bytes, bufferlist& dst_bl);
//  int write_object (const ghobject_t& oid, const off_t ooff, const ssize_t bytes, bufferlist& src_bl); // similar to submit_entry
//  void _data_write_transactions 

  int object_extent_map_update (const ghobject_t& oid, const off_t ooff, const off_t foff, const string& fname) {return 0;}
#endif




};

#if 0

#define BUDDY_PREALLOC_SIZE 1UL << 35 // 16M
//#define BUDDY_PREALLOC_SIZE 1UL << 28 // 16M
//#define BUDDY_INIT_ALLOC_SIZE 1UL << 30 // 1G 
#define BUDDY_SLOT_NUM 11
#define PUNCH_HOLE_DATA_FILE

const static uint64_t BUDDY_INIT_ALLOC_SIZE(1UL << 30);


class BuddyLogDataFileObject {

  public:
    CephContext* cct;

    // file information 
    string fname;
    bool directio;
    int type;

    int dfd;
    off_t tail_off;
	off_t total_pool_bytes;
	off_t total_reserved_bytes;
    off_t total_used_bytes;
    off_t total_alloc_bytes;
    off_t prealloc_bytes;
    off_t prewrite_unit_bytes;

    bool file_prewrite;
    bool file_prealloc; 
    bool file_inplace_write;
	bool bg_reclaim;


	Mutex slot_lock;
	vector<ssize_t> slot_used_bytes;
	vector<ssize_t> slot_limit_bytes;

	double get_slot_util(int i){

	  double d = static_cast<double>(slot_limit_bytes[i]);
	  double n = static_cast<double>(slot_used_bytes[i]);
	  return n / d;
	}

	double low_util_ratio;
	double high_util_ratio;

 
	class PunchHoleThread : public Thread {
	  BuddyLogDataFileObject *fo;
	public:
	  explicit PunchHoleThread(BuddyLogDataFileObject *fo_) : fo(fo_) {}
	  void *entry() override {
		fo->punch_hole_thread_entry();
		return 0;
	  }
	} punch_hole_thread;


	void punch_hole_thread_entry();
	Mutex punch_hole_lock;
	bool force_punch_hole;
	bool stop_punch_hole;
	Cond punch_hole_cond; 

	class PreWriteThread : public Thread {
	  BuddyLogDataFileObject *fo;
	public:
	  explicit PreWriteThread(BuddyLogDataFileObject *fo_) : fo(fo_) {}
	  void *entry() override {
		fo->prewrite_thread_entry();
		return 0;
	  }
	} prewrite_thread;

	void prewrite_thread_entry();
	Mutex prewrite_lock;
	bool force_prewrite;
	bool stop_prewrite;
	Cond prewrite_cond; 

	int do_prewrite();


  private:
    // space management 
    uint64_t max_fbytes;
    uint64_t used_fbytes;

  public:
    Mutex lock; 
    //RWLock lock; // log_index_map lock 
    //map<ghobject_t, buddy_index_map_t> log_index_map;
    map<off_t, ssize_t> free_index_map; // foff. not ooff!!!!

    int create_or_open_file(int flag);
    int delete_file();
    int close_file();
    void stat_file();
	void _stat_file();

    int alloc_space(coll_t cid, const ghobject_t& oid, const off_t ooff, const ssize_t bytes, 
      vector<buddy_iov_t>& iov);
    //int get_space_info(const ghobject_t& oid, const off_t ooff, const ssize_t bytes,
     //  vector<buddy_iov_t>& iov);
    // @return: -1 on fail 

    int release_space(const buddy_index_map_t& omap); 

    int truncate_space(const ghobject_t& oid, ssize_t size) {return 0;}
    int clone_space(const ghobject_t& ooid, const ghobject_t& noid, vector<buddy_iov_t>& iov){return 0;}
    int clone_space(const ghobject_t& ooid, const ghobject_t& noid, off_t srcoff, size_t bytes, 
	off_t dstoff, vector<buddy_iov_t>& iov) {return 0;}
//    int write(bufferlist& bl, uint64_t foff);
    int write_fd(bufferlist& bl, uint64_t foff);
//    int write_fd(bufferlist& bl, uint64_t foff, int fd);
    void sync();

    //int read_fd(const ghobject_t& oid, bufferlist& bl, uint64_t foff, ssize_t size);
    int read(bufferlist& bl, uint64_t foff, size_t size);
    int read_fd(bufferlist& bl, uint64_t foff, size_t size);
    int read_fd(bufferlist& bl, uint64_t foff, size_t size, int fd);
    int preallocate(uint64_t offset, size_t len);

  

    void encode(bufferlist& bl) const 
    {
      ENCODE_START(1, 1, bl);
      //::encode(log_index_map, bl);
      ::encode(tail_off, bl);
	  ::encode(total_pool_bytes, bl);
	  ::encode(total_reserved_bytes, bl);
      ::encode(total_used_bytes, bl);
      ::encode(total_alloc_bytes, bl);
	  ::encode(slot_used_bytes, bl);
	  ::encode(slot_limit_bytes, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p)
    {
      DECODE_START(1, p);
      //::decode(log_index_map, p);
      //::decode(tail_off, p);
      ::decode(tail_off, p);
	  ::decode(total_pool_bytes, p);
	  ::decode(total_reserved_bytes, p);
      ::decode(total_used_bytes, p);
      ::decode(total_alloc_bytes, p);
	  ::decode(slot_used_bytes, p);
	  ::decode(slot_limit_bytes, p);
      DECODE_FINISH(p);
    }
  
    BuddyLogDataFileObject(CephContext* cct_, string fname_, bool dio, bool prealloc);
 
    ~BuddyLogDataFileObject(){
    }
};


WRITE_CLASS_ENCODER(BuddyLogDataFileObject)

#endif
#endif
