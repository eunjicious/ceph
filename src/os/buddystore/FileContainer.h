
#ifndef CEPH_FILECONTAINEROBJECTSTORE_H
#define CEPH_FILECONTAINEROBJECTSTORE_H

//#include <mutex>
#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "common/RWLock.h"
#include "common/Finisher.h"
//#include "BuddyStore.h"
#include "include/compat.h"
#include "buddy_types.h"
#include "os/ObjectStore.h"

#define SYS_PREFIX "sys_prefix"
#define SYS_KEY "sys_key"

// FileContainer 는 write thread 가 돌면서 writeq 에 들어온 request 를 처리함. 
// buddy_iov_t 형태로 들어온 request 임. 
// BuddyStore 는 queue_op 대신 osr 을 buddy_iov_t 로 (바꾸든지 뽑아내든지) 
// BuddyStore 는 op_journal_transaction 다음에 fc 에 submit_entry 할 수 있는 코드 넣기. 
// op_container_write 로 하자 :



class FileContainer{
  CephContext* cct; 
  ObjectStore* store;
  string basedir;
  string meta_fn;
  string prefix_fn;
  //map<uint64_t, size_t> file_tail_map; // <file_seq, tail_off>
  //map<uint64_t, int> file_fd_map; // <file_seq, fd>

 // file 공간 할당받고 buffer align 맞추는 함수  
  Mutex fc_lock;
  uint64_t file_seq; // 0 is invalid 
  uint64_t curr_tail_off; 
  //int curr_wfd; 
  map<string, int> fd_map;

  // configs 
  bool directio; 
  bool prewrite; 

  
//  string fname;

public:
  // stat
  uint64_t total_alloc_bytes;
  uint64_t total_stored_bytes;
 
  Finisher fc_finisher;
  //Finisher* finisher;

  //--------- write thread  -----------// 
  void write_thread_entry();
  class ContainerWriteThread : public Thread {
	FileContainer *fc;
	public:
	explicit ContainerWriteThread(FileContainer *fc_) : fc(fc_) {}
	void *entry() override {
	  fc->write_thread_entry();
	  return 0;
	}
  } write_thread;
  bool write_stop;

private:
  KeyValueDB* free_extent_map_db;
  KeyValueDB* oxt_map_db; // ooff, <foff, bytes> 
  string extent_map_backend;
  string extent_map_dir;
  //map<string, int> file_map; // fname, fd

public:
  //--------- writeq -----------// 
  struct completion_item {
    uint64_t seq;
    Context *finish;
    utime_t start;
    TrackedOpRef tracked_op;
    completion_item(uint64_t o, Context *c, utime_t s,
		    TrackedOpRef opref)
      : seq(o), finish(c), start(s), tracked_op(opref) {}
    completion_item() : finish(0), start(0) {}
  };

  struct write_item {
    uint64_t seq;
	vector<buddy_iov_t> iov;
    //bufferlist bl;
    //uint32_t orig_len;
    TrackedOpRef tracked_op;

    //write_item(uint64_t s, bufferlist& b, int ol, TrackedOpRef opref) :
    write_item(uint64_t s, vector<buddy_iov_t>& iov, TrackedOpRef opref) :
      seq(s), iov(iov), tracked_op(opref) {
      //bl.claim(b, buffer::list::CLAIM_ALLOW_NONSHAREABLE); // potential zero-copy
    }
    write_item() {}
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
  void queue_writeq(write_item it) {
	Mutex::Locker l(writeq_lock);
	writeq.push_back(it);
  }

#if 0
  //OpSequencer* &peek_writeq(){ // front 
  write_item* &peek_writeq(){ // front 
	Mutex::Locker l(writeq_lock);
	assert(!writeq.empty());
	return writeq.front();
  }

  void pop_writeq(){ // front -- not used 
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
#endif

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
  Mutex fc_finisher_lock;
  Cond fc_finisher_cond;
  //  uint64_t journaled_seq;
  //  bool plug_journal_completions;

  // lock ordering 
  // writeq_lock --> fc_finisher_lock --> completions_lock 
  
  //--- utilities ---//
  string ghobject_key(const ghobject_t &oid);


  int _save();
  int _load();
  int _create_or_open_file(string fname, int flags);
  int _set_tail_off(int fd);
  void _close_file_all();

  int _write(buddy_iov_t& iov);
  int _read(int fd, buddy_iov_t& iov);

  int _alloc_space(coll_t cid, const ghobject_t& oid, const off_t ooff, 
	  const ssize_t bytes, vector<buddy_iov_t>& iov);

public:

  // start, stop  
  int mkfc();
  void mount();
  void umount();

  uint64_t writing_seq, written_seq; // protected by finisher_lock

  // submit_manager 
  class SubmitManager {
	CephContext* cct;
	Mutex lock; 
	//int writing_seq, written_seq, 
	uint64_t op_seq, submitted_seq;
  public:
	uint64_t submit_start() { 
	  lock.Lock();
	  uint64_t op = ++op_seq;
	  return op;
	}
	void submit_finish(uint64_t seq) { 
	  submitted_seq = seq;
	  lock.Unlock();
	}
	  //cct(cct), lock("FC:SubmitManager::lock", false, true, false, cct), 
	SubmitManager(CephContext* cct) : 
	  cct(cct), 
	  lock("FC:SubmitManager::lock"), 
	  op_seq(0), submitted_seq(0) {}
	  //writing_seq(0), written_seq(0), op_seq(0), submitted_seq(0) {}
	~SubmitManager() {}
  } submit_manager;

  int prepare_write(vector<ObjectStore::Transaction> &tls, vector<buddy_iov_t>& iov); 

  void submit_entry(uint64_t seq, vector<buddy_iov_t>& iov, Context* onfcwrite, TrackedOpRef osd_op = TrackedOpRef()); 
  void queue_completions_thru(uint64_t written_seq);
  void punch_hole_map_update(ObjectStore::Transaction& tr, uint32_t punch_hole_off, vector<buddy_iov_t>& iov); // add map info to tr.

  int oxt_map_update (vector<buddy_iov_t>& iov);
  int oxt_map_single_update(buddy_iov_t& iov);
  int oxt_map_lookup(const coll_t& cid, const ghobject_t& oid, uint64_t off, uint64_t len, vector<buddy_iov_t>& iov);

  int sync(); 
  // object operations 
  uint64_t get_size(const coll_t& cid, const ghobject_t& oid);

  int read(const coll_t& cid, const ghobject_t& oid, uint64_t offset, uint64_t len, bufferlist &bl);
  int remove(const coll_t& cid, const ghobject_t& oid);
  int write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, const bufferlist &bl){return 0;};
  int clone(const coll_t& cid, const ghobject_t& src, const ghobject_t& dest, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int truncate(const coll_t& cid, const ghobject_t& oid, uint64_t bytes);

  void encode(bufferlist& bl) const 
  {
	ENCODE_START(1, 1, bl);
	//::encode(log_index_map, bl);
	::encode(prefix_fn, bl);
	::encode(file_seq, bl);
	ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p)
  {
	DECODE_START(1, p);
	::decode(prefix_fn, p);
	::decode(file_seq, p);
	DECODE_FINISH(p);
  }

  explicit FileContainer(CephContext *cct_, const string& basedir_, ObjectStore* store_, bool dio=true):
	cct(cct_), 
	store(store_),
	basedir(basedir_), 
	meta_fn(basedir_ + "/container.meta"),
	prefix_fn(basedir_ + "/container.data"),
	fc_lock("FC::lock"),
	file_seq(0),
	curr_tail_off(0),
	directio(dio),
	prewrite(cct->_conf->buddystore_file_prewrite),
	total_alloc_bytes(0),
	total_stored_bytes(0),
	fc_finisher(cct, "BuddyStore", "fn_bd_fc"),
	write_thread(this), 
	write_stop(false),
	extent_map_backend("leveldb"),
	extent_map_dir(basedir_ + "/cotainer_map"),
	writeq_lock("FC::writeq_lock"),
	completions_lock("FC::completions_lock"),
	fc_finisher_lock("FC::finisher_lock"),
	writing_seq(0), written_seq(0),
	submit_manager(cct_)
  {}
  //FileContainer(){}
  ~FileContainer() {}
};

#endif


