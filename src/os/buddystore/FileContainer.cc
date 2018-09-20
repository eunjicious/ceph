#include "FileContainer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfc " 

const static int CEPH_DIRECTIO_ALIGNMENT(4096);
const static int MIN_PUNCH_HOLE_SIZE(8192);

static void append_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == '_') {
      out->push_back('%');
      out->push_back('u');
    } else {
      out->push_back(*i);
    }
  }
}

static string uint64_to_str_key (uint64_t num)
{
  uint64_t s = 1;
  uint64_t n = log10((s << 63) + (s << 63) - 1);
  string key = to_string(num);
  while ( n > key.length()) 
	key = "0" + key;
  return key;
}

//----- mkfc -----//
int FileContainer::mkfc()
{
  dout(3) << __func__ << dendl;
  _save(); 

  // open current data_file 
  string fname = prefix_fn + to_string(file_seq); 
  int fd = _create_or_open_file(fname, O_DIRECT | O_SYNC | O_CREAT); // flags 
  if (fd < 0) {
	dout(1) << __func__ << " Failed to create file" << dendl;
	return fd;
  }
  dout(3) << __func__ << " container file is created with fd = " << fd << dendl;

  int ret = _set_tail_off(fd);
  if (ret < 0) {
	dout(1) << __func__ << " Failed to set tail off" << dendl;
	return ret;
  }

  return 0;
}

int FileContainer::_save()
{
  dout(3) << __func__ << dendl;
  // encode metadata and store 
  bufferlist mbl;
  encode(mbl);
  int ret = mbl.write_file(meta_fn.c_str());
  if (ret < 0) {
	dout(3) << "Failed to write fc_meta" << dendl;
  }
  return ret;
}

int FileContainer::_load()
{
  dout(3) << __func__ << dendl;

  // load metadata and decode 
  bufferlist mbl;
 
  string err;
  int ret = mbl.read_file(meta_fn.c_str(), &err);
  if (ret < 0) {
	dout(3) << "Failed to read fc_meta" << dendl;
	return ret;
  }

  bufferlist::iterator bp = mbl.begin();
  decode(bp);
  return ret;
}

int FileContainer::_create_or_open_file(string fname, int out_flags)
{
  dout(3) << __func__ << " fname " << fname << dendl;

  // check fd_map 
  auto p = fd_map.find(fname);
  if (p != fd_map.end())
	return p->second;

  // open 
  int flags = O_RDWR;
  flags |= out_flags;
    
  //flags |= O_DIRECT | O_DSYNC;

  // open 
  int fd = ::open(fname.c_str(), flags, 0644);
  if (fd < 0){
    dout(3) << __func__ << " Failed to create file: " << fname << cpp_strerror(fd) << dendl; 
    return fd;
  }

  fd_map.insert(make_pair(fname, fd));
  return fd;

}

int FileContainer::_set_tail_off(int fd)
{
  dout(3) << __func__ << dendl;

  struct stat st;
  int r = ::fstat(fd, &st);
    
  if (r < 0) {
    dout(3) << __func__ << " File is not open" << dendl; 
    return r;
  }

  dout(3) << __func__ << " data_file size " << st.st_size << dendl;

  curr_tail_off = st.st_size;
  return r;
}


void FileContainer::_close_file_all()
{
  dout(3) << __func__ << dendl;

  for(auto p = fd_map.begin(); p != fd_map.end(); ++p) {
	// create or open file 
	int r = close(p->second);

	if (r < 0) {
	  dout(3) << __func__ << " Failed to close a file" << dendl;
	}
  }
}


//----- mount ----//
void FileContainer::mount()
{
  dout(3) << __func__ << dendl;

  _load();

  dout(3) << __func__ << " mkdir " << extent_map_dir.c_str() << dendl;
  int ret = ::mkdir(extent_map_dir.c_str(), 0777);
  if(ret < 0){
	dout(3) << "mkdir return = " << ret << dendl;
  }

  // kvstore for extent map 
  oxt_map_db = KeyValueDB::create(cct, extent_map_backend, extent_map_dir);

  if(oxt_map_db == NULL) {
	dout(3) << "Failed to oxt_map " << dendl; 
	return;
  }

  if (extent_map_backend == "rocksdb")
    ret = oxt_map_db->init(cct->_conf->filestore_rocksdb_options);
  else
    ret = oxt_map_db->init();

  if (ret < 0) {
    derr << "Error initializing oxt_map: " << cpp_strerror(ret) << dendl;
	return;
  }
    
  stringstream err;
  if (oxt_map_db->create_and_open(err)) {
      delete oxt_map_db;
      derr << "Error initializing " << extent_map_backend
	   << " : " << err.str() << dendl;
      ret = -1;
	  return;
  }
  dout(3) << "Success open oxt_map" << dendl;


  // kvstore for free extent map
  free_extent_map_db = KeyValueDB::create(cct, extent_map_backend, free_extent_map_dir);

  if(free_extent_map_db == NULL) {
	dout(3) << "Failed to oxt_map " << dendl; 
	return;
  }

  if (extent_map_backend == "rocksdb")
    ret = free_extent_map_db->init(cct->_conf->filestore_rocksdb_options);
  else
    ret = free_extent_map_db->init();

  if (ret < 0) {
    derr << "Error initializing oxt_map: " << cpp_strerror(ret) << dendl;
	return;
  }
    
  if (free_extent_map_db->create_and_open(err)) {
      delete free_extent_map_db;
      derr << "Error initializing " << extent_map_backend
	   << " : " << err.str() << dendl;
      ret = -1;
	  return;
  }
  dout(3) << "Success open free_extent_map_db" << dendl;



  // start finisher 
  fc_finisher.start();
 
  // start_writer
  write_stop = false;
  write_thread.create("fc_writer");

 
}

//----- umount ----//
void FileContainer::umount()
{
  dout(3) << __func__ << dendl;

  // close files 
  _close_file_all();

  // wait finisher 
  { 
	Mutex::Locker l(fc_finisher_lock);
	while(!completions.empty()){
	  dout(3) << "waiting for completions empty" << dendl;
	  fc_finisher_cond.Wait(fc_finisher_lock);
	}
  }

  dout(3) << "flush waiting for fc_finisher" << dendl; 
  fc_finisher.wait_for_empty();
  fc_finisher.stop();
  dout(3) << "flush done" << dendl; 

  // stop writer 
  // 얘가 처리를 하고 finisher_cond.Signal() 을 부르기 때문에 먼저 죽으면 안됨. 
  if (!write_stop) {
	Mutex::Locker l(writeq_lock);
	write_stop = true;
	writeq_cond.Signal();
  }
  write_thread.join();

  // store 
  _save();

}

string FileContainer::ghobject_key(const ghobject_t &oid)
{
  string out;
  append_escaped(oid.hobj.oid.name, &out);
  out.push_back('.');
  append_escaped(oid.hobj.get_key(), &out);
  out.push_back('.');
  append_escaped(oid.hobj.nspace, &out);
  out.push_back('.');

  char snap_with_hash[1000];
  char *t = snap_with_hash;
  char *end = t + sizeof(snap_with_hash);
  if (oid.hobj.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, "head");
  else if (oid.hobj.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, "snapdir");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)oid.hobj.snap);

  if (oid.hobj.pool == -1)
    t += snprintf(t, end - t, ".none");
  else
    t += snprintf(t, end - t, ".%llx", (long long unsigned)oid.hobj.pool);
  t += snprintf(t, end - t, ".%.*X", (int)(sizeof(uint32_t)*2), oid.hobj.get_hash());

  if (oid.generation != ghobject_t::NO_GEN ||
      oid.shard_id != shard_id_t::NO_SHARD) {
    t += snprintf(t, end - t, ".%llx", (long long unsigned)oid.generation);
    t += snprintf(t, end - t, ".%x", (int)oid.shard_id);
  }
  out += string(snap_with_hash);
  return out;
}

int FileContainer::_write(buddy_iov_t& iov)
{
  dout(3) << __func__ << " iov " << iov << dendl;

  string fname = prefix_fn + to_string(iov.file_seq);
  int flags = O_RDWR | O_CREAT;
  if (directio) {
	flags |= O_DIRECT | O_DSYNC;
  }
  
  //int fd = _create_or_open_file(fname, O_DIRECT | O_DSYNC | O_CREAT);
  int fd = _create_or_open_file(fname, flags);
  dout(3) << __func__ << " fname " << fname << " fd " << fd << dendl;

  int r;
  if(directio){
	bufferlist abl;
	abl.claim_append(iov.data_bl);
	abl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);
	r = abl.write_fd(fd, iov.foff);
  } else 
	r = iov.data_bl.write_fd(fd, iov.foff); 

  dout(3) << __func__ << " fname " << fname << " fd " << fd << " completed " << r << dendl;

  if (r) {
	dout(3) << __func__ << " Failed to write with " << r << dendl;
	return r;
  }
  return 0;
}


int FileContainer::_read(int fd, buddy_iov_t& iov)
{
  bufferlist ebl;

#if 0
  if (directio)
	ebl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);
#endif
  //  iov.data_bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);

  // for test 
  uint64_t boff = iov.get_alloc_soff();
  uint64_t blen = iov.get_alloc_bytes();
  uint64_t foff = iov.foff;
  uint64_t len = iov.bytes;

  dout(3) << __func__ << " fd " << fd << " boff " << boff << " blen " << blen << dendl;
  int ret = ebl.read_fd(fd, boff, blen);
  iov.data_bl.substr_of(ebl, foff - boff, len);
  ret = iov.data_bl.length();

  if (ret < static_cast<int>(len)) 
    dout(10) << __func__ << " Read less "<< cpp_strerror(ret) << dendl;

  return ret;
}


//---- alloc space ---//
int FileContainer::_alloc_space(coll_t cid, const ghobject_t& oid, const off_t ooff, 
	const ssize_t bytes, vector<buddy_iov_t>& iov) 
{
  dout(3) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << dendl;

#if 0
  bool need_punch_hole = false;
#endif

  {
	Mutex::Locker l(fc_lock);
  
	// file open
	//string fname = prefix_fn + to_string(file_seq);

	buddy_iov_t* niov = new buddy_iov_t(cid, oid, ooff, curr_tail_off, bytes, file_seq, 0); 

	dout(3) << __func__ << " new alloc " << niov->foff << " : " << niov->bytes << dendl;
	//buddy_iov_t* niov = new buddy_iov_t(cid, oid, fname, 0, ooff, curr_tail_off, bytes); 
	iov.push_back(*niov);

	// alloc 
	curr_tail_off += niov->get_alloc_bytes();

#if 0
	// bg_reclaim 
	if (need_punch_hole && bg_reclaim) {
	  dout(3) << " need_punch_hole true " << dendl;
	  punch_hole_lock.Lock();
	  force_punch_hole = true;
	  punch_hole_cond.Signal();
	  punch_hole_lock.Unlock();
	}
#endif
  }
  return 0;
}


//---- prepare write ----//
// 실제 공간을 할당받아서 해당 정보를 iov 에 넣어줌. 
// buddy_iov_t 는 io 정보와 file 위치 정보 모두 가지고 있음. 

int FileContainer::prepare_write(vector<ObjectStore::Transaction> &tls, vector<buddy_iov_t>& tls_iov) 
{

  dout(5) << __func__  << " transactions = " << tls.size() << dendl;

  for (vector<ObjectStore::Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {

    ObjectStore::Transaction tr = *p; 
    ObjectStore::Transaction::iterator i = tr.begin();

    for(vector<ObjectStore::Transaction::Op>::iterator op_p = tr.punch_hole_ops.begin();
	  op_p != tr.punch_hole_ops.end(); ++op_p) {

      vector<buddy_iov_t> op_iov; 

      uint64_t ooff = op_p->off;
      uint64_t bytes = op_p->len;
      coll_t cid = i.get_cid(op_p->cid);
      ghobject_t oid = i.get_oid(op_p->oid);

	  // op_iov is set 
	  // iov 는 vector 형태로 유지. object 가 여러개의 iov 로 쪼개질 수 있으니까. 
      int ret = _alloc_space(cid, oid, ooff, bytes, op_iov);

      if (ret < 0){
		dout(5) << "Failed to alloc_space" << dendl;
		return -ENOENT;
      }

	  // prepare buffer 
      uint32_t punch_hole_off = op_p->punch_hole_off;
      uint32_t header_len = sizeof(__u32);
	  uint32_t data_start_off = punch_hole_off + header_len;

	  for(vector<buddy_iov_t>::iterator ip = op_iov.begin();
		  ip != op_iov.end(); ++ip){

		bufferlist newdata;
		newdata.append_zero((*ip).foff - (*ip).get_alloc_soff());

		dout(5) << " punch_hole_off " << punch_hole_off << dendl;
		dout(5) << " data_start_off " << data_start_off << dendl;
		dout(5) << " foff " << (*ip).foff << dendl;
		dout(5) << " get_alloc_soff " << (*ip).get_alloc_soff() << dendl;
		dout(5) << " newdata length " << newdata.length() << dendl;
		
		// copy data from transactions' bl 
		newdata.substr_of(
		  tr.data_bl, 
		  data_start_off + (*ip).src_off, 
		  (*ip).bytes);

		dout(5) << " newdata length " << newdata.length() << dendl;
		
		newdata.append_zero((*ip).get_alloc_bytes() - newdata.length());

		dout(5) << " get_alloc_bytes " << (*ip).get_alloc_bytes() << dendl;
		dout(5) << " newdata length " << newdata.length() << dendl;

		(*ip).data_bl.claim(newdata);

		assert((*ip).get_alloc_bytes() == (*ip).data_bl.length());
		dout(5) << op_iov << dendl;

#if 0
		// test
		bufferlist::iterator dp = (*ip).data_bl.begin();
		const char *dptr;
		dp.get_ptr_and_advance(10, &dptr);
		for(i=0; i<10; i++) 
		  dout(3) << __func__ << " after " << *(dptr+i) << dendl;
#endif
	  }

	  
	  // tls_iov 는 실제 write_thread 가 file 에 io 할 때 쓰는 정보임. 
	  tls_iov.insert(tls_iov.end(), op_iov.begin(), op_iov.end());

	  punch_hole_map_update(tr, punch_hole_off, op_iov);

	} // end of punch_hole_ops loop  
  } // end of tls loop

  dout(5) << __func__  << " iov = " << tls_iov.size() << dendl;
  return 0;
}


//----- submit_entry ----//
void FileContainer::submit_entry(uint64_t seq, vector<buddy_iov_t>& iov, Context* onfcwrite, TrackedOpRef osd_op) 
{
  dout(3) << __func__ << " seq " << seq << dendl;

  // for debug 
  for(auto p = iov.begin(); p != iov.end(); ++p)
	dout(3) << __func__ << " iov " << (*p) << dendl;

  Mutex::Locker l2(writeq_lock);
  Mutex::Locker l1(completions_lock);

  completions.push_back(
	  completion_item(seq, onfcwrite, ceph_clock_now(), osd_op));

  if (writeq.empty())
	writeq_cond.Signal();
  writeq.push_back(write_item(seq, iov, osd_op));


}

#if 0
void FileContainer::merge_write()
{
		for(vector<buddy_iov_t>::iterator iovp = it->iov.begin();
			iovp != it->iov.end(); iovp++) {

		  buddy_iov_t iov = *iovp;

		#if 0
		  set<buddy_iov_t>::iterator r = aggr_iovec.lower_bound(iov);

		  // 1. check prev_node
		  if(r != aggr_iovec.begin()){
			--r;
			buddy_iov_t r_iov = (*r);
			//if((r->foff + r->alloc_bytes) == iov.foff){
			//if((r->get_eoff() + 1) == iov.foff){
			//if((r->get_alloc_eoff()) == iov.get_alloc_soff()){
			if((r_iov.get_alloc_eoff()) == iov.get_alloc_soff()){
			  // 왠지 이거.. 새로운거 안만들고 그냥 추가해도 되지 않을까.. 

			  // new data 
			  bufferlist newdata;
			  newdata.claim(iov.data_bl);

			  // creates a new vector 
			  buddy_iov_t niov (*r);
			  niov.data_bl.append(newdata);

			  //niov.foff = iov.foff; 
			  //niov.alloc_bytes += iov.alloc_bytes;
			  //assert(niov.alloc_bytes == niov.data_bl.length());
			  
			  dout(3) << __func__ << niov << dendl;

			  // erase and add 
			  aggr_iovec.erase(r);
			  aggr_iovec.insert(niov);

			  continue;
			}
		  }

		  // 2. check next node 
		  if(r != aggr_iovec.end()){
			buddy_iov_t r_iov = (*r);
			if(iov.get_alloc_eoff() == r_iov.get_alloc_soff()) {

			  // new data 
			  bufferlist newdata;
			  newdata.claim(iov.data_bl);

			  // creates a new vector 
			  buddy_iov_t niov (*r);
			  niov.data_bl.claim_prepend(newdata);
			  niov.foff = iov.foff;


			  dout(3) << __func__ << niov << dendl;

			  // erase and add 
			  aggr_iovec.erase(r);
			  aggr_iovec.insert(niov);

			  continue;
			}
		  }
		  #endif

		  // no aggregation 
		  dout(3) << __func__ << " no aggregation " << dendl;
		  aggr_iovec.insert(iov);

		} // end of iov for loop 


}
#endif
//----- write_thread_entry -----//
void FileContainer::write_thread_entry()
{

  dout(3) << "fc write_thread_entry start" << dendl;
  utime_t lat;
  utime_t start;

  writeq_lock.Lock(); // ----- lock here!

  while (!write_stop) {

	if (writeq.empty()) {
	  dout(3) << "fc write_thread_entry going to sleep" << dendl;
	  writeq_cond.Wait(writeq_lock);
	  dout(3) << "fc write_thread_entry woke up" << dendl;
	  continue;
	}

	//-------------------------------
	while(!writeq.empty()){

	  list<write_item> items;
	  // new buddy_iov_t list 
	  set<buddy_iov_t> aggr_iovec;
	  writeq.swap(items); // if alread locked 
	  //batch_pop_writeq(items); // this will get a lock inside  
	  writeq_lock.Unlock(); // ---- unlock here!  

	  dout(3) << __func__<< " writeq_items = " << items.size() << dendl;

	  for (list<write_item>::iterator it = items.begin();
		  it != items.end(); ++it) {

		for(vector<buddy_iov_t>::iterator iovp = it->iov.begin();
			iovp != it->iov.end(); iovp++) {

		  aggr_iovec.insert(*iovp);
		} // total_bl for items 

		fc_finisher_lock.Lock();
		writing_seq = writing_seq < it->seq? it->seq : writing_seq;
		fc_finisher_lock.Unlock();

		dout(3) << __func__ << " writing_seq = " << writing_seq << " it->seq " << it->seq << dendl;
	  }

	  while(!items.empty()){
		items.pop_front();
	  }

	  //--------------------------------
	  // 2. write buffers
	  dout(3) << __func__ << " aggr_iovec size " << aggr_iovec.size() << dendl;

	  for(set<buddy_iov_t>::iterator iovp = aggr_iovec.begin();
		  iovp != aggr_iovec.end();
		  iovp++)
	  {
		dout(3) << __func__ << "foff " << iovp->foff << " len " << iovp->data_bl.length() << dendl;

		buddy_iov_t iov = *iovp;
		start = ceph_clock_now(); 

		// 이거 나중에 풀기 
		dout (3) << __func__ << " " << iov << dendl; 
		int ret = _write(iov);

		lat = ceph_clock_now();
		lat -= start;
		dout(5) << __func__ << " write_fd lat " << lat << dendl;
	  }

	  writeq_lock.Lock();
	  {
		Mutex::Locker l(fc_finisher_lock);
		written_seq = writing_seq;
		queue_completions_thru(written_seq);
	  }
	} // end of while writeq_empty 

  } // end of while(!write_stop)

  write_stop = false; 
  writeq_lock.Unlock();
  dout(3) << __func__ << " terminate " << dendl;
}

// Finisher 로 onfcwrte 를 옮겨주는 작업. 
// Context 를 write_item 으로 받아서 바로 위에서 해줘도 될 거 같은데 
void FileContainer::queue_completions_thru(uint64_t seq)
{
  dout(3) << __func__ << " seq " << seq << dendl;
  assert(fc_finisher_lock.is_locked());
  utime_t now = ceph_clock_now();
  list<completion_item> items;
  batch_pop_completions(items);
  list<completion_item>::iterator it = items.begin();

  dout(3) << __func__ << " completions_item = " << items.size() << dendl;
  while (it != items.end()) {
	dout(3) << __func__ << " queueing start " << dendl;
    completion_item& next = *it;
    if (next.seq > seq){
	  dout(3) << __func__ << " next seq " << next.seq << dendl;
      break;
	}
    utime_t lat = now;
    lat -= next.start;
    dout(3) << "queue_completions_thru seq " << seq
	     << " queueing seq " << next.seq
	     << " " << next.finish
	     << " lat " << lat << dendl;

    assert(next.finish);
    if (next.finish){
      dout(3) << __func__ << " finisher_queue seq = " << seq << dendl;
      fc_finisher.queue(next.finish);
    }
#if 0
//    if (next.tracked_op)
//      next.tracked_op->mark_event("journaled_completion_queued");
#endif
    items.erase(it++);
  }

  batch_unpop_completions(items);

  dout(3) << __func__ << " wake up finisher " << dendl;
  fc_finisher_cond.Signal();
}


void FileContainer::punch_hole_map_update(ObjectStore::Transaction& tr, uint32_t punch_hole_off, vector<buddy_iov_t>& op_iov)
{
  // 그냥 iovector 돌면서 Transaction::iov_t 만들어서 저장. 
  string fname;
  vector<ObjectStore::Transaction::iov_t> tr_iov;

  for(vector<buddy_iov_t>::iterator iovp = op_iov.begin();
	iovp != op_iov.end(); ++iovp) {

	  fname = prefix_fn + to_string((*iovp).file_seq);
	  tr_iov.push_back(ObjectStore::Transaction::iov_t(
		fname, (*iovp).ooff, (*iovp).foff, (*iovp).bytes));
  }
  tr.punch_hole_map.insert(make_pair(punch_hole_off, tr_iov)); 
}


//----- oxt_map ----------//

int FileContainer::oxt_map_update(vector<buddy_iov_t>& iov)
{
  for(vector<buddy_iov_t>::iterator p = iov.begin(); 
	p != iov.end(); ++p) 
  {
	dout(3) << __func__ << " " << *p << dendl;
	int r = oxt_map_single_update(*p);
	if ( r < 0 ) {
	  dout(3) << "Failed to update map" << dendl;
	  return r;
	}
  }
  return 0;
}

int FileContainer::oxt_map_single_update(buddy_iov_t& iov)
{
  string prefix = iov.cid.to_str(); // collection name 
  string key = ghobject_key(iov.oid); // oid 

  // delete an overlapped range with a new extent 
  uint64_t ns = 0, ne = 0, os = 0, oe = 0, ooff, bytes; 
  ooff = iov.ooff;
  bytes = iov.bytes;

  // read existing index map 
  bufferlist bl;
  oxt_map_db->get(prefix, key, &bl);
  map<uint64_t, buddy_iov_t> omap;  

  if(bl.length() == 0){
	dout(3) << __func__ << " " << iov << " new index: " << ooff << " : " << iov << dendl;
	// insert new omap 
	omap.insert(make_pair(ooff, iov));
	KeyValueDB::Transaction t = oxt_map_db->get_transaction();
	bufferlist nbl;
	::encode(omap, nbl); // 여기서는 src_off 까지 같이 되겠지.. 
	t->set(prefix, key, nbl); 
	return oxt_map_db->submit_transaction(t);
  }

  // found 
  bufferlist::iterator bp = bl.begin();
  ::decode(omap, bp);
  dout(3) << __func__ << " " << iov << " has omap with entries " << omap.size() << dendl;

  ns = ooff;
  ne = ooff + bytes -1;

  // upperbound 를 하면 동일한 게 있어도 그것보다 큰 것중에 가장 작은거 찾아줌. 
  // 주어진 게 마지막 것보다 더 크다면? omap.end() 를 반환하게 됨. 
  auto p = omap.upper_bound(ooff);
  
  // 만약 가장 작은 경우면 begin 을 반환함. 
  // begin 이랑 end 가 같으면 빈 omap 임. 
  // 그런데 어차피 omap.end == p 가 되니까 while 문 안들어가고 나옴. 
  if(p!= omap.begin())  
	--p;

  list<uint64_t> delete_list;
  list<buddy_iov_t> frag_list;
  
  /// punch out 
  while(p != omap.end()){

	dout(10) << __func__ << " p iov " << p->second << dendl;
	os = p->second.ooff;
	oe = p->second.ooff + p->second.bytes - 1;

	if (os > ne)
	  break;

	if (oe < ns){
	  p++;
	  continue;
	}

	buddy_iov_t prev_idx = p->second;
	buddy_iov_t post_idx = p->second;

	delete_list.push_back(os);

	if(ns == os && ne == oe) {
	  dout(3) << __func__ << " full match " << dendl;
	  break;
	}

	if(ns >= os && ns <= oe){
	  dout(3) << __func__ << " partial right match " << dendl;
	  prev_idx.bytes -= (oe - ns + 1);
	  dout(3) << __func__ << " prev_idx " << prev_idx << dendl;
	  if(prev_idx.bytes > 0)
		frag_list.push_back(prev_idx);
	  //      omap->index_map.insert(make_pair(prev_idx.ooff, prev_idx));
	}

	if(ne >= os && ne <= oe) {
	  dout(3) << __func__ << " partial left match " << dendl;
	  post_idx.ooff -= (ne - os + 1);
	  post_idx.foff -= (ne - os + 1);
	  post_idx.bytes -= (ne - os + 1); 
	  dout(3) << __func__ << " post_idx " << post_idx << dendl;
	  if(post_idx.bytes > 0)
		frag_list.push_back(post_idx);
	  //omap->index_map.insert(make_pair(post_idx.ooff, post_idx));
	}
	p++;
  }

  //delete_index:
  for(auto ooff_p = delete_list.begin(); ooff_p != delete_list.end() ; ooff_p++){
    dout(3) << __func__ << " delete : " << *ooff_p << dendl;

	// omap 에 삭제할 object 의 offset 이 있어야 함. 
	map<uint64_t, buddy_iov_t>::iterator entry_p = omap.find(*ooff_p);
	assert(entry_p != omap.end());

	buddy_iov_t iov = entry_p->second;

	// free extent map 은 ooff 를 기준으로 sorting 되어야 함. 
	// 숫자를 string 으로 바로 바꾸니까 100 이 9 보다 앞에 와서 안됨. 
	// 64bit 에서 가능한 십진수 자리수대로 0000 붙여서 string 만들었음. 
	string prefix = prefix_fn + to_string(iov.file_seq);
	string curr_start = uint64_to_str_key(iov.foff);
	uint64_t curr_end = iov.get_alloc_eoff();

	dout(3) << __func__ << " free_extent_map insert " << prefix << " " << curr_start << " " << curr_end << dendl;
	bufferlist fbl; 
	::encode(curr_end, fbl);
	KeyValueDB::Transaction t = free_extent_map_db->get_transaction();
	t->set(prefix, key, bl);
	free_extent_map_db->submit_transaction(t);

	// omap 에서 삭제 
	dout(3) << __func__ << " delete ooff " << *ooff_p << " from omap " << dendl;
    omap.erase(*ooff_p); // omap 에서는 지움. ooff 가 key 가 됨. 
  }

  //insert_partial_index:
  for(list<buddy_iov_t>::iterator p = frag_list.begin();
      p != frag_list.end(); p++){
  
      dout(3) << __func__ << " frag : " << *p << dendl;
	  auto entry = omap.find((*p).ooff);
	  assert(entry == omap.end());
      omap.insert(make_pair((*p).ooff, (*p)));
  }

#if 0
  // debug
  auto dp = omap.find(ooff);
  assert(dp == omap.end());
#endif

  dout(3) << __func__ << " insert new index " << ooff << " " << iov << dendl;
  omap.insert(make_pair(ooff, iov));
#if 0
  dp = omap.find(ooff);
  assert(dp->second == iov);
#endif

  bufferlist nbl;
  KeyValueDB::Transaction t = oxt_map_db->get_transaction();
  ::encode(omap, nbl); // 여기서는 src_off 까지 같이 되겠지.. 
  t->set(prefix, key, nbl); 
  oxt_map_db->submit_transaction(t);

#if 0
  // debug 
  map<uint64_t, buddy_iov_t> domap;  
  bufferlist dbl;
  oxt_map_db->get(prefix, key, &dbl);
  auto dbp = dbl.begin();
  ::decode(domap, dbp);
  auto dmp = domap.find(ooff);
  dout(3) << __func__ << " find iov " << dmp->second << dendl;
#endif

  return 0;

}

int FileContainer::oxt_map_lookup(const coll_t& cid, const ghobject_t& oid, uint64_t ooff, uint64_t len, vector<buddy_iov_t>& iov)
{
  // 실제 이 lookup 함수가 불리는 건 data 를 read 할때.  oxt_map 에 새로운
  // entry 를 넣을 때는 기존의 entry 를 읽기는 하는데 어차피 entry 만 읽어서
  // 겹치는 부분 삭제하고 다시 추가하니까 실제 데이터를 읽어오는 것 까지는
  // 필요없음. 원래 구현했던 함수에서는 vector<buddy_iov_t> 를 보내서 거기에 io 정보 받아오도록 함. 
  dout(3) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << " len " << len << dendl;

  map<uint64_t, buddy_iov_t> omap;  

  string prefix = cid.to_str(); 
  string key = ghobject_key(oid);
  bufferlist bl;

  oxt_map_db->get(prefix, key, &bl);

  if(bl.length() == 0){
	dout(10) << __func__ << " Not found " << dendl;
	return -1;
  }
  auto bp = bl.begin();
  ::decode(omap, bp);

  // debug 
  for(auto it = omap.begin(); it != omap.end(); ++it) {
	dout(3) << __func__ << " oxt_map: " << it->second << dendl;
  }

  uint64_t rbytes = len; // remaining bytes 
  uint64_t soff = ooff;
  uint64_t eoff = ooff + len;
  uint64_t foff = 0;
  uint64_t bytes = 0;
  uint64_t peoff = 0;

  dout(3) << __func__ << " oxt_map size = " << omap.size() << " soff " << soff << dendl;

  // search starts from the first smaller offset than the target 
  map<uint64_t, buddy_iov_t>::iterator p = omap.upper_bound(soff);

  // if p is same as omap.begin() and omap.end(), omap is empty. 이건 위에서
  // 걸러짐.  if p is same as omap.begin(), soff is the smallest value.  이
  // 경우는 읽고자 하는 부분이 실제 데이터보다 앞에 있는데, 그 부분은 없고 뒤에
  // 더 있다는 뜻. punch hole 을 허용하지 않는 이상 발생하지 않음. 허용하지
  // 않는 걸로 정리. 아래의 assert 문은 hole 이 있는 경우를 체크하는 것임. 

  assert(p != omap.begin());
  p--;

  while(rbytes > 0) {

	// contiguity check  
	assert(soff >= p->second.ooff && soff <= (p->second.ooff + p->second.bytes));
	dout(3) << __func__ << " soff " << p->first << " iov " << p->second << dendl;

	foff = p->second.foff + (soff - p->second.ooff); 
	peoff = p->second.ooff + p->second.bytes;
	bytes = (peoff < eoff? peoff : eoff) - soff;

	buddy_iov_t* niov = new buddy_iov_t(cid, oid, soff, foff, bytes, p->second.file_seq); 
	iov.push_back(*niov);

	rbytes -= bytes; 
	soff += bytes;

	p++; 
  }

  assert(rbytes == 0);

  return 0;
}

int FileContainer::sync()
{
  assert(directio);
  KeyValueDB::Transaction t = oxt_map_db->get_transaction();

  bufferlist bl;

  {
	Mutex::Locker l(fc_lock);
	::encode(file_seq, bl);
	map<string, bufferlist> to_write;
	to_write[SYS_KEY] = bl;
	t->set(SYS_PREFIX, to_write);
  }
  return oxt_map_db->submit_transaction_sync(t);
}

size_t FileContainer::get_size(const coll_t& cid, const ghobject_t& oid)
{
  return 0;
}


//---- read -----// 
int FileContainer::read(const coll_t& cid, const ghobject_t& oid, uint64_t off, uint64_t len, bufferlist &bl)
{
  dout(3) << __func__ << " cid " << cid << " oid " << oid << " off " << off << " len " << len << dendl; 
  // map 정보 읽어와서 실제 bl 에 담아주면 됨. 
  vector<buddy_iov_t> iov;
  int ret = oxt_map_lookup(cid, oid, off, len, iov); 

  if (ret < 0) // not found 
	return 0;

  dout(10) << __func__ << " iov.size = " << iov.size() << dendl; 

  string fname; 
  int fd; 

  for(auto ip = iov.begin(); ip != iov.end(); ++ip){
	// open file 
	fname = prefix_fn + to_string(ip->file_seq);
	fd = _create_or_open_file(fname, 0);

	dout(3) << __func__ << " data_bl " << (*ip).data_bl.length() << dendl;
	ret = _read (fd, (*ip)); // iov 가 const 아니어도 넘어가는지 모르겠음. 
	// 읽어온 거를 다 붙여서 저장해야할듯. 
	bl.claim_append((*ip).data_bl);
  }
  dout(3) << __func__ << " requested: " << len << " read: " << bl.length() << dendl;
  //return bl.length();
  //ret = bl.length();

  return ret;
}


//---- remove -----// 
int FileContainer::remove(const coll_t& cid, const ghobject_t& oid)
{
  dout(3) << __func__ << " cid " << cid << " oid " << oid << dendl;
  // get oxt map 
  map<uint64_t, buddy_iov_t> omap;  

  string prefix = cid.to_str(); 
  string key = ghobject_key(oid);
  bufferlist bl;

  // object 가 차지하고 있던 공간을 회수하여 free extent map 에 추가 
  // 해당 영역에 punch_hole 을 날릴지는 background thread 가 처리
  // YUIL
  oxt_map_db->get(prefix, key, &bl);
  if(bl.length() == 0)
	return 0;

  bufferlist::iterator bp = bl.begin(); 
  ::decode(omap, bp);


  KeyValueDB::Transaction t = free_extent_map_db->get_transaction();
  string curr_prefix(""), curr_key("");


  dout(3) << __func__ << " omap size = " << omap.size() << dendl;
  for (map<uint64_t, buddy_iov_t>::iterator p = omap.begin(); p != omap.end(); ++p)
  {

	// check merge possible  
	uint64_t curr_start = p->second.get_alloc_soff(); // 지울 애들 
	uint64_t curr_end = p->second.get_alloc_eoff(); // 

	curr_prefix = prefix_fn + to_string(p->second.file_seq);
	curr_key = uint64_to_str_key(curr_start);

	dout(3) << __func__ << " curr_start " << curr_start << " curr_end " << curr_end 
	  << " curr_prefix " << curr_prefix << " curr_key " << curr_key << dendl;

	uint64_t prev_start, prev_end, next_start, next_end;

	// 앞에 꺼랑 merge 될 수 있는지 확인. 
	KeyValueDB::Iterator iter = free_extent_map_db->get_iterator(curr_prefix);
	
	int ret = iter->seek_for_prev(curr_key);  // ok 면 ret 값이 0 임.
	
	if(iter->valid()) {
	  assert(ret == 0);

	  pair<string, string> prev_full_key = iter->raw_key();
	  prev_start = std::stoull(prev_full_key.second);
	  bufferlist prev_bl = iter->value(); 
	  bufferlist::iterator bp = prev_bl.begin();
	  ::decode(prev_end, bp);

	  dout(3) << __func__ << " prev: prev_start " << prev_start << " prev_end " << prev_end << dendl;
	  assert(prev_start <= curr_start);

	  if(prev_end >= curr_start) {
		curr_end = prev_end > curr_end ? prev_end : curr_end;	
		curr_start = prev_start;
		curr_key = to_string(curr_start);
		// 여긴 원래거 지울 필요 없음. key 가 같으니까. 
	  }
	}
#if 0 
	// 뒤에꺼랑 합칠 수 있는지 확인. 만약 가능하다면 뒤의 것은 삭제해야 함. 
	for(iter->upper_bound(curr_key); iter->valid(); iter->next())
	{
	  pair<string, string> next_full_key = iter->raw_key();
	  assert(next_full_key.first == curr_prefix);

	  next_start = std::stoull(next_full_key.second);
	  bufferlist next_bl = iter->value(); 
	  bufferlist::iterator bp = next_bl.begin();
	  ::decode(next_end, bp);

	  dout(3) << __func__ << " next_start " << next_start << " next_end " << next_end << dendl;
	  assert(next_start > curr_start); 

	  if(curr_end < next_start) 
		break;

	  curr_end = next_end > curr_end ? next_end : curr_end;	
	  t->rmkey(next_full_key.first, next_full_key.second);
		// 여기는 원래 key 지워야 함. 
	}

#endif
	if ((curr_end - curr_start) >= MIN_PUNCH_HOLE_SIZE){
	  do_punch_hole(curr_prefix, curr_start, curr_end - curr_start);
	} else { 
	  dout(3) << __func__ << " tr: prefix " << curr_prefix << " curr_key " << curr_key << " curr_end " << curr_end << " curr_start " << curr_start << dendl;
	  bufferlist bl; 
	  ::encode(curr_end, bl);
	  t->set(curr_prefix, curr_key, bl);
	}
	//t->set(curr_prefix, curr_key, bbl);
  } // end of omap 

  int r = free_extent_map_db->submit_transaction(t);
  if (r < 0) {
	dout(3) << __func__ << " Failed to insert entry into free extent map " << dendl;
	assert(0);
  }

#if 0
  // debug 
  int count = 0;
  KeyValueDB::Iterator it = free_extent_map_db->get_iterator(curr_prefix);
  for (it->seek_to_first(); it->valid(); it->next()){
	count++;
	pair<string, string> full_key = it->raw_key();
	dout(3) << __func__ << " free_extent_map: prefix " << full_key.first << " offset " << full_key.second << dendl;
	if(full_key.first != curr_prefix)
	  break;
  }
  dout(3) << __func__ << " free_extent_map entries : " << count << dendl;

  it->lower_bound(curr_key);
  if(it->valid()){
	pair<string, string> prev_full_key = it->raw_key();
	dout(3) << __func__ << " lower_bound result = prefix " << prev_full_key.first << " key " << prev_full_key.second << dendl;

  } else {
	dout(3) << __func__ << " lower_bound is not valid " << dendl;
  }	
#endif


  // object extent map update 
  //if(oxt_map_db){
	//KeyValueDB::Transaction 
	t = oxt_map_db->get_transaction(); 
	t->rmkey(prefix, key);	
	r = oxt_map_db->submit_transaction(t);
	if (r < 0) {
	  dout(3) << __func__ << "Failed to submit_transaction " << dendl;
	}

	// debug 
	bufferlist dbl;
	oxt_map_db->get(prefix, key, &dbl);
  	assert(dbl.length() == 0);

	dout(3) << __func__ << " Succed to remove key " << prefix << " " << key << dendl;
  //}
  return 0;
}

int FileContainer::clone(const coll_t& cid, const ghobject_t& src, const ghobject_t& dest, uint64_t srcoff, uint64_t len, uint64_t dstoff)
{

  // src object 에서 dst 로 write 하라는 건데.. 
  // 이거 transaction 에서 encoding 했을 때 어떻게 하는지? 
  // src 에 정보가 있으니까 이건 log 에 데이터가 없을 것임. 
  // 여기에서 실제 읽어와서 write 해야함. 

  // 1. fc->read(src) to bl 
  // 2. write 를 해야함 

  return 0;
}

int FileContainer::truncate(const coll_t& cid, const ghobject_t& oid, uint64_t bytes) 
{
  return 0;
};

int FileContainer::do_punch_hole(string fname, uint64_t soff, uint64_t bytes)
{
  dout(3) << __func__ << " fname " << fname << " soff " << soff << " bytes " << bytes << dendl;

  int fd = _create_or_open_file(fname, O_RDWR); // flags 
  if (fd < 0) {
	dout(1) << __func__ << " Failed to open file" << dendl;
	return fd;
  }

  int ret = fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, soff, bytes);
  if (ret < 0 ){
      dout(1) << __func__ << " failed to punch_hole " << dendl;
  }
  return ret;
}

#if 0
//----- punch_hole_thread_entry -----//
void FileContainer::punch_hole_thread_entry()
{

  dout(3) << "fc punch_hole_thread_entry start" << dendl;
  utime_t lat;
  utime_t start;

  punch_hole_lock.Lock();

  utime_t interval;
  interval.set_from_double(30.0);

  while (!stop_punch_hole) {
    utime_t startwait = ceph_clock_now();

    if (!force_punch_hole) {
      dout(10) << __func__ << " waiting for interval " <<  interval << dendl;
      punch_hole_cond.WaitInterval(punch_hole_lock, interval);
    }
    if (force_punch_hole) {
      dout(10) << __func__ << " force_punch_hole " << dendl;
      force_punch_hole = false;
    }
    if (stop_punch_hole) {
      dout(10) << __func__ << " stop punch_hole_thread " << dendl;
      break;

    } else {
      utime_t woke = ceph_clock_now();
      woke -= startwait;
      dout(10) << __func__ << " woke up after " << woke << dendl;
    }

    punch_hole_lock.Unlock();

	// do_punch_hole 

  }

  stop_punch_hole = false;
  punch_hole_lock.Unlock();

  dout(3) << __func__ << " terminate " << dendl;
}
#endif
