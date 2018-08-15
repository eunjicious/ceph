#include "BuddyLogDataFileObject.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfilestore " 

const static int64_t ONE_MEG(1 << 20);
const static int CEPH_DIRECTIO_ALIGNMENT(4096);

/**********************************
 *  BuddyHashIndexFile functions 
 **********************************/


BuddyLogDataFileObject::BuddyLogDataFileObject(CephContext* cct_, 
      string fname_, bool dio=true, bool prealloc=true) :
      cct(cct_),
      fname(fname_),
      directio(dio),
      dfd(-1),
      tail_off(0),
	  total_pool_bytes(0),
	  total_reserved_bytes(0),
      total_used_bytes(0),
      total_alloc_bytes(0),
      prealloc_bytes(0),
      prewrite_unit_bytes(cct->_conf->buddystore_file_prewrite_unit),
      file_prewrite(cct->_conf->buddystore_file_prewrite),
      file_prealloc(!file_prewrite),
      file_inplace_write(cct->_conf->buddystore_file_inplace_write),
	  bg_reclaim(cct->_conf->filestore_max_sync_interval),
	  slot_lock("BuddyLogDataFileObject::slock"),
	  slot_used_bytes(BUDDY_SLOT_NUM, 0),
	  slot_limit_bytes(BUDDY_SLOT_NUM, BUDDY_INIT_ALLOC_SIZE), 
	  low_util_ratio(0.6),
	  high_util_ratio(0.8),
	  punch_hole_thread(this),
	  punch_hole_lock("BuddyLogDataFileObject::phlock"),
	  stop_punch_hole(false),
	  prewrite_thread(this),
	  prewrite_lock("BuddyLogDataFileObject::pwrtlock"),
	  stop_prewrite(false),
      lock("BuddyLogDataFileObject::lock") {
	//create_or_open_file(0);
		_stat_file();
      } 


/******
 * create_or_open_file  
 */
int BuddyLogDataFileObject::create_or_open_file(int out_flags)
{
  ldout(cct,10) << __func__ << dendl;

  int flags = O_RDWR | O_CREAT;
  flags |= out_flags;
    
  if (directio) 
    flags |= O_DIRECT | O_DSYNC;

  // open 
  int r = ::open(fname.c_str(), flags, 0644);
  if (r < 0){
    ldout(cct,10) << __func__ << "Failed to create file: " << fname << cpp_strerror(r) << dendl; 
    return r;
  }
  dfd = r;

  struct stat st;
  r = ::fstat(dfd, &st);
    
  if (r < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return r;
  }
  ldout(cct, 10) << __func__ << " data_file size " << st.st_size << dendl;


  // we can hold it for later .. 
  if(file_prealloc){
    ldout(cct, 10) << __func__ << " file_prealloc is set " << dendl;
    preallocate(0, BUDDY_PREALLOC_SIZE);
  }

  // prewrite ... 
  if (file_prewrite) {

    ldout(cct, 10) << __func__ << " prewrite_unit_bytes " << prewrite_unit_bytes << dendl;
    bufferlist bl;
    //bl.append_zero(prewrite_unit_bytes);
    bl.append_zero(1UL << 24); // 16MB

    uint64_t prewrite_size = prewrite_unit_bytes << 10;
    uint64_t pos = st.st_size;

    ldout(cct,10) << __func__ << " current data_file size " << pos << dendl;

    while (pos < prewrite_size) {
      r = write_fd(bl, pos);
      //pos += bl.length();
      pos += prewrite_unit_bytes;
    }

    ldout(cct, 10) << __func__ << " prewrite write ret = " << r << dendl;
  }

  r = ::fstat(dfd, &st);

  ldout(cct,10) << __func__ << " data_file_size " << st.st_size << dendl;

  //total_pool_bytes = BUDDY_SLOT_NUM * BUDDY_INIT_ALLOC_SIZE;
  

  total_pool_bytes = 11811160064; // 11G
  //total_pool_bytes = 11 * 1048576 * 1024; 
  total_reserved_bytes = total_pool_bytes + (1UL << 30);

  ldout(cct, 4) << __func__ << " BUDDY_PREALLOC_SIZE " << BUDDY_PREALLOC_SIZE << dendl;
  ldout(cct, 4) << __func__ << " BUDDY_SLOT_NUM " << BUDDY_SLOT_NUM << dendl;
  ldout(cct, 4) << __func__ << " BUDDY_INIT_ALLOC_SIZE " << BUDDY_INIT_ALLOC_SIZE << dendl;

  ldout(cct, 4) << __func__ << " total_pool_bytes " << (total_pool_bytes >> 20) << dendl;
  ldout(cct, 4) << __func__ << " total_reserved_bytes " << (total_reserved_bytes >> 20) << dendl; 
  //st.st_size * 0.8;
  //total_pool_bytes = st.st_size ;

  ldout(cct, 4) << __func__ << " bg_reclaim " << bg_reclaim << dendl;

//---------- if read buffered mode ----- // 
//  ::close(fd);

  if (bg_reclaim) {
//---- thread create ---//
	punch_hole_thread.create("buddy_punch");
	prewrite_thread.create("buddy_pwrt");
  }

  return 0;
}

/******
 * delete_file  
 */
int BuddyLogDataFileObject::delete_file()
{
  ldout(cct,10) << __func__ << dendl;

  if (dfd > 0)
    close_file();

  int r = ::unlink(fname.c_str());
  if (r < 0){
    ldout(cct,10) << "Failed to delete file: " << fname << dendl; 
  }
  return 0;
}

/******
 * close_file
 */
int BuddyLogDataFileObject::close_file()
{
  if (dfd < 0){
    ldout(cct, 10) << "no file exists" << dendl; 
    return -1;
  }

  //int r = ::close(fd);

  VOID_TEMP_FAILURE_RETRY(::close(dfd));

  if (bg_reclaim){

  // thread join 
  punch_hole_lock.Lock();
  stop_punch_hole = true;
  punch_hole_cond.Signal();
  punch_hole_lock.Unlock();
  punch_hole_thread.join();

  prewrite_lock.Lock();
  stop_prewrite = true;
  prewrite_cond.Signal();
  prewrite_lock.Unlock();
  prewrite_thread.join();
  }

  return 0; 

}

/******
 * stat_file
 */
void BuddyLogDataFileObject::stat_file()
{
  ldout(cct, 10) << " fname " << fname << " tail_off " << tail_off <<  
	" total_used_bytes " << total_used_bytes << 
	" total_alloc_bytes " << total_alloc_bytes << dendl;

}
void BuddyLogDataFileObject::_stat_file()
{
  ldout(cct, 4) << " tail_off " << tail_off << dendl;
  ldout(cct, 4) << " total_pool_bytes " << total_pool_bytes << dendl;
  ldout(cct, 4) << " total_reserved_bytes " << total_reserved_bytes << dendl;
  ldout(cct, 4) << " total_used_bytes " << total_used_bytes << dendl;
  ldout(cct, 4) << " total_alloc_bytes " << total_alloc_bytes << dendl;

  int i;
  for (i = 0; i < BUDDY_SLOT_NUM; i++){
	ldout(cct, 4) << " slot(KB) " << (i * 4) << " slot_used(MB) " << (slot_used_bytes[i] >> 20) << 
	  " slot_limit(MB) "  << (slot_limit_bytes[i] >> 20) << dendl;
  }

}

/*****************
 * index update
 ****/
// move to collection ... 
#if 0
int BuddyLogDataFileObject::ninsert_index(const ghobject_t& oid, const uint64_t ooff, 
    const uint64_t foff, const uint64_t bytes )
{


  ldout(cct, 10) << __func__ << " oid " << oid << " ooff " << ooff <<
    " foff " << foff << " bytes " << bytes << dendl;

  RWLock::WLocker l(lock);

  buddy_index_map_t* omap;
  buddy_index_t* nidx = new buddy_index_t(ooff, foff, bytes); 

  uint64_t ns = 0, ne = 0, os = 0, oe = 0;
  map<uint64_t, buddy_index_t>::iterator sp, p;

  // find map 
  map<ghobject_t, buddy_index_map_t>::iterator omap_p = log_index_map.find(oid);

  // not found? create and insert entry. done! 
  if(omap_p == log_index_map.end()){
    ldout(cct, 15) << __func__ <<  " oid is not found. create index map " << dendl;

    // create index_map 
    omap = new buddy_index_map_t();
    log_index_map.insert(make_pair(oid, (*omap)));
    omap_p = log_index_map.find(oid);
  }

  // found map 
  omap = &omap_p->second;
  sp = omap->index_map.upper_bound(ooff);
  
  if (sp == omap->index_map.begin() && sp == omap->index_map.end()){
    ldout(cct, 15) << __func__ << " empty index map " << dendl;
    goto insert_new_index;
  }

  // prev 부터 돌면서 남아있는게 없을 때까지 punch out 
  // 겹치는게 있으면 original map 에서 줄여버리거나 삭제. 
  
  ns = ooff;
  ne = ooff + bytes;

  if(sp!= omap->index_map.begin()) p = --sp;
  else p = sp;

  /// punch out 
  // 내가 짰지만.. 정말 잘짠거 같구나.. 
  while(p != omap->index_map.end()){
    os = p->second.ooff;
    oe = p->second.ooff + p->second.used_bytes;

    if (os > ne) 
      break;

    if (oe < ns)
      continue;


    buddy_index_t prev_idx = p->second;
    buddy_index_t post_idx = p->second;

    omap->index_map.erase(os);

    if(ns == os && ne == oe) {
      break;
    }

    if(ns > os && ns < oe){
      prev_idx.used_bytes -= (oe -ns + 1);
      omap->index_map.insert(make_pair(prev_idx.ooff, prev_idx));
    }

    if(ne > os && ne < oe) {
      post_idx.ooff -= (ne - os + 1);
      post_idx.foff -= (ne - os + 1);
      post_idx.used_bytes -= (ne - os + 1); 
      omap->index_map.insert(make_pair(post_idx.ooff, post_idx));
    }
    p++;
  }

  insert_new_index:
  // insert 
  omap->index_map.insert(make_pair(ooff, (*nidx)));

// for debugging... 
  for(map<uint64_t, buddy_index_t>::iterator tmp = omap->index_map.begin(); 
      tmp != omap->index_map.end() ; tmp++){
    ldout(cct, 10) << __func__ << " oid " << oid << " index_map = " << (*tmp) << dendl;
  }
  
  return 0;

}
#endif

/******
 * alloc_space 
 * 중요 함수 
 */

int BuddyLogDataFileObject::alloc_space(coll_t cid, const ghobject_t& oid, 
    const uint64_t ooff, const uint64_t bytes, vector<buddy_iov_t>& iov)
{
  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << dendl;
  bool need_punch_hole = false;
  {
	Mutex::Locker l(lock);

	//buddy_iov_t* niov = new buddy_iov_t(cid, oid, fname, 0, ooff, tail_off, bytes); 
	buddy_iov_t* niov = new buddy_iov_t(cid, oid, ooff, tail_off, bytes, 0, 0);
	iov.push_back(*niov);

	// alloc 
	tail_off += niov->get_alloc_bytes();

	// bg_reclaim 
	if (need_punch_hole && bg_reclaim) {
	  ldout(cct, 4) << " need_punch_hole true " << dendl;
	  punch_hole_lock.Lock();
	  force_punch_hole = true;
	  punch_hole_cond.Signal();
	  punch_hole_lock.Unlock();
	}
  }
  return 0;

#if 0

  bool need_punch_hole = false;
  {
  Mutex::Locker l(lock);

  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << dendl;

  uint64_t eoff = ooff + bytes - 1;
  uint64_t bsoff = round_down(ooff); 
  uint64_t beoff = round_up(eoff);
  uint64_t off_in_blk = ooff - bsoff;
  uint64_t alloc_bytes = beoff - bsoff;

  // 1. allocate space at block granularity 
  uint64_t foff = tail_off + off_in_blk;


  tail_off += alloc_bytes;
  total_alloc_bytes += alloc_bytes;
  total_used_bytes += bytes;


//  if( file_prealloc && ((prealloc_bytes - total_alloc_bytes) < BUDDY_PREALLOC_LOW_WATERMARK)) 
//    preallocate(prealloc_bytes, BUDDY_PREALLOC_SIZE);

 
  // 3. create buddy_iov_t  
  //buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, ooff, foff, bytes); 
  //
  // 여기에서 0 대신에 ooff 를 주면 문제가 생김. 
  // 넘어온 data_bl 에서 "어디서부터 copy" 해올지를 저거 보고 결정하는데 
  // 0 을 주면 "지금 넘어온거의 처음" 부터라는 의미. 
  // 즉, off_in_src 가 되는게 맞음. 
  // 만약 쪼개진다면- 저걸 그 offset 에 맞게 0대신 세팅해야함. 
  // log 에서 하나로 할당할 때는 저렇게 하니까. 
  //
  //buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, 0, foff, bytes); 
  buddy_iov_t* niov = new buddy_iov_t(cid, oid, fname, 0, ooff, foff, bytes); 
 // assert(niov->off_in_blk == off_in_blk);
  //assert(niov->src_off == off_in_blk);

  iov.push_back(*niov);


  // update slot_used_byets
  int index=0;
  int blks = alloc_bytes >> BUDDY_FALLOC_SIZE_BITS;

  assert(alloc_bytes > 0);

  ldout(cct, 10) << __func__ << " blks " << blks << dendl;

  while(blks > 1) {
	index++;
	blks = blks >> 1;
  }

  assert(blks == 1);

  ldout(cct, 10) << __func__ << " index " << index << dendl;
  slot_used_bytes[index] += alloc_bytes;
  //if (static_cast<double>(slot_limit_bytes[index]) - slot_used_bytes[index] < static_cast<double>(slot_limit_bytes[index])*0.2){

  if (get_slot_util(index) > high_util_ratio) {
	ldout(cct, 4) << __func__ << " need_punch_hole" << dendl;
	need_punch_hole = true;
  }

  ldout(cct, 4) << __func__ << " slot_used_bytes " << slot_used_bytes[index] / 1024 << " KB " << dendl;
  ldout(cct, 4) << __func__ << " slot_limit_bytes " << slot_limit_bytes[index] / 1024 << " KB " << dendl;
  ldout(cct, 4) << __func__ << " niov " << *niov << " tail_off = " << tail_off << 
    " alloc_bytes " << alloc_bytes << " total_used_bytes " << total_used_bytes << dendl;    

#if 0
  // 이 부분은.. write 에서 해야함 
  // 2. insert_index 로 추가시키기. 
  //insert_index(niov);
  int r = insert_index(oid, ooff, foff, bytes);
  if(r < 0)
    ldout(cct, 10) << __func__ << " Failed to insert index " << dendl;
#endif
  }

  if (need_punch_hole && bg_reclaim) {
	ldout(cct, 4) << " need_punch_hole true " << dendl;
	punch_hole_lock.Lock();
	force_punch_hole = true;
	punch_hole_cond.Signal();
	punch_hole_lock.Unlock();
  }
 
  return 0;
#endif
}

/**********
 * release_space
 * */

//int BuddyLogDataFileObject::release_space(const ghobject_t& oid)
int BuddyLogDataFileObject::release_space(const buddy_index_map_t& omap)
{

  Mutex::Locker l(lock);

  // 해당 object 의 map을 받아서 지우고 return 하면 
  // 호출한 함수에서 마저 지우면 됨.. 

#if 0
  // 여기는 _remove 로 이동하고 
  // find map 
  map<ghobject_t, buddy_index_map_t>::iterator omap_p = log_index_map.find(oid);

  // not found? create and insert entry. done! 
  if(omap_p == log_index_map.end()){
    ldout(cct, 15) << __func__ <<  " oid index does not exist " << dendl;
    return 0;
  }
#endif

  // found map 
  //buddy_index_map_t omap = omap_p->second;

  for(map<uint64_t, buddy_index_t>::const_iterator p = omap.index_map.begin();
      p != omap.index_map.end(); p++) {

    ldout(cct, 10) << __func__ << " free index : " << p->second << dendl;
    // add to free map
	buddy_index_t idx = p->second;

    auto r = free_index_map.insert(make_pair(idx.foff, idx.get_alloc_bytes()));
    if(!r.second) {
      ldout(cct, 10) << __func__ << " free_index_map already contains buddy_index_t " << *p << dendl;
      assert(0);
    }

    // punch_hole : we might want to coalescing holes in the future..
    //int ret = fallocate(dfd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, p->second.foff, p->second.alloc_bytes);
    int ret = fallocate(dfd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, 
	  idx.get_alloc_soff(), idx.get_alloc_bytes());

    if (ret < 0 ){
      ldout(cct, 10) << __func__ << " failed to punch_hole " << *p << dendl;
      assert(0);
    }
  }

  ldout(cct, 10) << __func__ << " free index size : " << free_index_map.size() << dendl;


  // 이 부분은.. _remove 로 이동. 
  //auto r = log_index_map.erase(oid);

  return 0;
}

#if 0
/******
 * get_space_info
 */

int BuddyLogDataFileObject::get_space_info(const ghobject_t& oid, const uint64_t ooff, const uint64_t bytes, 
    vector<buddy_iov_t>& iov)
{


  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << " bytes " << bytes << dendl;

  RWLock::RLocker l(lock);

  // find map 
  map<ghobject_t, buddy_index_map_t>::iterator omap_p = log_index_map.find(oid);

  if(omap_p == log_index_map.end()){
    return -1;
  }

  // found map 
  buddy_index_map_t* omap = &omap_p->second;
  map<uint64_t, buddy_index_t>::iterator p;

// for debugging... 
  for(map<uint64_t, buddy_index_t>::iterator tmp = omap->index_map.begin(); 
      tmp != omap->index_map.end() ; tmp++){
    ldout(cct, 10) << __func__ << " oid " << oid << " index_map = " << (*tmp) << dendl;
  }


  uint64_t rbytes = bytes;
  uint64_t soff = ooff;
  uint64_t eoff = ooff + bytes;
  uint64_t foff = 0;
  uint64_t fbytes = 0;
  uint64_t peoff = 0;

  p = omap->index_map.upper_bound(soff);
  if (p == omap->index_map.begin()){
    ldout(cct, 10) << __func__ << " not found index starting with ooff " << ooff << dendl;
    return -1;
  }

  p--;

  while(rbytes > 0) {

    ldout(cct, 20) << __func__ << " rbytes = " << rbytes << " p.ooff " << p->second.ooff << " p.eoff " << p->second.ooff + p->second.used_bytes << dendl;

    assert(soff >= p->second.ooff && soff <= (p->second.ooff + p->second.used_bytes));

    peoff = p->second.ooff + p->second.used_bytes;

    foff = p->second.foff + (soff - p->second.ooff);
    fbytes = (peoff < eoff? peoff : eoff) - soff;

    buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, soff, foff, fbytes); 
    iov.push_back(*niov);

    rbytes -= fbytes;
    soff += fbytes;

    p++;
  }

  assert(rbytes == 0);


  return 0;
}
#endif


#if 0
/******
 * truncate_space 
 */
int BuddyLogDataFileObject::truncate_space(const ghobject_t& oid, uint64_t size)
{
  ldout(cct,10) << __func__  << " oid " << oid << " size " << size << dendl;

  uint64_t hoff = hash_to_hoff(oid);

  RWLock::WLocker l(lock);

  map<uint64_t, buddy_lindex_t>::iterator p = hash_index_map.find(hoff);

  if(p == hash_index_map.end())
    return -1; // no exist

  ldout(cct,10) << "before: oid " << oid << " ubytes " << p->second.used_bytes 
    << " abytes " << p->second.alloc_bytes << dendl;

  // add more space?
  uint64_t pad_bytes;
  uint64_t pad_off;
  if(size > p->second.used_bytes){
    pad_bytes = size - p->second.used_bytes;
    pad_off = p->second.used_bytes + 1;
    vector<buddy_iov_t> iov; 
    alloc_space(BD_DATA_T, oid, pad_off, pad_bytes, iov); 
  }
  else {
    p->second.used_bytes = size;
    p->second.alloc_bytes = round_up(p->second.used_bytes);
  }

  ldout(cct,10) << "after: oid " << oid << " ubytes " << p->second.used_bytes 
    << " abytes " << p->second.alloc_bytes << dendl;

  return 0;
}

/******
 * clone_space 
 */

int BuddyLogDataFileObject::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, vector<buddy_iov_t>& iov) 
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid << dendl;

  RWLock::WLocker l(lock);
  // get ooid space 
  uint64_t ohoff = hash_to_hoff(ooid);

  map<uint64_t, buddy_lindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }
  return clone_space(ooid, noid, 0, op->second.used_bytes, 0, iov);
}

int BuddyLogDataFileObject::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, uint64_t srcoff, size_t bytes, uint64_t dstoff, 
    vector<buddy_iov_t>& iov)
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid 
    << " srcoff " << srcoff << " bytes " << bytes << " dstoff " << dstoff<< dendl;

  RWLock::WLocker l(lock);

  // get ooid space 
  uint64_t ohoff = hash_to_hoff(ooid);
  uint64_t nhoff = hash_to_hoff(noid);
  ldout(cct,10) << "ohoff " << ohoff << " nhoff " << nhoff << dendl;

  map<uint64_t, buddy_lindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }

  // alloc space for new object 
  alloc_space(op->second.ctype, noid, 0, (uint64_t)(dstoff + bytes), iov);  
  
  ldout(cct,10) << "read_file " << fname << " " << ohoff+srcoff << "~" << bytes << dendl; 
  // read and copy 
  bufferlist src_bl;
  string err;
  src_bl.read_file(fname.c_str(), ohoff + srcoff, bytes, &err);

  ldout(cct,10) << "read size " << src_bl.length() << dendl;

  // write 
  // generate_iov()
  // buddy_iov_t* niov = new buddy_iov_t(BD_ORIG_F, fname, 0, nhoff + dstoff, bytes);
  // alloc_space create iovector in iov 
  for(vector<buddy_iov_t>::iterator ip = iov.begin();
      ip != iov.end(); ip++){
    assert(ip->data_bl.length() == 0);
    ip->data_bl.substr_of(src_bl, (*ip).ooff, (*ip).ooff + (*ip).bytes);
    (*ip).ooff = 0;
  }
  //niov->data_bl.claim(src_bl);

  return 0;

}

#endif

#if 0
int BuddyLogDataFileObject::write(bufferlist& bl, uint64_t foff, size_t len)
{

  // open with direct I/O 

  // write_fd 

  // close

}


/******
 * write_fd 
 */
    
int BuddyLogDataFileObject::write_fd(bufferlist& bl, uint64_t foff, int fd)
{
  ldout(cct, 5) << __func__ << " fd " << fd << " bl.length " << bl.length() << " foff " << foff << dendl;

  if (fd < 0){
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return 1;
  }

/**** alignment has to be made in buddystore 

  // foff 도 align 맞춰줘야 함.. 젠장.. 
  // 앞의 부분 없으면 채워서 보내줘야 함... 
  // 여기서 보내기 전에 BLK_SIZE (적어도 512바이트 단위) 로 size 맞춰줘야 함.
  uint64_t orig_len = bl.length();
  uint64_t align_len = round_up (bl.length());
  uint64_t align_foff = round_down (foff);
  ldout(cct, 10) << __func__ << " align_len " << align_len << " align_off " << align_foff << dendl;

  bufferlist abl;

  // 원래 여기서 기존 데이터 읽어와야 하는데.. 우선 걍 하자. 
  abl.append_zero(foff - align_foff);
  abl.claim_append(bl);
  abl.append_zero(align_len - orig_len);
*/

  if(directio){
    assert((foff % BUDDY_FALLOC_SIZE) == 0);
    assert((bl.length() % BUDDY_FALLOC_SIZE) == 0);
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);
  }

#if 0
  ldout(cct, 10) << __func__ << " dfd " << dfd << " abl.length(align) " << abl.length() << " align_foff " << align_foff << dendl;

  assert(abl.length() % BUDDY_FALLOC_SIZE == 0);
  assert(align_foff % BUDDY_FALLOC_SIZE == 0);
#endif

  utime_t start = ceph_clock_now(); 

  int ret = bl.write_fd(fd, foff);
    
  if (ret) {
    ldout(cct, 10) << __func__ <<  " Error in write "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }

  utime_t lat = ceph_clock_now();
  lat -= start;
  
  ldout(cct, 5) << __func__ << " data_file_write_fd lat " << lat << dendl;

  return 0;
}
#endif


/******
 * write_fd 
 */
    
int BuddyLogDataFileObject::write_fd(bufferlist& bl, uint64_t foff)
{
  ldout(cct, 5) << __func__ << " dfd " << dfd << " bl.length " << bl.length() << " foff " << foff << dendl;

  if (dfd < 0){
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return 1;
  }

/**** alignment has to be made in buddystore 

  // foff 도 align 맞춰줘야 함.. 젠장.. 
  // 앞의 부분 없으면 채워서 보내줘야 함... 
  // 여기서 보내기 전에 BLK_SIZE (적어도 512바이트 단위) 로 size 맞춰줘야 함.
  uint64_t orig_len = bl.length();
  uint64_t align_len = round_up (bl.length());
  uint64_t align_foff = round_down (foff);
  ldout(cct, 10) << __func__ << " align_len " << align_len << " align_off " << align_foff << dendl;

  bufferlist abl;

  // 원래 여기서 기존 데이터 읽어와야 하는데.. 우선 걍 하자. 
  abl.append_zero(foff - align_foff);
  abl.claim_append(bl);
  abl.append_zero(align_len - orig_len);
*/

  if(directio){
    assert((foff % BUDDY_FALLOC_SIZE) == 0);
    assert((bl.length() % BUDDY_FALLOC_SIZE) == 0);
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);
  }

#if 0
  ldout(cct, 10) << __func__ << " dfd " << dfd << " abl.length(align) " << abl.length() << " align_foff " << align_foff << dendl;

  assert(abl.length() % BUDDY_FALLOC_SIZE == 0);
  assert(align_foff % BUDDY_FALLOC_SIZE == 0);
#endif

  utime_t start = ceph_clock_now(); 

  int ret = bl.write_fd(dfd, foff);
    
  if (ret) {
    ldout(cct, 10) << __func__ <<  " Error in write "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }

  utime_t lat = ceph_clock_now();
  lat -= start;
  
  ldout(cct, 5) << __func__ << " data_file_write_fd lat " << lat << dendl;

  return 0;
}


/******
 * sync 
 */
    
void BuddyLogDataFileObject::sync()
{
  if (directio) {
    ldout(cct,10) << __func__ << " O_DSYNC " << dendl;
    return;
  }
  
  if (dfd < 0) {
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return;
  }
    
  int ret = 0;
#if defined(DARWIN) || defined(__FreeBSD__)
  ret = ::fsync(dfd);
#else
  ret = ::fdatasync(dfd);
#endif
  if (ret < 0) {
    //derr << __func__ << " fsync/fdatasync failed: " << cpp_strerror(errno) << dendl;
    ldout(cct, 10) << __func__ << " fsync/fdata sync failed " << dendl;
    ceph_abort();
  }

  return;
}



int BuddyLogDataFileObject::read(bufferlist& bl, uint64_t foff, size_t len)
{

  // open with buffered I/O 
  int flags = O_RDWR | O_CREAT;
   
  // open 
  int r = ::open(fname.c_str(), flags, 0644);
  if (r < 0){
    ldout(cct,10) << __func__ << "Failed to create file: " << fname << cpp_strerror(r) << dendl; 
    return r;
  }

  int fd = r;

  struct stat st;
  r = ::fstat(fd, &st);
    
  if (r < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return r;
  }
  ldout(cct, 10) << __func__ << " data_file size " << st.st_size << dendl;

  // read_fd  
  r = read_fd(bl, foff, len, fd);

  // close
  ::close(fd);

  return r;
}


/******
 * read_fd 
 */
int BuddyLogDataFileObject::read_fd(bufferlist& bl, uint64_t foff, size_t len, int fd)
{
  // 사실은 iov 를 받아서 처리해야 하는게 맞음. 
  // 다 따로따로 떨어져 있을테니까..

  ldout(cct,10) << __func__  << " foff " << foff << " len " << len << dendl;
  
  if (directio)
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);


  ldout(cct,10) << __func__ << " read_debug: fd " << fd << " foff " << foff << " len " << len << " bl.length() " << bl.length() << dendl;

  // for test 
  uint64_t blen = round_up(len);
  uint64_t boff = round_down(foff);
  bufferlist bbl;

  ldout(cct,10) << __func__ << " read_debug: boff " << boff << " blen " << blen << dendl;

  int ret = bbl.read_fd(fd, boff, blen);
  ldout(cct,10) << __func__ << " read_debug: direct read: ret " << ret << dendl; 

  ldout(cct,10) << __func__ << " read_debug: bbl.length() " <<  bbl.length() << " substr off " << foff - boff << 
    " len " << len << dendl; 
  bl.substr_of(bbl, foff - boff, len);
  ret = bl.length();

  //int ret = bl.read_fd(fd, foff, len);
  
  if (ret < static_cast<int>(len)) 
    ldout(cct, 10) << __func__ << "Error in read: " << cpp_strerror(ret) << dendl;

  return ret;
}



/******
 * read_fd 
 */
int BuddyLogDataFileObject::read_fd(bufferlist& bl, uint64_t foff, size_t len)
{
  // 사실은 iov 를 받아서 처리해야 하는게 맞음. 
  // 다 따로따로 떨어져 있을테니까..

  ldout(cct,10) << __func__  << " foff " << foff << " len " << len << dendl;
  
  if (directio)
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);


  ldout(cct,10) << __func__ << " read_debug: dfd " << dfd << " foff " << foff << " len " << len << " bl.length() " << bl.length() << dendl;

  // for test 
  uint64_t blen = round_up(len);
  uint64_t boff = round_down(foff);
  bufferlist bbl;

  ldout(cct,10) << __func__ << " read_debug: boff " << boff << " blen " << blen << dendl;

  int ret = bbl.read_fd(dfd, boff, blen);
  ldout(cct,10) << __func__ << " read_debug: direct read: ret " << ret << dendl; 

  ldout(cct,10) << __func__ << " read_debug: bbl.length() " <<  bbl.length() << " substr off " << foff - boff << 
    " len " << len << dendl; 
  bl.substr_of(bbl, foff - boff, len);
  ret = bl.length();

  //int ret = bl.read_fd(dfd, foff, len);
  
  if (ret < static_cast<int>(len)) 
    ldout(cct, 10) << __func__ << "Error in read: " << cpp_strerror(ret) << dendl;

  return ret;
}



/******
 * preallocate 
 */
int BuddyLogDataFileObject::preallocate(uint64_t offset, size_t len)
{
  ldout(cct, 10) << __func__ << " fd " << dfd << " offset " << offset << " len " << len << dendl;

//#ifdef CEPH_HAVE_FALLOCATE
  if (dfd < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return -1;
  }


//#if 0  // 이부분이 아무것도 아닌거 같은데 성능에 영향이 크듯. 
  int ret;

  // 파일 읽기. 
  // 만약 파일 끝을 preallocate 할거면 이렇게 해야함.  
  struct stat st;
  ret = ::fstat(dfd, &st);
    
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return -errno;
  }


  // read file size 
//  uint64_t fsize = st.st_size;
//  uint64_t soff = round_up(fsize - 1);

//  soff = soff < offset ? offset : soff;
//  assert(soff % BUDDY_FALLOC_SIZE == 0);

  //size_t alloc_bytes = 0;
  size_t alloc_unit = 1 << 20; // 1MB 
  alloc_unit = len < alloc_unit? len : alloc_unit;
    
  ret = ftruncate(dfd, len);
  //ret = fallocate(**fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, offset, len);
    
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " Failed to ftruncate" << dendl; 
    return -errno;
  } 

  ret = fallocate(dfd, 0, offset, len);
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " Failed to fallocate" << dendl; 
    return -errno;
  }

  ldout(cct, 10) << __func__ << " fd " << dfd << " offset " << offset << " len " << len << " succeed " << dendl;
  //ret = fallocate(dfd, 0, offset, (uint64_t)1 << 34); // 16G preallocate  
#if 0
  else {
      // ensure we extent file size, if needed
      if (offset + len > (uint64_t)st.st_size) {
	ret = ::ftruncate(**fd, offset + len);
	if (ret < 0) {
	  ret = -errno;
	  lfn_close(fd);
	  goto out;
	}
      }
    }
    lfn_close(fd);

    if (ret >= 0 && m_filestore_sloppy_crc) {
      int rc = backend->_crc_update_zero(**fd, offset, len);
      assert(rc >= 0);
    }

    if (ret == 0)
      goto out;  // yay!
    if (ret != -EOPNOTSUPP)
      goto out;  // some other error
//# endif
#endif
//#endif // 여기 
  return 0;
}


void BuddyLogDataFileObject::punch_hole_thread_entry()
{
  ldout(cct,4) << __func__ << dendl;


  punch_hole_lock.Lock();

  utime_t interval;
  interval.set_from_double(3000.0);

  while (!stop_punch_hole) {
    utime_t startwait = ceph_clock_now();

    if (!force_punch_hole) {
      ldout(cct,5) << __func__ << " waiting for interval " <<  interval << dendl;
      punch_hole_cond.WaitInterval(punch_hole_lock, interval);
    }
    if (force_punch_hole) {
      ldout(cct,5) << __func__ << " force_force_punch_hole " << dendl;
      force_punch_hole = false;
    }
    if (stop_punch_hole) {
      ldout(cct,5) << __func__ << " stop punch_hole_thread " << dendl;
      break;
    } else {
      utime_t woke = ceph_clock_now();
      woke -= startwait;
      ldout(cct,5) << __func__ << " woke up after " << woke << dendl;
    }

    punch_hole_lock.Unlock();

	//----- do punch_hole -----// 
	utime_t start = ceph_clock_now(); 

	ldout(cct, 5) << __func__ << " do_punch_hole " << dendl;
	// 저속으로 하려면 여기에서 interval 을 짧게줘서 
	// 특정 크기이상으로 write 할 때까지 계속 실행. 
	// 512KB 나 1M 를 2G 쓸때까지 수행 :

#if 0
  for(map<uint64_t, buddy_index_t>::const_iterator p = omap.index_map.begin();
      p != omap.index_map.end(); p++) {

    ldout(cct, 10) << __func__ << " free index : " << p->second << dendl;
    // add to free map
    auto r = free_index_map.insert(make_pair(p->second.foff, p->second.alloc_bytes));
    if(!r.second) {
      ldout(cct, 10) << __func__ << " free_index_map already contains buddy_index_t " << *p << dendl;
      assert(0);
    }

    // punch_hole : we might want to coalescing holes in the future..
    int ret = fallocate(dfd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, p->second.foff, p->second.alloc_bytes);

    if (ret < 0 ){
      ldout(cct, 10) << __func__ << " failed to punch_hole " << *p << dendl;
      assert(0);
    }
  }
#endif
	//slot_lock.Lock();
	lock.Lock();
	
	uint64_t punch_hole_size = 0;
	bool need_prewrite = false;
	int i; 
	//map<uint64_t, uint64_t> hole_map;
	list<uint64_t> holes; // length

	_stat_file();
	
	for (i = 0; i < BUDDY_SLOT_NUM; i++){

	  double util = static_cast<double>(slot_used_bytes[i]) / static_cast<double>(slot_limit_bytes[i]);
	  ldout(cct, 5) << " slot " << i << " slot_limit_bytes " << slot_limit_bytes[i] << dendl;
	  ldout(cct, 5) << " slot " << i << " slot_used_bytes " << slot_used_bytes[i] << dendl;
	  ldout(cct, 5) << " slot " << i << " utilization " << util << dendl;
  
	  if (util < low_util_ratio) {
		// punch_hole: 
		uint64_t hole = 1UL << 24; // 16MB

		slot_limit_bytes[i] = slot_limit_bytes[i] - hole;
		//uint64_t hole = slot_limit_bytes[i]*0.1;
		total_pool_bytes -= hole;
		
		holes.push_back(hole);

		ldout(cct, 5) << " shrink: slot " << i << " slot_limit_bytes " << slot_limit_bytes[i] << dendl;
	  }
	  if (util > high_util_ratio) {
		// 원래는 여기에서 체크해야함. prewrite reserve 해놓은거.. 
		// 근데 지금은 그냥 함. 
		// add limit size 
		slot_limit_bytes[i] += slot_limit_bytes[i];
		total_pool_bytes += slot_limit_bytes[i];

		ldout(cct, 5) << " increase: slot " << i << " slot_limit_bytes " << slot_limit_bytes[i] << dendl;
	  }
	}

	
	if ((total_reserved_bytes < total_pool_bytes) || 
	  ((total_reserved_bytes - total_pool_bytes) < 1UL << 30)){
	  need_prewrite = true;
	}

	_stat_file();

#ifdef PUNCH_HOLE_DATA_FILE
	lock.Unlock();

	// do punch 
	for(list<uint64_t>::iterator p = holes.begin();
	  p != holes.end();
	  p++){

	  struct stat st;
	  int r = ::fstat(dfd, &st);

	  if (r < 0) {
		ldout(cct,4) << __func__ << " File is not open" << dendl; 
		break;
	  }

	  ldout(cct,10) << __func__ << " tmp_file size " << st.st_size << dendl;
		
	  uint64_t pos = st.st_size - (*p);

	  r = fallocate(dfd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, pos, *p);
	  if (r < 0) {
		ldout(cct,4) << __func__ << " Failed to punch_hole" << dendl; 
		break;
	  }
	  punch_hole_size += *p;

	}

	utime_t lat = ceph_clock_now(); 
	lat -= start;

	ldout(cct,4) << __func__ << "time " << start << " lat " << lat << " punch_hole_size(MB) " << 
	  (punch_hole_size >> 20) << dendl;

	  
	if (need_prewrite){
		ldout(cct,5) << " need prewrite " << dendl;
		prewrite_lock.Lock();
		prewrite_cond.Signal();
		prewrite_lock.Unlock();
	}


    punch_hole_lock.Lock();
  }

#else

	  // open 
	  int flags = O_RDWR | O_CREAT | O_DIRECT;
	  string tmp_fname = fname + ".tmp";
	  int r = ::open(tmp_fname.c_str(), flags, 0644);

	  if (r < 0){
		  ldout(cct,4) << __func__ << "Failed to create file: " << tmp_fname << cpp_strerror(r) << dendl; 
		  break;
	  }	
	  
	  int fd = r;

	  struct stat st;
	  r = ::fstat(fd, &st);
    
	  if (r < 0) {
		ldout(cct,4) << __func__ << " File is not open" << dendl; 
		break;
	  }

	  ldout(cct,10) << __func__ << " tmp_file size " << st.st_size << dendl;
		
	  uint64_t pos = rand() % ((total_reserved_bytes - *p) - tail_off); 
	  //uint64_t pos = rand() % st.st_size; 

	  r = fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, pos, *p);
	  if (r < 0) {
		ldout(cct,4) << __func__ << " Failed to punch_hole" << dendl; 
		break;
	  }
	  punch_hole_size += *p;

	}

	utime_t lat = ceph_clock_now(); 
	lat -= start;

	ldout(cct,4) << __func__ << "time " << start << " lat " << lat << " punch_hole_size(MB) " << 
	  (punch_hole_size >> 20) << dendl;

	  
	if (need_prewrite){
		ldout(cct,5) << " need prewrite " << dendl;
		prewrite_lock.Lock();
		prewrite_cond.Signal();
		prewrite_lock.Unlock();
	}


    punch_hole_lock.Lock();
  }
#endif

  stop_punch_hole = false;
  punch_hole_lock.Unlock();

}
void BuddyLogDataFileObject::prewrite_thread_entry()
{
  ldout(cct, 4) << __func__ << dendl;

  uint64_t to_be_written = 1UL << 30;
  uint64_t written = 0;

  prewrite_lock.Lock();

  utime_t start, lat;
  utime_t interval;
  interval.set_from_double(30.0);

  while (!stop_prewrite) {
    utime_t startwait = ceph_clock_now();

    if (!force_prewrite) {
      ldout(cct,4) << __func__ << " waiting for interval " <<  interval << dendl;
      prewrite_cond.WaitInterval(prewrite_lock, interval);
    }
    if (force_prewrite) {
      ldout(cct,4) << __func__ << " force_index_sync " << dendl;
      force_prewrite = false;
    }
    if (stop_prewrite) {
      ldout(cct,4) << __func__ << " stop prewrite_thread " << dendl;
      break;
    } else {
      utime_t woke = ceph_clock_now();
      woke -= startwait;
      ldout(cct,4) << __func__ << " woke up after " << woke << dendl;
    }

    prewrite_lock.Unlock();

	if (interval == 30.0)
	  start = ceph_clock_now();	

	//-------- do prewrite ------- // 
	ldout(cct, 4) << __func__ << " do_prewrite " << dendl;
	int r = do_prewrite();
	if (r < 0) {
	  ldout(cct, 4) << __func__ << " Failed to prewrite " << dendl;
	  break;
	}
	written += r;
	ldout(cct, 4) << __func__ << " written(MB) " << (written >> 20) << dendl;

	if (written < to_be_written) {
	  interval.set_from_double(1.0);
	} else {
	  lat = ceph_clock_now();
	  lat -= start;
	  written = 0;
	  interval.set_from_double(30.0);
  
	  ldout(cct, 4) << __func__ << " time " << start << " lat " << lat << " prewrite_size(MB) " << (to_be_written >> 20) << dendl; 
	}
  
    prewrite_lock.Lock();

  }

  stop_prewrite = false;
  prewrite_lock.Unlock();

}

int BuddyLogDataFileObject::do_prewrite()
{

  ldout(cct,10) << __func__ << dendl;

  int flags = O_RDWR | O_CREAT | O_DIRECT;

  // open 
  string tmp_fname = fname + ".tmp";
  int r;
  r = ::open(tmp_fname.c_str(), flags, 0644);
  if (r < 0){
    ldout(cct,10) << __func__ << "Failed to create file: " << tmp_fname << cpp_strerror(r) << dendl; 
    return r;
  }

  int fd = r;

  struct stat st;
  r = ::fstat(fd, &st);
    
  if (r < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return r;
  }

  ldout(cct, 10) << __func__ << " tmp_file size " << st.st_size << dendl;

  // do prewrite ..  
  bufferlist bl;

  uint64_t prewrite_size = 1UL << (20 + bg_reclaim); // 1MB 
  uint64_t pos = st.st_size;

  bl.append_zero(prewrite_size);


  ldout(cct,10) << __func__ << " current data_file size " << pos << dendl;
      
  r = bl.write_fd(fd, pos);
  //r = write_fd(bl, pos);

  if (r < 0) {
	ldout(cct, 10) << __func__ << " error in write " << dendl;
	return 0; 
  }
  int written = bl.length();

  r = ::fstat(fd, &st);
  ldout(cct,10) << __func__ << " data_file_size " << st.st_size << dendl;

  ::close(fd);

  return written;

}
