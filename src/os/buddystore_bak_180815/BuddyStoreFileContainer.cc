
// create container 
void BuddyStore::new_file_container()
{
  fc = new FileContainer(cct, basedir, this);
  return;
}

void BuddyStore::file_container_start()
{
  assert(fc != NULL);
  fc->mount();

}

void BuddyStore::file_container_stop()
{
  fc->sync();
  fc->umount();
}


struct C_FCWriteCompletion : public Context {
  BuddyStore *fs;
  BuddyStore::OpSequencer *osr;
  BuddyStore::Op *o;
  //Context *onfcwrite;

  //C_FCWriteCompletion(BuddyStore *f, BuddyStore::OpSequencer *os, BuddyStore::Op *o, Context *onfcwrite):
  C_FCWriteCompletion(BuddyStore *f, BuddyStore::OpSequencer *os, BuddyStore::Op *o):
    fs(f), osr(os), o(o) { }
  void finish(int r) override {
    fs->_finish_fcwrite(osr, o);
  }
};

//void BuddyStore::_finish_fcwrite(OpSequencer* osr, Op* o, Context* ondisk)
void BuddyStore::_finish_fcwrite(OpSequencer* osr, Op* o)
{
#if 0
  
  //  utime_t lat = ceph_clock_now();
  utime_t lat = ceph_clock_now();
  lat -= o->start;

  dout(5) << __func__ << " seq " << o->op << " lat " << lat << dendl; 


  int r = osr->dec_jcount(o->op);
  
  dout(10) << __func__ << " seq " << o->op << " jcount = " << r << dendl;  

  // jcount 체크하고 안되면 그냥 돌아감. 
  if (r > 0){
    return;
  }

  op_wq_lock.Lock();
  queue_op(osr, o);
  op_wq_lock.Unlock();

  // getting blocked behind an ondisk completion.
  assert(o->ondisk == ondisk);

  if (ondisk) {
    Mutex::Locker locker(ondisk_finisher_lock);
    dout(20) << __func__ << "finisher_queue" << dendl;
    ondisk_finisher.queue(ondisk);
  //  dout(10) << " queueing ondisk " << ondisk << dendl;
    //ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(ondisk);
  }

  list<Context*> to_queue;
  osr->dequeue_wait_ondisk(&to_queue);

  if (!to_queue.empty()) {
    Mutex::Locker locker(ondisk_finisher_lock);
    dout(20) << "to_queue is not empty" << dendl;
    ondisk_finisher.queue(to_queue);
    //ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(to_queue);
  }

  /// time 
  if (logger) {
    logger->tinc(l_buddystore_journal_all_latency, lat);
  }

  dout(5) << __func__ << " seq " << o->op << " container write complete lat " << lat << dendl; 
#endif

}

#if 0
// submit op 
//void BuddyStore::file_container_submit(vector<buddy_iov_t>& iov, 
//  OpSequencer *osr, Context* onfcwrite, TrackedOpRef osd_op)   
void BuddyStore::file_container_submit(vector<buddy_iov_t>& iov, 
  Context* onfcwrite, TrackedOpRef osd_op)   
{

  Mutex::Locker l(fc_lock);

  // 이건 여기에서 넣고 finisher 에서 뺴든가 
  // 아님 아예 추가하지 말든가 
  //osr->queue_data(o);

  //Op* o = osr->pop_queue_data();
  assert(o->tls_iov.size() > 0);

  //submit_entry(vector<buddy_iov_t>& iov, Context *onwrite, TrackedOpRef osd_op = TrackedOpRef());
  fc->submit_entry(iov, onfcwrite, osd_op);

}
#endif
