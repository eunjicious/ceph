// data_file codes 


// 이게 OpSequencer 를 공유 해야하기 때문에 결국 thread 는 buddystore 에서 돌려야 함. 
// osr 에 있는 Op를 꺼내면 거기에 tls 가 있고, iov 가 있음. 
// iov 를 합쳐서 io 수행. 
// object 의 개념보다는 buddy_iov_t 로 통신. 
// merge 하는 부분 

    // -- data_file_map --
    // 사실 이게.. buddy_index_map_t 를 Object 에 넣으면 되는건데
    map<ghobject_t, buddy_index_map_t> data_file_index_map; // data_file_index_map  

    int data_file_insert_index(const ghobject_t& oid, const off_t ooff, const off_t foff, const ssize_t bytes); 
    int data_file_get_index(const ghobject_t& oid, const off_t ooff, const ssize_t bytes, vector<buddy_iov_t>& iov);


    void encode_index(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(data_file_index_map, bl);
      ENCODE_FINISH(bl);
    }
    void decode_index(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(data_file_index_map, p);
      DECODE_FINISH(p);
    }


  // -- data file --
  bool data_directio;
  bool data_flush;
  bool data_sync;
  bool data_hold_in_memory;
  bool file_prewrite;
  bool file_inplace_write;

  uint64_t last_data_file_seq;

  BuddyLogDataFileObject data_file;
  FileContainerObject* fcon;

  bool debug_read



  ///////////////////////////////////////
  
  // 1. init 

  // 2. sync : umount 할때 그냥 해당 file 에 대해서 (directory?) fsync 호출.    


  void BuddyStore::generate_iov_data_bl(vector<buddy_iov_t>& iov, bufferlist& bl, uint32_t start_off)
{
 

	  int ret = data_file.alloc_space(cid, oid, off, len, op_iov);
