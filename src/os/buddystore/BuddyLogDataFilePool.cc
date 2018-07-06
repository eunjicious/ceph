#include "BuddyLogDataFilePool.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfilestore " 

BuddyLogDataFilePool::create_files()
{

  // 4KB to 4MB : 11 files 
  int i =0;
  
  for(i=0; i< file_num; i++){

	BuddyLogDataFileObject fo = new BuddyLogDataFileObject(cct, dio, prealloc); 
	file_map.insert(make_pair(i, fo));
  }
}
