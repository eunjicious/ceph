#ifndef CEPH_FILECONTAINEROS_H
#define CEPH_FILECONTAINEROS_H

#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "common/RWLock.h"
//#include "BuddyStore.h"
#include "include/compat.h"
#include "buddy_types.h"


//#include "FileContainer.h"

// FileContainer 를 사용해서 데이터를 저장할 때 필요한 부분을 여기에서 구현하면 되는 것임. 
// 예를 들면 submit_entry 를 FileJournal 이 제공해준다면 BuddyStore 에서 이러한 인터페이스를 사용해서 데이터 저장하는 함수 등 필요. 
// BDJourn/alingObjectStore 는 apply manager 하고 submit manager 를 사용해서 구현. 
// 이 클래스는 BuddyStore 에서 상속받을 것임. 
// 왜 apply_manager 와 submit_manager 로 묵었는지 알겠음. 
// 이렇게 하지않으면 이름이 겹쳐서 쓸수가 없음. 
// GenericJournal.h 는 안만들었음. container 자체를 하기 그래서. 

class FileContainerObjectStore {
private:
  CephContext* fc_cct;
  string fc_path;

protected: 
  //FileContainer* fc; 

  // file 을 저장하고 읽어오는 거 밖에 없을듯. 
  // write manager, read manager, 
  // 삭제라든가 생성 같은거는? 
  // op manager 로 한꺼번에 묶어 버리는 것도 좋을 듯. 
  // touch 는 journal 에만 쓰고 날림. object 에 실제 데이터 쓰지 않으면. 
  // op_zero 는 내부적으로 write 를함. 
  // op_trunctate 는?? 구현 안했음. 현재 코드는 그냥 size 를 줄였음. 
  // truncate op 는 지원할 필요있을듯 
  // 사실 object operation 만 지원하면 되는 듯. 
  // read, write, clone, truncate 

  void file_container_start(){}; // start 라는게 좀 그렇지만. thread.시작. 
  void file_container_stop(){}; // thread stop 시키고 sync 하고. 

  void file_container_write(){}; // submit_entry 

public:
  FileContainerObjectStore(CephContext* cct_, const std::string& path_) :
	fc_cct(cct_), fc_path(path_) {}

  ~FileContainerObjectStore() {}

};

#endif
