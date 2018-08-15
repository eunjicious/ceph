#include "BuddyLogDataFileObject.h"


class BuddyLogDataFilePool {
  public:
	CephContext* cct;

	int	file_num;
	bool dio;
	bool prealloc;

	//vector<BuddyLogDataFileObject>& file_vec;
	map<int, BuddyLogDataFileObject&> file_map;

	int create_files(int file_num);
	int delete_files();
	int sync_files();

	BuddyLogDataObject& get_fo(int write_size);

	BuddyLogDataFilePool(CephContext* _cct, bool dio, bool prealloc): 
	  cct (_cct), 
	  file_num(11),
	{

	}
	~BuddyLogDataFilePool(){}		
};


