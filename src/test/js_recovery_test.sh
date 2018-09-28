#!/bin/bash -x
  
CEPH_DIR=/home/ubuntu/ceph
CEPH_BIN=$CEPH_DIR/build/bin
SAMPLE_DIR=/tmp/sample_dir

jsyeon_read_object() {
	start_ver=$1
	end_ver=$2
	num_objs=$3
	obj_size=$4
	cmp_list=()
	[ -d "${SAMPLE_DIR}" ] || die "must setup_tempdir"
	for v in `seq $start_ver $end_ver`; do	
		chr=`perl -e "print chr(48+$v)"`
		for i in `seq -w 1 $num_objs`; do
			head -c $obj_size /dev/zero  | tr '\0' "$chr" > $SAMPLE_DIR/out_"$i"_"$v"
			/home/ubuntu/ceph/build/bin/rados -p $pool get "obj$i"_"$v" $SAMPLE_DIR/out_"$i"_"$v" || die "radostool failed"
			echo "|" $(diff $SAMPLE_DIR/out_"$i"_"$v" $SAMPLE_DIR/in_"$i"_"$v" | wc -l )>> $SAMPLE_DIR/result.txt || die "got back incorrect obj$i"
		done
		echo "========================================" >> $SAMPLE_DIR/result.txt
	done	
}

jsyeon_start_recovery() {
	CEPH_NUM_OSD=$1
	osd=0
	while [ $osd -lt $CEPH_NUM_OSD ]; do
		ceph -c ./ceph.conf tell osd.$osd debug kick_recovery_wq 0
		osd=$((osd+1))
	done
}
poll_cmd(){
	command=$1
	search_str=$2
	polling_interval=$3
	total_time=$4
    
	t=0
	while [ $t -lt $total_time ]; do
		$command | grep "$search_str"
		[ $? -eq 0 ] && return 1
		sleep $polling_interval
		t=$(($t+polling_interval))
	done  
	return 0
}
jsyeon_restart_osd() {
	osd_index=$1
        #ceph-osd -i $osd_index -c /home/ubuntu/ceph.conf &
        #ceph-osd -i $osd_index  &
	/home/ubuntu/ceph/build/bin/ceph-osd -i $osd_index --osd-journal /dev/xvdb1 --osd-data /var/lib/ceph/osd/ceph-0 &
}
jsyeon_stop_osd() {
	osd_index=$1
	pidfile="osd.$osd_index.pid"
	pid=`ps -ef | grep "ceph-osd" | head -n 1 | awk '{print $2}'`
	echo $pid > $pidfile
	        if kill $pid ; then
	                #poll_cmd "eval test -e $pidfile ; echo \$?" "1" 1 30
	                #[ $? -eq 1 ] && return 0
                        #echo "ceph-osd process did not terminate correctly"
			echo "kill ceph-osd(pid) : $pid"
			sleep 4
			ps -ef | grep "ceph-osd" 
		else
			echo "kill `cat $pidfile` failed"
                fi
      
        return 1
}


jsyeon_write_object() {
	start_ver=$1
	end_ver=$2
	num_objs=$3
	obj_size=$4
	pool=$5
	mkdir $SAMPLE_DIR
	[ -d "${SAMPLE_DIR}" ] || die "sample dir don't make"
	for v in `seq $start_ver $end_ver`; do
		chr=`perl -e "print chr(48+$v)"`
		for i in `seq -w 1 $num_objs`; do
			head -c $obj_size /dev/zero | tr '\0' "$chr" > $SAMPLE_DIR/in_"$i"_"$v"
			/home/ubuntu/ceph/build/bin/rados -p $pool put "obj$i"_"$v" $SAMPLE_DIR/in_"$i"_"$v" || die "radostool failed"  
		done
	done	
}

jsyeon_recovery_impl() {
	echo "==============================="
	echo "jsyeon recovery function start!"

	sudo rm -r $SAMPLE_DIR
	
	##pool setting
	ceph osd pool set "rbd" size 1	

	jsyeon_write_object 0 20 10 1000 "rbd"
	echo "-----------stop osd 0 start----"
	jsyeon_stop_osd 0
	echo "-----------stop osd 0 end------"
	sleep 2 
	
	free
	sync
	echo 3 > /proc/sys/vm/drop_caches
    
	jsyeon_restart_osd 0  
	
	sleep 5

	jsyeon_read_object 0 20 10 1000

#	start_recovery 1 
    
	echo "jsyeon recovery function end!!!"
	echo "==============================="
}

jsyeon_recovery_impl
