/*
 *for scc algorithm
 */
//#include "pregel_app_hashmin.h"
//
//int main(int argc, char* argv[]){
//	init_workers();
//	pregel_hashmin("/toyFolder", "/toyOutput", true);
//	worker_finalize();
//	return 0;
//}


/*
 *for sssp algorithm
 */
#include "pregelplus_similation_do.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_similation("/simgraph", "/simresult", false);
	worker_finalize();
	return 0;
}



