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
#include "pregel_app_SimpleSingleSourceShortestPath.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_hashmin("/toyTwitter", "/toyTwitterOutput4", true);
	worker_finalize();
	return 0;
}



