/*for scc algorithm
 */
//#include "pregel_app_hashmin.h"
//
//int main(int argc, char* argv[]){
//	init_workers();
//	pregel_hashmin("/toyFolder", "/toyOutput", true);
//	worker_finalize();
//	return 0;
//}


/*for sssp algorithm
 */
#include "pregelplus_similation_do.h"
#ifdef SIMULATION
#ifdef DEBUG2
int main2(int argc, char* argv[]){
#else
int main(int argc, char* argv[]){
#endif
#else
int main1(int argc, char* argv[]){
#endif
	init_workers();
	pregel_similation("/simgraph", "/simresult", false);
	worker_finalize();
	return 0;
}



