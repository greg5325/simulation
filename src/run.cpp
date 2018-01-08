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
int main(int argc, char* argv[]) {
#else
	int main1(int argc, char* argv[]) {
#endif
	init_workers();

	if(get_worker_id() == MASTER_RANK){
		printf("number of args:%d\n",argc);
	    for (int i = 0; i<argc; i++){
	        cout << argv[i] << endl;
	    }
	}

#ifdef little
	pregel_similation("/simgraph", "/simresult", false);
#else
	pregel_similation("/input/twetter/vertexformat", "/output/twetter/simresult", false);
#endif
	worker_finalize();
	return 0;
}
