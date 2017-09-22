#include "gspan.h"
#include <unistd.h>

#define OPT " [-m minsup] [-d] [-e] [-w] "

void usage(void) {
	std::cout << "gspan implementation by Taku Kudo" << std::endl;
	std::cout << std::endl;
	std::cout << "usage: gspan [-m minsup] [-D] [-e] [-w] [-L maxpat]"
			<< std::endl;
	std::cout << std::endl;
	std::cout << "options" << std::endl;
	std::cout << "  -h, show this usage help" << std::endl;
	std::cout << "  -m minsup, set the minimum support (absolute count)"
			<< std::endl;
	std::cout << "  -D, use directed edges, default: undirected" << std::endl;
	std::cout << "  -e, output substructures in encoded form (?)" << std::endl;
	std::cout << "  -w, where (?)" << std::endl;
	std::cout << "  -L maxpat, the maximum number of outputted substructures"
			<< std::endl;
	std::cout
			<< "  -n minnodes, the minimum number of nodes in substructes (default: 0)"
			<< std::endl;
	std::cout << std::endl;

	std::cout
			<< "The graphs are read from stdin, and have to be in this format:"
			<< std::endl;
	std::cout << "t" << std::endl;
	std::cout << "v <vertex-index> <vertex-label>" << std::endl;
	std::cout << "..." << std::endl;
	std::cout << "e <edge-from> <edge-to> <edge-label>" << std::endl;
	std::cout << "..." << std::endl;
	std::cout << "<empty line>" << std::endl;
	std::cout << std::endl;

	std::cout
			<< "Indices start at zero, labels are arbitrary unsigned integers."
			<< std::endl;
	std::cout << std::endl;
}
#ifdef SIMULATION
#ifdef DEBUG2
int main(int argc, char **argv) {
#else
	int main1(int argc, char **argv) {
#endif
#else
	int main(int argc, char **argv) {
#endif
//	std::cout << "zjh:gspan started" << std::endl;

	freopen("./src/gSpan/input", "r", stdin); //redirect the stdin
//	std::string s;
//	while(std::cin>>s){
//		std::cout<<s<<" ";
//	}
//	exit(0);

	unsigned int minsup = 1;

	//maxpat_min(max): lower(upper) bound on node count.
	unsigned int maxpat = 1; //maxpat_min
	unsigned int minnodes = 4; //maxpat_max
	bool where = true;
	bool enc = false;
#ifdef DIRECTED
	bool directed = true;
#else
	bool directed = false;
#endif

	int opt;
	while ((opt = getopt(argc, argv, "edws::m:L:Dhn:")) != -1) {
		std::cout << "option " << (char) opt << std::endl;
		switch (opt) {
		case 's':
		case 'm':
			minsup = atoi(optarg);
			break;
		case 'n':
			minnodes = atoi(optarg);
			break;
		case 'L':
			maxpat = atoi(optarg);
			break;
		case 'd': // same as original gSpan
		case 'e':
			enc = true;
			break;
		case 'w':
			where = true;
			break;
		case 'D':
			directed = true;
			break;
		case 'h':
		default:
			usage();
			return -1;
		}
	}

	GSPAN::gSpan gspan;

#ifdef SIMULATION
	gspan.run(1, 1, 3, false, false, true);
#else
	gspan.run(std::cin, std::cout, minsup, maxpat, minnodes, enc, where,
			directed);
#endif
}
