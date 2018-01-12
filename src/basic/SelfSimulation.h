#ifndef SelfSimulation
#define SelfSimulation

#include <vector>
using namespace std;

vector<unsigned int> selfsimulation(vector<char> & labels, vector<vector<int> > & queryVertexToEdges) {
	vector<unsigned int> prevsims;
	vector<unsigned int> sims;
	vector<unsigned int> removes;
	for (unsigned int v = 0; v < labels.size(); v++) {
		//init prevsim of v
		unsigned int prevsim = 0;
		for (unsigned int j = 0; j < labels.size(); j++) {
			prevsim |= 1 << j;
		}
		prevsims.push_back(prevsim);

		//init sim of v
		if (queryVertexToEdges[v].size() == 0) {
			unsigned int sim = 0;
			for (unsigned int u = 0; u < labels.size(); u++) {
				if (labels[v] == labels[u]) {
					sim |= 1 << u;
				}
			}
			sims.push_back(sim);
		} else {
			unsigned int sim = 0;
			for (unsigned int u = 0; u < labels.size(); u++) {
				if (labels[v] == labels[u]
						&& queryVertexToEdges[u].size() > 0) {
					sim |= 1 << u;
				}
			}
			sims.push_back(sim);
		}

		//init remove of v
		unsigned int pre_V = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init pre_V
			if (queryVertexToEdges[i].size() > 0) {
				pre_V |= 1 << i;
			}
		}

		unsigned int pre_sim = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init pre_sim_v
			unsigned int child = 0;
			for (unsigned int j = 0; j < queryVertexToEdges[i].size(); j++) {
				child |= 1 << queryVertexToEdges[i][j];
			}
			if (child & sims[v]) {
				pre_sim |= 1 << i;
			}
		}

		removes.push_back(pre_V & (~pre_sim)); //push the value of remove_v
	}

	bool exist = false;
	int v = -1;
	for (vector<unsigned int>::iterator it = removes.begin();
			it != removes.end(); it++) {
		exist = exist || *it;
		v++;
		if (exist)
			break;
	}

	while (exist) {
		for (unsigned int u = 0; u < labels.size(); u++) {
			unsigned int childs = 0;
			for (vector<int>::iterator child = queryVertexToEdges[u].begin();
					child != queryVertexToEdges[u].end(); child++) {
				childs |= 1 << *child;
			}
			if (childs & 1 << v) { //for all u in pre(v)
				for (unsigned int w = 0; w < labels.size(); w++) {
					if (removes[v] & 1 << w) { //for all w in remove(v)
						if (sims[u] & 1 << w) { //if w in sim(u)
							sims[u] &= ~(1 << w);

							for (unsigned int w__ = 0; w__ < labels.size();
									w__++) {
								unsigned int childs = 0;
								for (vector<int>::iterator child =
										queryVertexToEdges[w__].begin();
										child != queryVertexToEdges[w__].end();
										child++) {
									childs |= 1 << *child;
								}
								if (childs & 1 << w) { //for all w__ in pre(w)
									if (!(childs & sims[u])) {
										removes[u] |= 1 << w__;
									}
								}
							}
						}
					}
				}
			}
		}

		prevsims[v] = sims[v];
		removes[v] = 0;

		exist = false;
		v = -1;
		for (vector<unsigned int>::iterator it = removes.begin();
				it != removes.end(); it++) {
			exist = exist || *it;
			v++;
			if (exist)
				break;
		}
	}

	return sims;
}

vector<int> leastMatchCount(vector<unsigned int> & sims) {
	vector<int> least_match_counts;

	for (unsigned int v = 0; v < sims.size(); v++) {
		int least_match_count = 0;
		for (unsigned int i = 0; i < sims.size(); i++) {
			if (sims[v] & 1 << i) {
				least_match_count++;
			}
		}
		least_match_counts.push_back(least_match_count);
	}

	return least_match_counts;
}

vector<vector<int> > leastMatchCount2(vector<char> & labels, vector<vector<int> > & queryVertexToEdges){
	vector<unsigned int> prevsims;
	vector<unsigned int> sims;
	vector<unsigned int> removes;
	for (unsigned int v = 0; v < labels.size(); v++) {
		//init prevsim of v
		unsigned int prevsim = 0;
		for (unsigned int j = 0; j < labels.size(); j++) {
			prevsim |= 1 << j;
		}
		prevsims.push_back(prevsim);

		//init sim of v
		if (queryVertexToEdges[v].size() == 0) {
			unsigned int sim = 0;
			for (unsigned int u = 0; u < labels.size(); u++) {
				if (labels[v] == labels[u]) {
					sim |= 1 << u;
				}
			}
			sims.push_back(sim);
		} else {
			unsigned int sim = 0;
			for (unsigned int u = 0; u < labels.size(); u++) {
				if (labels[v] == labels[u]
						&& queryVertexToEdges[u].size() > 0) {
					sim |= 1 << u;
				}
			}
			sims.push_back(sim);
		}

		//init remove of v
		unsigned int pre_V = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init pre_V
			if (queryVertexToEdges[i].size() > 0) {
				pre_V |= 1 << i;
			}
		}

		unsigned int pre_sim = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init pre_sim_v
			unsigned int child = 0;
			for (unsigned int j = 0; j < queryVertexToEdges[i].size(); j++) {
				child |= 1 << queryVertexToEdges[i][j];
			}
			if (child & sims[v]) {
				pre_sim |= 1 << i;
			}
		}

		removes.push_back(pre_V & (~pre_sim)); //push the value of remove_v
	}

	bool exist = false;
	int v = -1;
	for (vector<unsigned int>::iterator it = removes.begin();
			it != removes.end(); it++) {
		exist = exist || *it;
		v++;
		if (exist)
			break;
	}

	while (exist) {
		for (unsigned int u = 0; u < labels.size(); u++) {
			unsigned int childs = 0;
			for (vector<int>::iterator child = queryVertexToEdges[u].begin();
					child != queryVertexToEdges[u].end(); child++) {
				childs |= 1 << *child;
			}
			if (childs & 1 << v) { //for all u in pre(v)
				for (unsigned int w = 0; w < labels.size(); w++) {
					if (removes[v] & 1 << w) { //for all w in remove(v)
						if (sims[u] & 1 << w) { //if w in sim(u)
							sims[u] &= ~(1 << w);

							for (unsigned int w__ = 0; w__ < labels.size();
									w__++) {
								unsigned int childs = 0;
								for (vector<int>::iterator child =
										queryVertexToEdges[w__].begin();
										child != queryVertexToEdges[w__].end();
										child++) {
									childs |= 1 << *child;
								}
								if (childs & 1 << w) { //for all w__ in pre(w)
									if (!(childs & sims[u])) {
										removes[u] |= 1 << w__;
									}
								}
							}
						}
					}
				}
			}
		}

		prevsims[v] = sims[v];
		removes[v] = 0;

		exist = false;
		v = -1;
		for (vector<unsigned int>::iterator it = removes.begin();
				it != removes.end(); it++) {
			exist = exist || *it;
			v++;
			if (exist)
				break;
		}
	}


	vector<vector<int> > least_match_counts;
	least_match_counts.resize(queryVertexToEdges.size());

	for(unsigned int src=0;src<queryVertexToEdges.size();src++){
		least_match_counts[src].resize(queryVertexToEdges[src].size(),0);
		for(unsigned int dst=0;dst<queryVertexToEdges[src].size();dst++){
			for(unsigned int i=0;i<queryVertexToEdges[src].size();i++){
				if(sims[queryVertexToEdges[src][dst]] & 1<<queryVertexToEdges[src][i]){
					least_match_counts[src][dst]++;
				}
			}
		}
	}
	return least_match_counts;
}




#endif
