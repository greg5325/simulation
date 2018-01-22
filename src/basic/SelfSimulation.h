#ifndef SelfSimulation
#define SelfSimulation

#include <vector>
using namespace std;

vector<unsigned int> selfsimulation(vector<char> & labels,
		vector<vector<int> > & queryVertexToEdges) {
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

vector<unsigned int> selfdualsimulation(vector<char> & labels,
		vector<vector<int> > & outEdges, vector<vector<int> > & inEdges) {

	vector<unsigned int> prevsims;
	vector<unsigned int> sims;
	vector<unsigned int> removes_pre;
	vector<unsigned int> removes_suc;

	for (unsigned int v = 0; v < labels.size(); v++) {
		//init prevsim of v
		unsigned int prevsim = 0;
		for (unsigned int j = 0; j < labels.size(); j++) {
			prevsim |= 1 << j;
		}
		prevsims.push_back(prevsim);

		//init sim of v
		unsigned int sim = 0;
		if (outEdges[v].size() == 0) {
			if (inEdges[v].size() == 0) {
				for (unsigned int u = 0; u < labels.size(); u++)
					if (labels[v] == labels[u])
						sim |= 1 << u;
			} else {
				for (unsigned int u = 0; u < labels.size(); u++)
					if (labels[v] == labels[u] && inEdges[u].size() > 0)
						sim |= 1 << u;
			}
		} else {
			if (inEdges[v].size() == 0) {
				for (unsigned int u = 0; u < labels.size(); u++)
					if (labels[v] == labels[u] && outEdges[u].size() > 0)
						sim |= 1 << u;
			} else {
				for (unsigned int u = 0; u < labels.size(); u++)
					if (labels[v] == labels[u] && outEdges[u].size() > 0
							&& inEdges[u].size() > 0)
						sim |= 1 << u;
			}
		}
		sims.push_back(sim);

		//init remove_pre of v
		unsigned int pre_V = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init pre_V
			if (outEdges[i].size() > 0) {
				pre_V |= 1 << i;
			}
		}

		unsigned int pre_sim = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init pre_sim_v
			unsigned int child = 0;
			for (unsigned int j = 0; j < outEdges[i].size(); j++) {
				child |= 1 << outEdges[i][j];
			}
			if (child & sims[v]) {
				pre_sim |= 1 << i;
			}
		}

		removes_pre.push_back(pre_V & (~pre_sim)); //push the value of remove_pre_v

		//init remove_suc of v
		unsigned int suc_V = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init suc_V
			if (inEdges[i].size() > 0) {
				suc_V |= 1 << i;
			}
		}

		unsigned int suc_sim = 0;
		for (unsigned int i = 0; i < labels.size(); i++) { //init suc_sim_V
			if (sims[v] & 1 << i) {
				for (unsigned int j = 0; j < outEdges[i].size(); j++) {
					suc_sim |= 1 << outEdges[i][j];
				}
			}
		}
		removes_suc.push_back(suc_V & (~suc_sim)); //push the value of remove_suc_v
	}

//	vector<unsigned int> prevsims;
//	vector<unsigned int> sims;
//	vector<unsigned int> removes_pre;
//	vector<unsigned int> removes_suc;

	bool exist = false;
	int v;
	for (unsigned int i = 0; i < removes_pre.size(); i++) {
		exist = exist || removes_pre[i] || removes_suc[i];
		v = i;
		if (exist)
			break;
	}

	while (exist) {

//		printf("========================while start=======================");
//
//		for (int i = 0; i < sims.size(); i++) {
//			printf("sims of %d:", i);
//			for (int j = 0; j < labels.size(); j++) {
//				if (sims[i] & 1 << j) {
//					printf("%d ", j);
//				}
//			}
//			printf("\n");
//		}
//		printf("=============sims end=============\n\n");
//
//		for (int i = 0; i < removes_pre.size(); i++) {
//			printf("remove_pre of %d:", i);
//			for (int j = 0; j < labels.size(); j++) {
//				if (removes_pre[i] & 1 << j) {
//					printf("%d ", j);
//				}
//			}
//			printf("\n");
//		}
//		printf("===========removes_pre end===========\n\n");
//
//		for (int i = 0; i < removes_pre.size(); i++) {
//			printf("remove_suc of %d:", i);
//			for (int j = 0; j < labels.size(); j++) {
//				if (removes_suc[i] & 1 << j) {
//					printf("%d ", j);
//				}
//			}
//			printf("\n");
//		}
//		printf("===========removes_suc end===========\n\n");

		if (removes_pre[v]) {
			for (unsigned int u = 0; u < labels.size(); u++) {
				unsigned int childs = 0;
				for (vector<int>::iterator child = outEdges[u].begin();
						child != outEdges[u].end(); child++) {
					childs |= 1 << *child;
				}
				if (childs & 1 << v) { //for all u in pre(v)
					for (unsigned int w = 0; w < labels.size(); w++) {
						if (removes_pre[v] & 1 << w) { //for all w in remove(v)
							if (sims[u] & 1 << w) { //if w in sim(u)
								sims[u] &= ~(1 << w);

								for (unsigned int w__ = 0; w__ < labels.size();
										w__++) {
									unsigned int childs = 0;
									for (vector<int>::iterator child =
											outEdges[w__].begin();
											child != outEdges[w__].end();
											child++) {
										childs |= 1 << *child;
									}
									if (childs & 1 << w) { //for all w__ in pre(w)
										if (!(childs & sims[u])) {
											removes_pre[u] |= 1 << w__;
										}
									}
								}

								for (unsigned int w__index = 0;
										w__index < outEdges[w].size();
										w__index++) {
									unsigned int w__ = outEdges[w][w__index];
									unsigned int parents = 0;

									for (vector<int>::iterator parent =
											inEdges[w__].begin();
											parent != inEdges[w__].end();
											parent++) {
										parents |= 1 << *parent;
									}

									if (!(parents & sims[u])) {
										removes_suc[u] |= 1 << w__;
									}

								}
							}
						}
					}
				}
			}

			prevsims[v] = sims[v];
			removes_pre[v] = 0;
		} else {
			for (unsigned int u_index = 0; u_index < outEdges[v].size();
					u_index++) { //for all u in suc(v)
				unsigned int u = outEdges[v][u_index];
				for (unsigned int w = 0; w < labels.size(); w++) {
					if (removes_suc[v] & 1 << w) { //for all w in remove(v)
						if (sims[u] & 1 << w) { //w simulate u
							sims[u] &= ~(1 << w);

							for (unsigned int w__ = 0; w__ < labels.size();
									w__++) {
								unsigned int childs = 0;
								for (vector<int>::iterator child =
										outEdges[w__].begin();
										child != outEdges[w__].end(); child++) {
									childs |= 1 << *child;
								}
								if (childs & 1 << w) { //for all w__ in pre(w)
									if (!(childs & sims[u])) {
										removes_pre[u] |= 1 << w__;
									}
								}
							}

							for (unsigned int w__index = 0;
									w__index < outEdges[w].size(); w__index++) {
								unsigned int w__ = outEdges[w][w__index];
								unsigned int parents = 0;

								for (vector<int>::iterator parent =
										inEdges[w__].begin();
										parent != inEdges[w__].end();
										parent++) {
									parents |= 1 << *parent;
								}

								if (!(parents & sims[u])) {
									removes_suc[u] |= 1 << w__;
								}
							}
						}
					}
				}
			}
			prevsims[v] = sims[v];
			removes_suc[v] = 0;
		}

		exist = false;
		for (unsigned int i = 0; i < removes_pre.size(); i++) {
			exist = exist || removes_pre[i] || removes_suc[i];
			v = i;
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

vector<vector<int> > leastMatchCount2(vector<char> & labels,
		vector<vector<int> > & queryVertexToEdges) {
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

	for (unsigned int src = 0; src < queryVertexToEdges.size(); src++) {
		least_match_counts[src].resize(queryVertexToEdges.size(), 0);
		for (unsigned int dst = 0; dst < queryVertexToEdges[src].size();
				dst++) {
			for (unsigned int i = 0; i < queryVertexToEdges[src].size(); i++) {
				if (sims[queryVertexToEdges[src][dst]]
						& 1 << queryVertexToEdges[src][i]) {
					least_match_counts[src][queryVertexToEdges[src][dst]]++;
				}
			}
		}
	}
	return least_match_counts;
}

vector<vector<vector<int> > > leastDualMatchCount(vector<char> & labels,
		vector<vector<int> > & outEdges, vector<vector<int> > & inEdges) {

	vector<unsigned int> sims = selfdualsimulation(labels, outEdges, inEdges);

	vector<vector<vector<int> > > least_match_counts;
	least_match_counts.resize(2);

	vector<vector<int> > & least_pre_match_counts = least_match_counts[0];
	least_pre_match_counts.resize(outEdges.size());

	for (unsigned int src = 0; src < outEdges.size(); src++) {
		least_pre_match_counts[src].resize(outEdges.size(), 0);
		for (unsigned int dst = 0; dst < outEdges[src].size(); dst++) {
			for (unsigned int i = 0; i < outEdges[src].size(); i++) {
				if (sims[outEdges[src][dst]] & 1 << outEdges[src][i]) {
					least_pre_match_counts[src][outEdges[src][dst]]++;
				}
			}
		}
	}

	vector<vector<int> > & least_suc_match_counts = least_match_counts[1];
	least_suc_match_counts.resize(inEdges.size());

	for (unsigned int src = 0; src < inEdges.size(); src++) {
		least_suc_match_counts[src].resize(inEdges.size(), 0);
		for (unsigned int dst = 0; dst < inEdges[src].size(); dst++) {
			for (unsigned int i = 0; i < inEdges[src].size(); i++) {
				if (sims[inEdges[src][dst]] & 1 << inEdges[src][i]) {
					least_suc_match_counts[src][inEdges[src][dst]]++;
				}
			}
		}
	}

	return least_match_counts;
}

#endif
