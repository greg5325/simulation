#include "basic/pregel-dev.h"
#include <string>
#include <vector>
using namespace std;
typedef unsigned int UINT_32;
typedef unsigned char UINT_8;
#define ABSENT_ELEMENT -1
#define PRESENT_ELEMENT 1

//void init() {
//	init_Query();
//	partialSupp.resize(q.labels.size(), 0);
//}
UINT_8 GetBit(UINT_32 number, UINT_32 index) {
	if (index < 0 || index > 31)
		return 0xff; //如果传入参数有问题，则返回0xff，表示异常
	return (number >> index) & 1UL;
}

struct CCValue_pregel {
	char label;
	int id;
	int inDegree;
	vector<VertexID> inNeighbors;
	int outDegree;
	vector<VertexID> outNeighbors; //邻接顶点
	vector<vector<int> > simcountStack; //子结点simulate数[*,*,*....]
//TODO:for simset, consider using bitmap to store the set to accelerate the process
	vector<vector<VertexID> > simsetStack; //自身simulate结点{*,*...}
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v) {
	m << v.label;
	m << v.id;
	m << v.inDegree;
	m << v.inNeighbors;
	m << v.outDegree;
	m << v.outNeighbors;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v) {
	m >> v.label;
	m >> v.id;
	m >> v.inDegree;
	m >> v.inNeighbors;
	m >> v.outDegree;
	m >> v.outNeighbors;
	return m;
}

class CCVertex_pregel: public Vertex<VertexID, CCValue_pregel, int> {
public:
	void broadcast(int msg) {
		vector<VertexID> & nbs = value().outNeighbors;
		for (int i = 0; i < nbs.size(); i++) {
			send_message(nbs[i], msg);
		}
	}

	void Vnormalcompute(MessageContainer & messages) {
		vector<vector<VertexID> > &simsetStack = value().simsetStack;
		vector<vector<int> > &simcountStack = value().simcountStack;
		if (mutated) {
			//first, simcountStack and simsetStack
			simsetStack.resize(edges.size());
			if (simsetStack.size() > 1) {
				simsetStack[simsetStack.size() - 1] =
						simsetStack[simsetStack.size() - 2];
			}
			simsetStack[simsetStack.size() - 1].resize(q.labels.size());

			vector<VertexID> &simset = simsetStack[simsetStack.size() - 1];

			simcountStack.resize(edges.size());
			if (simcountStack.size() > 1) {
				simcountStack[simcountStack.size() - 1] =
						simcountStack[simcountStack.size() - 2];
			}
			simcountStack[simcountStack.size() - 1].resize(q.labels.size());
			vector<int> &simcount = simcountStack[simcountStack.size() - 1];

			vector<int> &partialSupp = partialSuppStack[partialSuppStack.size()
					- 1];
//			/*
//			 * initial the simcount array, the size of this array equals to the number of vertexes in q
//			 */

//			for (int j = 0; j < q.labels.size(); j++) {
//				value().simcount.push_back(value().inDegree);
//			}

			if (gspanMsg.fromlabel != -1) {
				simcount[gspanMsg.fromid] = value().inDegree;
			}
			if (gspanMsg.tolabel != -1) {
				simcount[gspanMsg.toid] = value().inDegree;
			}

//			 *initial simset and increment the partialSupport
//			 */
//			for (int i = 0; i < q.labels.size(); ++i) {
//				if (value().label == q.labels[i]) {
//					value().simset.push_back(i);
//					partialSupp[i]++;
//				}
//			}
			if (gspanMsg.fromlabel != -1) {
				if (value().label == gspanMsg.fromlabel) {
					simset[gspanMsg.fromid] = PRESENT_ELEMENT;
					partialSupp[gspanMsg.fromid]++;
				} else {
					simset[gspanMsg.fromid] = ABSENT_ELEMENT;
				}
			}
			if (gspanMsg.tolabel != -1) {
				if (value().label == gspanMsg.tolabel) {
					simset[gspanMsg.toid] = PRESENT_ELEMENT;
					partialSupp[gspanMsg.toid]++;
				} else {
					simset[gspanMsg.toid] = ABSENT_ELEMENT;
				}
			}
//			/*
//			 * setup the bitmap_msg
//			 */
//			int bitmap_msg = 0x0;
//			for (int i = 0; i < q.labels.size(); i++) {
//				if (q.labels[i] != value().label) {
//					bitmap_msg |= 1 << i;
//				}
//			}

			//----------------pack up msg for sim a--------------------------
			int bitmap_msg = 0x0;

			//for new a, we have to update according to itself
			if (simset[gspanMsg.fromid] == ABSENT_ELEMENT
					&& gspanMsg.fromlabel != -1) {
				bitmap_msg |= 1 << gspanMsg.fromid;
			}

			//update according to b
			if (simset[gspanMsg.fromid] == PRESENT_ELEMENT) {
//				for(int dst=0;dst<lea/)
				if (simcount[gspanMsg.toid] <leastmatchcounts[gspanMsg.fromid][gspanMsg.toid]) {
//				if (simcount[gspanMsg.toid] ==0) {
					simset[gspanMsg.fromid] = ABSENT_ELEMENT;
					partialSupp[gspanMsg.fromid]--;
					bitmap_msg |= 1 << gspanMsg.fromid;
				}
			}
			//---------------update sim b--------------------------
			//only need to update new b according to itself
			if (simset[gspanMsg.toid] == ABSENT_ELEMENT
					&& gspanMsg.tolabel != -1) {
				bitmap_msg |= 1 << gspanMsg.toid;
			}
//			/*
//			 * print the bitmap message
//			 */
//			/*			printf("bitmap_msg from v %d is %d:", value().id,bitmap_msg);
//			 for (int i = 31; i >= 0; i--) {
//			 printf("%d", GetBit(bitmap_msg, i));
//			 if (i % 8 == 0)
//			 putchar(' ');
//			 }
//			 printf("\n");*/

//			//broadcast message
			if (bitmap_msg != 0)
				broadcast(bitmap_msg);
			vote_to_halt();
		} else {

			//alias here
			vector<VertexID> &simset = simsetStack[simsetStack.size() - 1];
			vector<int> &simcount = simcountStack[simcountStack.size() - 1];
			vector<int> &partialSupp = partialSuppStack[partialSuppStack.size()
					- 1];
			/*
			 * update the simcount array
			 * according to the recieved messages
			 */
			for (int i = 0; i < messages.size(); i++) {
				for (int j = 0; j < simcount.size(); j++) {
					if (GetBit(messages[i], j) == 1) {
						simcount[j]--;
						assert(simcount[j]>=0);
					}
				}
			}
			/*
			 * update the simset and setup the message
			 * update the partialSupp if simset is updated
			 *
			 * consider using -1 to denote the absent element
			 */
			int trans_messages = 0x00;
			bool can_sim = true;
			for (int i = 0; i < simset.size(); i++) {
				if (simset[i] == ABSENT_ELEMENT)
					continue; //Neglect the null element
				/*
				 * for element i, check if this vertex can simulate it
				 *
				 * iterate over all the outNeighbors of i, and check if the
				 * updated simcount can cover i's outNeighbors
				 */
				for (int j = 0; j < q.queryVertexToEdges[i].size(); j++) {
					if (simcount[q.queryVertexToEdges[i][j]] < leastmatchcounts[i][q.queryVertexToEdges[i][j]]) {
//					if (simcount[q.queryVertexToEdges[i][j]] ==0) {
						//simulation failed

						//first, setup the message
						trans_messages |= 1 << i;
						//second, update partialSupp
						partialSupp[i]--;
						//third, delete sim_v from simset
						simset[i] = ABSENT_ELEMENT;
						break;
					}
				}
			}

			/*
			 * send message
			 */
			if (trans_messages != 0) { //broadcast only if there is message
				broadcast(trans_messages);
			}
			vote_to_halt();
		}
	}

	void Vpreprocessing(MessageContainer & messages) {
		if (preprocessSuperstep == 1) {
			vector<VertexID> & nbs = value().outNeighbors;
			for (int i = 0; i < nbs.size(); i++) {
				send_message(nbs[i], value().label);
			}
		} else if (preprocessSuperstep == 2) {
			//summarize the edge frequency in this partition
			for (MessageContainer::iterator it = messages.begin();
					it != messages.end(); ++it) {
				edgeFrequent[value().label][*it]++;
			}
		}
		vote_to_halt();
	}

	virtual void compute(MessageContainer & messages) {
		if (phase == preprocessing) {
			Vpreprocessing(messages);
		} else if (phase == normalcomputing) {
			Vnormalcompute(messages);
		}
	}
};
//=============================Aggregator==============================================================================
struct SimulationPartial {
	vector<int> matchcount;
};
struct SimulationFinal {
	vector<int> matchcount;
};
ibinstream & operator<<(ibinstream & m, const SimulationPartial & v) {
	m << v.matchcount;
	return m;
}
obinstream & operator>>(obinstream & m, SimulationPartial & v) {
	m >> v.matchcount;
	return m;
}
ibinstream & operator<<(ibinstream & m, const SimulationFinal & v) {
	m << v.matchcount;
	return m;
}
obinstream & operator>>(obinstream & m, SimulationFinal & v) {
	m >> v.matchcount;
	return m;
}

/* each worker holds an Aggregator
 */
class CCAggregator_pregel: public Aggregator<CCVertex_pregel, SimulationPartial,
		SimulationFinal> {
public:
	//initialized at each worker before each superstep
	virtual void init() {
	}
	//aggregate each computed vertex (after vertex compute)
	virtual void stepPartial(CCVertex_pregel* v) {
	}
	//call when sync_agg by each worker (not master)
	//the returned value is gathered by the master to aggregate all the partial values
	virtual SimulationPartial* finishPartial() {
		sum.matchcount = partialSuppStack[partialSuppStack.size() - 1]; //deep copy?
		return &sum;
	}
	//only called by the master, to agg worker_partial, each worker(not master) once
	virtual void stepFinal(SimulationPartial* part) {
		for (int i = 0; i < sum.matchcount.size(); i++)
			sum.matchcount[i] += part->matchcount[i];
	}
	//called by the master before broadcast aggregated value to workers,
	//the final aggregated value can be accessed by each worker in next super_step with: void* getAgg()
	virtual SimulationFinal* finishFinal() {
		supp = sum.matchcount[0]; //supp value of this round. can be broadcast or for other use!
		cout << "supVector:";
		for (int i = 0; i < sum.matchcount.size(); i++) {
			cout << sum.matchcount[i] << " ";
			supp = supp <= sum.matchcount[i] ? supp : sum.matchcount[i];
		}
		cout << endl;
		SimulationFinal * finalsum = (SimulationFinal*) getAgg();
		return finalsum;
	}
private:
	SimulationPartial sum;
};
//=====================================================================
class CCWorker_pregel: public Worker<CCVertex_pregel, CCAggregator_pregel> {
	char buf[100];

public:
	virtual CCVertex_pregel* toVertex(char* line) {
		/*
		 * format of the input graph:
		 * srcID	label outDegree N1 N2.... inDegree
		 */
		char * pch;
		pch = strtok(line, "\t"); //srcID
		CCVertex_pregel* v = new CCVertex_pregel;
		v->id = atoi(pch);
		v->value().id = v->id;
		pch = strtok(NULL, " "); //label
		char* label = pch;
#ifdef little
		v->value().label = label[0];
#else
		v->value().label = atoi(label);
#endif
		pch = strtok(NULL, " "); //outDegree
		v->value().outDegree=atoi(pch);
		for (int i = 0; i < v->value().outDegree; i++) {
			pch = strtok(NULL, " "); //neighbor
			v->value().outNeighbors.push_back(atoi(pch));
		}
		pch = strtok(NULL, " "); //inDegree
		v->value().inDegree = atoi(pch);
		for (int i=0;i< v->value().inDegree;i++){
			pch=strtok(NULL," ");
			v->value().inNeighbors.push_back(atoi(pch));
		}
		return v;
	}

	virtual void toline(CCVertex_pregel* v, BufferedWriter & writer) {
//		sprintf(buf, "vid:%d\t can_similate:%d \n", v->value().id,
//				v->value().simset[0]);
		writer.write(buf);
	}

};
//=============================use no combiner==============================
class CCCombiner_pregel: public Combiner<VertexID> {
public:
	virtual void combine(VertexID & old, const VertexID & new_msg) {
	}
};

void mine() {
	Worker<CCVertex_pregel, CCAggregator_pregel> * w = (Worker<CCVertex_pregel,
			CCAggregator_pregel>*) workercontext;
	w->looponsim();
}
void pregel_similation(string in_path, string out_path, bool use_combiner) {
//	init();
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	CCWorker_pregel worker;
	workercontext = &worker;
	CCCombiner_pregel combiner;
	if (use_combiner)
		worker.setCombiner(&combiner);
	CCAggregator_pregel SimulationAggregator;
	worker.setAggregator(&SimulationAggregator);
	worker.run(param);
}
