#include "basic/pregel-dev.h"
#include <string>
#include <vector>
using namespace std;
typedef unsigned int UINT_32;
typedef unsigned char UINT_8;
#define ABSENT_ELEMENT -1
//============================
//label
#define label_type char

//=================Query graph=========================
struct Query_Graph {
	vector<char> labels;
	vector<vector<VertexID> > queryVertexToEdges;
};
static Query_Graph q;
void init_Query() {
	q.labels.push_back('a');
	q.labels.push_back('b');
	q.labels.push_back('c');
	q.labels.push_back('d');
	vector<VertexID> temp;
	temp.push_back(1);
	temp.push_back(2);
	q.queryVertexToEdges.push_back(temp);
	temp.clear();
	temp.push_back(2);
	q.queryVertexToEdges.push_back(temp);
	temp.clear();
	temp.push_back(3);
	q.queryVertexToEdges.push_back(temp);
	temp.clear();
	temp.push_back(1);
	q.queryVertexToEdges.push_back(temp);
	temp.clear();
}
//=================support metric====================
vector<int> partialSupp;
void init(){
	init_Query();
	partialSupp.resize(q.labels.size(),0);
}

UINT_8 GetBit(UINT_32 number, UINT_32 index) {
	if (index < 0 || index > 31)
		return 0xff; //如果传入参数有问题，则返回0xff，表示异常
	return (number >> index) & 1UL;
}

struct CCValue_pregel {
	char label;
	int id;
	int inDegree;
	vector<VertexID> outNeighbors; //邻接顶点
	vector<VertexID> simcount; //子结点simulate数[*,*,*....]
	/*
	 * for simset, consider using bitmap to store the set to accelerate the process
	 */
	vector<VertexID> simset; //自身simulate结点{*,*...}
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v) {
	m << v.label;
	m << v.id;
	m << v.inDegree;
	m << v.outNeighbors;
	m << v.simcount;
	m << v.simset;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v) {
	m >> v.label;
	m >> v.id;
	m >> v.inDegree;
	m >> v.outNeighbors;
	m >> v.simcount;
	m >> v.simset;
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

	virtual void compute(MessageContainer & messages) {
		if (step_num() == 1) {
			/*
			 * initial the simcount array, the size of this array equals to the number of vertex
			 */
			for (int j = 0; j < q.labels.size(); j++) {
				value().simcount.push_back(value().inDegree);
			}
			/*put all the initialization here
			 *
			 *initial simset and increment the partialSupport
			 */
			for (int i = 0; i < q.labels.size(); ++i) {
				if (value().label == q.labels[i]){
					value().simset.push_back(i);
					partialSupp[i]++;
				}
			}
			/*
			 * setup the bitmap_msg
			 */
			int bitmap_msg = 0x0;
			for (int i = 0; i < q.labels.size(); i++) {
				if (q.labels[i] != value().label) {
					bitmap_msg |= 1 << i;
				}
			}
			/*
			 * print the bitmap message
			 */
			/*			printf("bitmap_msg from v %d is %d:", value().id,bitmap_msg);
			 for (int i = 31; i >= 0; i--) {
			 printf("%d", GetBit(bitmap_msg, i));
			 if (i % 8 == 0)
			 putchar(' ');
			 }
			 printf("\n");*/

			//broadcast message
			broadcast(bitmap_msg);
			vote_to_halt();
		} else {

			/*
			 * update the simcount array
			 * according to the recieved messages
			 */
			for (int i = 0; i < messages.size(); i++) {
				for (int j = 0; j < value().simcount.size(); j++) {
					if (GetBit(messages[i], j) == 1) {
						value().simcount[j]--;
						assert(value().simcount[j]>=0);
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
			for (int i = 0; i < value().simset.size(); i++) {
				int &sim_v = value().simset[i];
				if (sim_v == ABSENT_ELEMENT)
					continue; //Neglect the null element
				/*
				 * for element sim_v, check if this vertex can simulate it
				 *
				 * iterate over all the outNeighbors of sim_v, and check if the
				 * updated simcount can cover sim_v's outNeighbors
				 */
				for (int j = 0; j < q.queryVertexToEdges[sim_v].size(); j++) {
					if (value().simcount[q.queryVertexToEdges[sim_v][j]] == 0) {
						//simulation failed

						//first, setup the message
						trans_messages |= 1 << sim_v;
						//second, update partialSupp
						partialSupp[sim_v]--;
						//third, delete sim_v from simset
						sim_v = ABSENT_ELEMENT;
						break;
					}
				}
			}

			/*
			 * send message
			 */
			if (trans_messages != 0) {//broadcast only if there is message
				broadcast(trans_messages);
			}
			vote_to_halt();
		}
	}
};
//=============================Aggregator===================================
struct SimulationPartial{
	int vertexNum;
	int edgeNum;
	vector<int> matchcount;
};
struct SimulationFinal{
	int vertexNum;
	int edgeNum;
	vector<int> matchcount;
};
ibinstream & operator<<(ibinstream & m, const SimulationPartial & v){
	m<<v.vertexNum;
	m<<v.edgeNum;
	m<<v.matchcount;
	return m;
}
obinstream & operator>>(obinstream & m, SimulationPartial & v){
	m>>v.vertexNum;
	m>>v.edgeNum;
	m>>v.matchcount;
	return m;
}
ibinstream & operator<<(ibinstream & m, const SimulationFinal & v){
	m<<v.vertexNum;
	m<<v.edgeNum;
	m<<v.matchcount;
	return m;
}
obinstream & operator>>(obinstream & m, SimulationFinal & v){
	m>>v.vertexNum;
	m>>v.edgeNum;
	m>>v.matchcount;
	return m;
}
/*
 * each worker holds an Aggregator
 */
class CCAggregator_pregel:public Aggregator<CCVertex_pregel,SimulationPartial,SimulationFinal>{
public:
	/*
	 * initialized at each worker before each superstep
	 */
	virtual void init() {
		sum.vertexNum=0;
		sum.edgeNum=0;
		//sum.matchcount.clear();
		//sum.matchcount.push_back(1);
	}
	/*
	 * aggregate each computed vertex (after vertex compute)
	 */
    virtual void stepPartial(CCVertex_pregel* v)
    {
    	sum.edgeNum+=v->value().outNeighbors.size();
    	sum.vertexNum+=1;
    }
    /*
     * call when sync_agg by each worker (not master)
     * the returned value is gathered by the master to aggregate all the partial values
     */
    virtual SimulationPartial* finishPartial()
    {
    	sum.matchcount=partialSupp;
        return &sum;
    }
    /*
     * only called by the master, to agg worker_partial, each worker(not master) once
     */
    virtual void stepFinal(SimulationPartial* part)
    {
    	sum.vertexNum+=part->vertexNum;
    	sum.edgeNum+=part->edgeNum;
    	sum.matchcount=partialSupp;//deep copy?
    	for(int i=0;i<sum.matchcount.size();i++)
    		sum.matchcount[i]+=part->matchcount[i];
    }
    /*
     * called by the master to broadcast aggregated value after aggregator finished,
     * the final aggregated value can be accessed by each worker in next super_step with: void* getAgg()
     */
    virtual SimulationFinal* finishFinal()
    {
//    	cout<<"the sum.matchcount of size "<<sum.matchcount.size()<<" is:";
    	int supp=sum.matchcount[0];//supp value of this round. can be broadcast or for other use!
    	for(int i=0;i<sum.matchcount.size();i++){
//    		cout<<sum.matchcount[i]<<" ";
    		if(supp>sum.matchcount[i])
    			supp=sum.matchcount[i];
    	}
//    	cout<<endl;
//    	cout<<"The support now is "<<supp<<endl;
    	SimulationFinal * finalsum=(SimulationFinal*)getAgg();
    	finalsum->vertexNum=sum.vertexNum;
    	finalsum->edgeNum=sum.edgeNum;
//    	cout<<"vertex#"<<sum.vertexNum<<" edge#"<<sum.edgeNum<<" matchcount#"<<sum.matchcount[0]<<endl;

        return finalsum;
    }
private:
    SimulationPartial sum;
};
//=====================================================================
class CCWorker_pregel: public Worker<CCVertex_pregel,CCAggregator_pregel> {
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
		v->value().label = label[0];
		pch = strtok(NULL, " "); //outDegree
		int num = atoi(pch);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " "); //neighbor
			v->value().outNeighbors.push_back(atoi(pch));
		}
		pch = strtok(NULL, " "); //inDegree
		v->value().inDegree = atoi(pch);

		return v;
	}

	virtual void toline(CCVertex_pregel* v, BufferedWriter & writer) {
		/*
		 * this sprintf can be used only once,
		 * otherwise, the content will be overwritten by later call
		 */
		sprintf(buf, "vid:%d\t can_similate:%d \n", v->value().id,
				v->value().simset[0]);
		writer.write(buf);
	}
};
//=============================use no combiner==============================
class CCCombiner_pregel: public Combiner<VertexID> {
public:
	virtual void combine(VertexID & old, const VertexID & new_msg) {
	}
};



void pregel_similation(string in_path, string out_path, bool use_combiner) {
	init();
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	CCWorker_pregel worker;
	CCCombiner_pregel combiner;
	if (use_combiner)
		worker.setCombiner(&combiner);
	CCAggregator_pregel SimulationAggregator;
	worker.setAggregator(&SimulationAggregator);
	worker.run(param);
}
