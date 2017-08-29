#include "basic/pregel-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

struct CCValue_pregel
{
	int color;
	vector<VertexID> outNeighbors;
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v){
	m<<v.color;
	m<<v.outNeighbors;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v){
	m>>v.color;
	m>>v.outNeighbors;
	return m;
}

//====================================

class CCVertex_pregel:public Vertex<VertexID, CCValue_pregel, VertexID>
{
	public:
		void broadcast(VertexID msg)
		{
			vector<VertexID> & nbs=value().outNeighbors;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		void compute(MessageContainer & messages)
		{
//			cout<<"Worker #"<<get_worker_id()<<" is computing vertex @"<<id<<" in superstep #"<<step_num()<<"."<<endl;

			int maxDist=100;

			if(step_num()==1){
				value().color=maxDist;
			}

			int minDist = id==1 ? 0 : maxDist;
			for(int i=0; i<messages.size(); i++)
			{
				if(minDist>messages[i]) minDist=messages[i];
			}
			if (minDist < value().color) {
				value().color=minDist;
				broadcast(minDist+1);
			}
			vote_to_halt();
		}
};
//====================================
struct summary{
	int idsum;
	int colorsum;
};
ibinstream & operator<<(ibinstream & m, const summary & v){
	m<<v.idsum;
	m<<v.colorsum;
	return m;
}

obinstream & operator>>(obinstream & m, summary & v){
	m>>v.idsum;
	m>>v.colorsum;
	return m;
}
/*
 * each worker holds an Aggregator
 */
class CCAggregator_pregel:public Aggregator<CCVertex_pregel,summary,summary>{
public:
	/*
	 * initialized at each worker before each superstep
	 */
	virtual void init() {
		sum.idsum=0;
		sum.colorsum=0;
		cout<<"zjh:SP#"<<step_num()<<"W#"<<get_worker_id()<<"Agg.init"<<endl;
	}
	/*
	 * aggregate each computed vertex (after vertex compute)
	 */
    virtual void stepPartial(CCVertex_pregel* v)
    {
    	sum.colorsum+=v->value().color;
    	sum.idsum+=v->id;
    }
    /*
     * call when sync_agg by each worker (not master)
     * the returned value is gathered by the master to aggregate all the partial values
     */
    virtual summary* finishPartial()
    {
//    	cout<<"zjh:superstep #"<<step_num()<<"Worker #"<<get_worker_id()<<" finishPartial"<<endl;
        return &sum;
    }
    /*
     * only called by the master, to agg worker_partial, each worker(not master) once
     */
    virtual void stepFinal(summary* part)
    {
    	sum.idsum+=part->idsum;
    	sum.colorsum+=part->colorsum;
    }
    /*
     * called by the master to broadcast aggregated value after aggregator finished,
     * the final aggregated value can be accessed by each worker in next super_step with: void* getAgg()
     */
    virtual summary* finishFinal()
    {
        return &sum;
    }
private:
    summary sum;
};
//====================================
class CCWorker_pregel:public Worker<CCVertex_pregel,CCAggregator_pregel>
{
	char buf[100];

	public:
	/*
	 * User-defined Graph Loader, specified by users.
	 */
		//C version
		virtual CCVertex_pregel* toVertex(char* line)
		{
			char * pch;
			pch=strtok(line, "\t");
			CCVertex_pregel* v=new CCVertex_pregel;
			v->id=atoi(pch);
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().outNeighbors.push_back(atoi(pch));
			}
			return v;
		}
		/**
		 * User-defined GraphDumper, specified by users.
		 */
		virtual void toline(CCVertex_pregel* v, BufferedWriter & writer)
		{
			sprintf(buf, "%d\t%d\n", v->id, v->value().color);
			writer.write(buf);
		}
};

class CCCombiner_pregel:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};




void pregel_hashmin(string in_path, string out_path, bool use_combiner)
{
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	CCWorker_pregel worker;
	CCCombiner_pregel combiner;
	CCAggregator_pregel aggregator;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.setAggregator(&aggregator);
	worker.run(param);
}
