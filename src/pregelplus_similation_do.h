
//ecliseuiboiajfdojalfkdsbug???????!!!!!!!!!!!!!!!!!!!!
//#include stdio
#include "basic/pregel-dev.h"
#include <string>
#include <vector>
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

//Query graph
struct Query_Graph{
//	VertexID size;
	vector<char> labdlkfjel;
//	vector<VertexID> Qid;
	vector<vector<VertexID> > bugd;
};

static Query_Graph graph;
static VertexID sum_num=0;

class init_Query{
public:
	init_Query(){
//		graph.size=4;
		graph.labdlkfjel.push_back('a');
		graph.labdlkfjel.push_back('b');
		graph.labdlkfjel.push_back('c');
		graph.labdlkfjel.push_back('d');
		vector<VertexID> temp;
		temp.push_back(1);
		temp.push_back(2);
		graph.bugd.push_back(temp);
		temp.clear();
//		graph.Qid.push_back(1);
		temp.push_back(2);
		graph.bugd.push_back(temp);
//		graph.Qid.push_back(2);
		temp.clear();
		temp.push_back(3);
		graph.bugd.push_back(temp);
		temp.clear();
//		graph.Qid.push_back(3);
		temp.push_back(1);
		graph.bugd.push_back(temp);
		temp.clear();
//		graph.Qid.push_back(4);

		for(int i=0;i<graph.labdlkfjel.size();i++){
			printf("label=%c Q_id=%d edge=",graph.labdlkfjel[i],i);
			for(int j=0;j<graph.bugd[i].size();j++){
				printf("%d ",graph.bugd[i][j]);
			}
			printf("\n");
		}
	}
};

struct CCValue_pregel
{
	char label;
	int D_id;
	int outedge_num;
	vector<VertexID> edgdklsjfes;//邻接顶点
	vector<VertexID> simudlfjsdjflate;//子结点simulate数[*,*,*....]
	vector<VertexID> can_sidkfjskldjfmulate;//自身simulate结点{*,*...}
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v){
	m<<v.label;
	m<<v.D_id;
	m<<v.outedge_num;
	m<<v.edgdklsjfes;
	m<<v.simudlfjsdjflate;
	m<<v.can_sidkfjskldjfmulate;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v){
	m>>v.label;
	m>>v.D_id;
	m>>v.outedge_num;
	m>>v.edgdklsjfes;
	m>>v.simudlfjsdjflate;
	m>>v.can_sidkfjskldjfmulate;
	return m;
}

class CCVertex_pregel:public Vertex<VertexID, CCValue_pregel, vector<VertexID> >
{
	public:
		void broadcast(vector<VertexID> msg)
		{
			vector<VertexID> & nbs=value().edgdklsjfes;
			for(int i=0; i<nbs.size(); i++)
			{
				send_message(nbs[i], msg);
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			if(step_num()==1)//超步
			{

				for(int j=0;j<graph.labdlkfjel.size();j++){
				value().simudlfjsdjflate.push_back(value().outedge_num);
				}

//				vector<vector<VertexID> >::iterator iter;
//				for(iter=value().can_simulate.begin();iter!=value().can_simulate.end();iter++){
//				vector<VertexID> &nps=*iter;
				//print sum_num
				vector<VertexID> broad;
				printf("%d\n",sum_num);
				bool can_sim=false;
					for(int i=0;i<graph.bugd.size();i++){
						can_sim=false;
						vector<VertexID> &nps=value().can_sidkfjskldjfmulate;
						for(int j=0; j<nps.size(); j++){
							if(i==nps[j])can_sim=true;
						}
						if(!can_sim){
//							printf("i=:%d",i);
							broad.push_back(i);
						}
					}
//				}
				broadcast(broad);
				vote_to_halt();
			}
//-----------明天添加当结点不能simulate时，删除其value（）下的simulate数组---------------
			else
			{
//				for(MessageIter=messages.begin();MessageIter!=messages.end();MessageIter++){
//					int a=*MessageIter;
//					printf("minus=%d ",a);
//				}
//				printf("size=%d ",messages[0].size());
//				for(int i=0;i<messages[0].size();i++){
//					printf("%d ",messages[0][i]);
//				}
//				printf("\n");
				vector<vector<VertexID> >::iterator iter1;
				vector<VertexID>::iterator iter;
				for(iter1=messages.begin();iter1!=messages.end();iter1++){
						vector<VertexID> min=*iter1;
						for(iter=min.begin();iter!=min.end();iter++){
							VertexID minus=*iter;
							printf("minus=%d ",minus);
							value().simudlfjsdjflate[minus]--;
						}
				}
				bool can_sim=true;
				vector<VertexID> broad;
				for(int j=0;j<value().can_sidkfjskldjfmulate.size();j++){
					can_sim=true;
					int judge=value().can_sidkfjskldjfmulate[j];
					if(judge!=404){
						for(int p=0;p<graph.bugd[judge].size();p++){
						int j=graph.bugd[judge][p];
						if(value().simudlfjsdjflate[j]<=0)can_sim=false;
						}
					}
					if(!can_sim){
						broad.push_back(judge);
						value().can_sidkfjskldjfmulate[j]=404;
					}
				}
				//print
				vector<VertexID>::iterator iter4;
							printf("id=:%d\t simulate:",value().D_id);
							for(iter4=value().simudlfjsdjflate.begin();iter4!=value().simudlfjsdjflate.end();iter4++){
								VertexID tmp4=*iter4;
								printf("%d ",tmp4);
							}
				printf("after can_simulate:  ");
				vector<VertexID>::iterator iter2;
						for(iter2=value().can_sidkfjskldjfmulate.begin();iter2!=value().can_sidkfjskldjfmulate.end();iter2++){
							VertexID tmp2=*iter2;
							printf("%d ",tmp2);
						}
						printf("\n");
						if(broad.size()>0){
				broadcast(broad);
						}
				vote_to_halt();
			}
		}
};

class CCWorker_pregel:public Worker<CCVertex_pregel>
{
	char buf[100];

	public:
		//C version
		virtual CCVertex_pregel* toVertex(char* line)//worker.h->load_graph
		{
			char * pch;
			pch=strtok(line, "\t");
			CCVertex_pregel* v=new CCVertex_pregel;
			v->id=atoi(pch);//v->id保存制表符（/t）之前的数据
			v->value().D_id=v->id;
			pch=strtok(NULL, " ");
			char* label=pch;//label保存顶点种类
			v->value().label=label[0];
//*********************************************************************************
//-------------------------can_simulate为二维数组时----------------------------------
//			vector<vector<VertexID> >::iterator iter2;
//			int p=0;
//			for(iter1=graph.label.begin();iter1!=graph.label.end();iter1++){
//				iter2=graph.Q_id.begin();
//				char tmp1=*iter1;
//				if(tmp1==atoi(pch)){
//					for(int q=0;q<p;q++){
//						iter2++;}
//				vector<VertexID> tmp2=*iter2;
//				v->value().can_simulate.push_back(tmp2);
//				}p++;
//			}
//********************************************************************************
//-------------------------can_simulate为1维数组时----------------------------------
			vector<char>::iterator iter1;
			int p=0;
			for(iter1=graph.labdlkfjel.begin();iter1!=graph.labdlkfjel.end();iter1++){
				char tmp1=*iter1;
				if(tmp1==label[0]){
				v->value().can_sidkfjskldjfmulate.push_back(p);
				}p++;
			}
//********************************************************************************
			pch=strtok(NULL, " ");
			int num=atoi(pch);//num保存邻接点个数
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				v->value().edgdklsjfes.push_back(atoi(pch));//v->value保存每个邻接顶点的ID
			}
			pch=strtok(NULL, " ");//在邻接点后面插入顶点出度
			v->value().outedge_num=atoi(pch);

//print
//			//print D_id
//			cout<<"D_id="<<v->value().D_id<<endl;
//			//print label
//			cout<<"label"<<v->value().label<<endl;
//			//print can_simulate
//			printf("can_simulate:");
//			vector<VertexID>::iterator iter2;
//			for(iter2=v->value().can_simulate.begin();iter2!=v->value().can_simulate.end();iter2++){
//				VertexID tmp2=*iter2;
//				printf("%d ",tmp2);
//			}
//			//print num
//			printf("num=%d ",num);
//			//print edges
//			vector<VertexID>::iterator iter3;
//			printf("edges: ");
//			for(iter3=v->value().edges.begin();iter3!=v->value().edges.end();iter3++){
//				VertexID tmp3=*iter3;
//				printf("%d ",tmp3);
//			}
//			//print simulate
//			vector<VertexID>::iterator iter4;
//			printf("simulate:");
//			for(iter4=v->value().simulate.begin();iter4!=v->value().simulate.end();iter4++){
//				VertexID tmp4=*iter4;
//				printf("%d ",tmp4);
//			}
//			printf("\n");

			sum_num++;
			return v;
		}

		virtual void toline(CCVertex_pregel* v, BufferedWriter & writer)
		{
//			vector<VertexID>::iterator iter;
//			iter=v->value().can_simulate.begin();
//			VertexID id=*iter;
			sprintf(buf, "vid:%d\t can_similate:%d \n", v->value().D_id,v->value().can_sidkfjskldjfmulate[0]);
//			sprintf(buf,"cansim:");
//				for(int i=0;i<v->value().can_simulate.size();i++){
//			sprintf(buf, "%d\t", v->value().can_simulate[i]);
//			}
			writer.write(buf);
		}
};

class CCCombiner_pregel:public Combiner<vector<VertexID> >
{
	public:
		virtual void combine(vector<VertexID> & old,const vector<VertexID> & new_msg)
		{
//			vector<VertexID>::iterator it_old;
			vector<VertexID>::const_iterator it_new;
			for(it_new=new_msg.begin();it_new!=new_msg.end();++it_new){
				VertexID co=*it_new;
				old.push_back(co);
			}
//			for(int i=0;i<q;i++){
//				VertexID co=new_msg.at(i);
//				old[count]+=co;
//				count++;
//			}
		}
};


void pregel_similation(string in_path, string out_path, bool use_combiner)
{
	init_Query();
	WorkerParams param;
	param.input_path=in_path;
	param.output_path=out_path;
	param.force_write=true;
	param.native_dispatcher=false;
	CCWorker_pregel worker;
	CCCombiner_pregel combiner;
	if(use_combiner) worker.setCombiner(&combiner);
	worker.run(param);
}
