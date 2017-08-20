#include "basic/pregel-dev.h"
#include <string>
#include <vector>
using namespace std;
typedef unsigned int UINT_32;
typedef unsigned char UINT_8;

//Query graph
struct Query_Graph {
	vector<char> label;
	vector<vector<VertexID> > Q_id;
};

static Query_Graph graph;
static VertexID sum_num = 0;

class init_Query {
public:
	init_Query() {
		graph.label.push_back('a');
		graph.label.push_back('b');
		graph.label.push_back('c');
		graph.label.push_back('d');
		vector<VertexID> temp;
		temp.push_back(1);
		temp.push_back(2);
		graph.Q_id.push_back(temp);
		temp.clear();
		temp.push_back(2);
		graph.Q_id.push_back(temp);
		temp.clear();
		temp.push_back(3);
		graph.Q_id.push_back(temp);
		temp.clear();
		temp.push_back(1);
		graph.Q_id.push_back(temp);
		temp.clear();

		for (int i = 0; i < graph.label.size(); i++) {
			printf("label=%c Q_id=%d edge=", graph.label[i], i);
			for (int j = 0; j < graph.Q_id[i].size(); j++) {
				printf("%d ", graph.Q_id[i][j]);
			}
			printf("\n");
		}
	}
};

UINT_8 GetBit(UINT_32 number, UINT_32 index) {
	if (index < 0 || index > 31)
		return 0xff; //如果传入参数有问题，则返回0xff，表示异常
	return (number >> index) & 1UL;
}

struct CCValue_pregel {
	char label;
	int D_id;
	int outedge_num;
	vector<VertexID> edges; //邻接顶点
	vector<VertexID> simulate; //子结点simulate数[*,*,*....]
	vector<VertexID> can_simulate; //自身simulate结点{*,*...}
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v) {
	m << v.label;
	m << v.D_id;
	m << v.outedge_num;
	m << v.edges;
	m << v.simulate;
	m << v.can_simulate;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v) {
	m >> v.label;
	m >> v.D_id;
	m >> v.outedge_num;
	m >> v.edges;
	m >> v.simulate;
	m >> v.can_simulate;
	return m;
}

class CCVertex_pregel: public Vertex<VertexID, CCValue_pregel, int> {
public:
	void broadcast(int msg) {
		vector<VertexID> & nbs = value().edges;
		for (int i = 0; i < nbs.size(); i++) {
			send_message(nbs[i], msg);
		}
	}

	virtual void compute(MessageContainer & messages) {
		if (step_num() == 1) //超步
				{
			int trans_messages = 0x00;
			for (int j = 0; j < graph.label.size(); j++) {
				value().simulate.push_back(value().outedge_num);
			}

			//print sum_num
			vector<VertexID> broad;
			bool can_sim = false;
			for (int i = 0; i < graph.Q_id.size(); i++) {
				can_sim = false;
				vector<VertexID> &nps = value().can_simulate;
				for (int j = 0; j < nps.size(); j++) {
					if (i == nps[j])
						can_sim = true;
				}
				if (!can_sim) {
					int tmp = 1;
					for (int p = 0; p < i; p++) {
						tmp = tmp * 2;
					}
					trans_messages += tmp;
				}
			}
			printf("trans_messages2=%d \n", trans_messages);
			for (int i = 31; i >= 0; i--) {
				printf("%d", GetBit(trans_messages, i));
				if (i % 4 == 0)
					putchar(' ');
			}
			broadcast(trans_messages);
			vote_to_halt();
		} else {
			int trans_messages = 0x00;
			printf("message_size=%d ", messages.size());

			vector<VertexID>::iterator iter;
			for (iter = messages.begin(); iter != messages.end(); iter++) {
				int message = *iter;
				for (int i = value().simulate.size(); i >= 0; i--) {
					int minus = GetBit(message, i);
					if (minus == 1)
						value().simulate[i]--;
				}
			}

			bool can_sim = true;
			for (int j = 0; j < value().can_simulate.size(); j++) {
				can_sim = true;
				int judge = value().can_simulate[j];
				if (judge != 404) {
					for (int p = 0; p < graph.Q_id[judge].size(); p++) {
						int j = graph.Q_id[judge][p];
						if (value().simulate[j] <= 0)
							can_sim = false;
					}
				}
				if (!can_sim) {
					int tmp = 1;
					for (int p = 0; p < judge; p++) {
						tmp = tmp * 2;
					}
					trans_messages += tmp;
					value().can_simulate[j] = 404;
				}
			}

			vector<VertexID>::iterator iter4;
			printf("id=:%d\t simulate:", value().D_id);
			for (iter4 = value().simulate.begin();
					iter4 != value().simulate.end(); iter4++) {
				VertexID tmp4 = *iter4;
				printf("%d ", tmp4);
			}
			printf("after can_simulate:  ");
			vector<VertexID>::iterator iter2;
			for (iter2 = value().can_simulate.begin();
					iter2 != value().can_simulate.end(); iter2++) {
				VertexID tmp2 = *iter2;
				printf("%d ", tmp2);
			}
			printf("\n");
			if (trans_messages != 0) {
				broadcast(trans_messages);
			}
			vote_to_halt();
		}
	}
};

class CCWorker_pregel: public Worker<CCVertex_pregel> {
	char buf[100];

public:
	virtual CCVertex_pregel* toVertex(char* line){
		char * pch;
		pch = strtok(line, "\t");
		CCVertex_pregel* v = new CCVertex_pregel;
		v->id = atoi(pch); //v->id保存制表符（/t）之前的数据
		v->value().D_id = v->id;
		pch = strtok(NULL, " ");
		char* label = pch; //label保存顶点种类
		v->value().label = label[0];
		vector<char>::iterator iter1;
		int p = 0;
		for (iter1 = graph.label.begin(); iter1 != graph.label.end(); iter1++) {
			char tmp1 = *iter1;
			if (tmp1 == label[0]) {
				v->value().can_simulate.push_back(p);
			}
			p++;
		}
//********************************************************************************
		pch = strtok(NULL, " ");
		int num = atoi(pch); //num保存邻接点个数
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
			v->value().edges.push_back(atoi(pch)); //v->value保存每个邻接顶点的ID
		}
		pch = strtok(NULL, " "); //在邻接点后面插入顶点出度
		v->value().outedge_num = atoi(pch);

		sum_num++;
		return v;
	}

	virtual void toline(CCVertex_pregel* v, BufferedWriter & writer) {
		sprintf(buf, "vid:%d\t can_similate:%d \n", v->value().D_id,
				v->value().can_simulate[0]);
		writer.write(buf);
	}
};

class CCCombiner_pregel: public Combiner<vector<VertexID> > {
public:
	virtual void combine(vector<VertexID> & old,
			const vector<VertexID> & new_msg) {
		vector<VertexID>::const_iterator it_new;
		for (it_new = new_msg.begin(); it_new != new_msg.end(); ++it_new) {
			VertexID co = *it_new;
			old.push_back(co);
		}
	}
};

void pregel_similation(string in_path, string out_path, bool use_combiner) {
	init_Query();
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	CCWorker_pregel worker;
	CCCombiner_pregel combiner;
	if (use_combiner)
		worker.setCombiner(&combiner);
	worker.run(param);
}
