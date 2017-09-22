/*
 $Id: misc.cpp,v 1.6 2004/05/21 05:50:13 taku-ku Exp $;

 Copyright (C) 2004 Taku Kudo, All rights reserved.
 This is free software with ABSOLUTELY NO WARRANTY.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 02111-1307, USA
 */
#include "gspan.h"
#include <assert.h>

namespace GSPAN {
#ifdef SIMULATION
const RMPath DFSCode::buildRMPath() {
#else
const RMPath &DFSCode::buildRMPath() {
#endif
	rmpath.clear();

	int old_from = -1;

	for (int i = size() - 1; i >= 0; --i) {
		if ((*this)[i].from < (*this)[i].to && // forward
				(rmpath.empty() || old_from == (*this)[i].to)) {
			rmpath.push_back(i);
			old_from = (*this)[i].from;
		}
	}

	return rmpath;
}

void History::build(Graph &graph, PDFS *e) { //graph is transGraph, e is a project.
											 // first build history
	clear();
	edge.clear();
	edge.resize(graph.edge_size());
	vertex.clear();
	vertex.resize(graph.size());

	if (e) {
		push_back(e->edge);
		edge[e->edge->id] = vertex[e->edge->from] = vertex[e->edge->to] = 1;

		for (PDFS *p = e->prev; p; p = p->prev) {
			push_back(p->edge); // this line eats 8% of overall instructions(!)
			edge[p->edge->id] = vertex[p->edge->from] = vertex[p->edge->to] = 1;
		}
		std::reverse(begin(), end());
	}
}

#ifdef DIRECTED
bool get_forward_rmpath(Graph &graph, int v1, int v2, Edge *e, int minlabel,
		History& history, EdgeList &result) {
	result.clear();
	assert(v1 >= 0 && v1 < graph.size ());
	assert(v2 >= 0 && v2 < graph.size ());
	int tolabel = graph[v2].label;

	/*
	 * walk all edges leaving from v1 / e->from
	 */
	for (Vertex::edge_iterator it = graph[v1].edge.begin();
			it != graph[v1].edge.end(); ++it) {

		int tolabel2 = graph[it->to].label;

		if (v2 == it->to || minlabel > tolabel2 || history.hasVertex(it->to))
			continue;

		if (e->elabel < it->elabel
				|| (e->elabel == it->elabel && tolabel < tolabel2)
				|| (e->elabel == it->elabel && tolabel == tolabel2
						&& e->from == v1))
			result.push_back(&(*it));
	}
	/*
	 * walk all edges pointing at v1 / e->from
	 */
	for (Graph::vertex_iterator vit = graph.begin(); vit != graph.end();
			++vit) {
		for (Vertex::edge_iterator eit = vit->edge.begin();
				eit != vit->edge.end(); ++eit) {
			if(eit->to != v1)
				continue;
			if(history.hasVertex(eit->from))
				continue;
			int tolabel2=graph[eit->from].label;
			if(minlabel>tolabel2)
				continue;

			if(e->elabel<eit->elabel
					||(e->elabel==eit->elabel && tolabel <= tolabel2))
				result.push_back(&(*eit));
		}
	}

	return (!result.empty());
}
#else
bool get_forward_rmpath(Graph &graph, Edge *e, int minlabel, History& history,
		EdgeList &result) {
	result.clear();
	assert(e->to >= 0 && e->to < graph.size ());
	assert(e->from >= 0 && e->from < graph.size ());
	int tolabel = graph[e->to].label;

	for (Vertex::edge_iterator it = graph[e->from].edge.begin();
			it != graph[e->from].edge.end(); ++it) {
		int tolabel2 = graph[it->to].label;
		if (e->to == it->to || minlabel > tolabel2 || history.hasVertex(it->to))
		continue;

		if (e->elabel < it->elabel
				|| (e->elabel == it->elabel && tolabel <= tolabel2))
		result.push_back(&(*it));
	}

	return (!result.empty());
}
#endif

#ifdef DIRECTED
bool get_forward_pure(Graph &graph, int v, int minlabel, History& history,
		EdgeList &result) {
	result.clear();

	assert(v>=0&&v<graph.size());

	/* Walk all edges leaving from vertex v.
	 */
	for (Vertex::edge_iterator it = graph[v].edge.begin();
			it != graph[v].edge.end(); ++it) {
		/* -e-> [e->to] -it-> [it->to]
		 */
		assert(it->to >= 0 && it->to < graph.size ());
		if (minlabel > graph[it->to].label || history.hasVertex(it->to))
			continue;

		result.push_back(&(*it));
	}

	/*
	 * Walk all edges pointing at vertex v
	 */
	for (Graph::vertex_iterator vit = graph.begin(); vit != graph.end();
			++vit) {
		for (Vertex::edge_iterator eit = vit->edge.begin();
				eit != vit->edge.end(); ++eit) {
			if (eit->to == v) {
				if (minlabel > graph[eit->to].label
						|| history.hasVertex(eit->from))
					continue;
				result.push_back(&(*eit));
			}
		}
	}

	return (!result.empty());
}
#else
bool get_forward_pure(Graph &graph, Edge *e, int minlabel, History& history,
		EdgeList &result) {
	result.clear();

	assert(e->to >= 0 && e->to < graph.size ());

	/* Walk all edges leaving from vertex e->to.
	 */
	for (Vertex::edge_iterator it = graph[e->to].edge.begin();
			it != graph[e->to].edge.end(); ++it) {
		/* -e-> [e->to] -it-> [it->to]
		 */
		assert(it->to >= 0 && it->to < graph.size ());
		if (minlabel > graph[it->to].label || history.hasVertex(it->to))
		continue;

		result.push_back(&(*it));
	}

	return (!result.empty());
}
#endif

/*
 * get all possible forward edges rooted at v.
 *
 * called when initial the first edge code of dfs_code.
 * get all the possible edges that can be used as the first edge of dfs_code
 *
 */
bool get_forward_root(Graph &g, Vertex &v, EdgeList &result) {
	result.clear();
	for (Vertex::edge_iterator it = v.edge.begin(); it != v.edge.end(); ++it) {
		assert(it->to >= 0 && it->to < g.size ());
#ifndef DIRECTED
		if (v.label <= g[it->to].label)
#endif
		result.push_back(&(*it));
	}

	return (!result.empty());
}

#ifdef DIRECTED
Edge *get_backward(Graph &graph, Edge *e1, Edge *e2, History& history, int v1,
		int v2) {
	Edge *up = 0, *down = 0;
	/*
	 * there are four or five occasions where we have to consider.
	 *
	 */
	int from = v1;
	int to = v2;

	for (Vertex::edge_iterator it = graph[from].edge.begin();
			it != graph[from].edge.end(); ++it) {
		if (history.hasEdge(it->id))
			continue; //if edge exists in the dfscode, then omit it
		if (it->to != to)
			continue;
		//here we have found a edge, check if it is the minimum edge.
		if ((e1->elabel < it->elabel)
				|| (e1->elabel == it->elabel
						&& (e1->from == from ?
								graph[e1->to].label : graph[e1->from].label)
								< graph[it->to].label)
				|| (e1->elabel == it->elabel
						&& (e1->from == from ?
								graph[e1->to].label : graph[e1->from].label)
								== graph[it->to].label) && e1->from == from) {
			down = &(*it);
			break;
		}
	}

	for (Vertex::edge_iterator it = graph[to].edge.begin();
			it != graph[to].edge.end(); ++it) {
		if (history.hasEdge(it->id))
			continue;
		if (it->to != from)
			continue;
		//found it
		if ((e1->elabel < it->elabel)
				|| (e1->elabel == it->elabel
						&& ((e1->from == from ?
								graph[e1->to].label : graph[e1->from].label)
								<= graph[it->from].label))) {
			up = &(*it);
			break;
		}
	}
	//choose one of up and down to return, or return null
	if (up) {
		if (down) {
			//choose one to return
			if (down->elabel < up->elabel)
				return down;
			else
				return up;
		} else {
			return up;
		}
	} else {
		return down;
	}
}

#else
Edge *get_backward(Graph &graph, Edge* e1, Edge* e2, History& history) {

	if (e1 == e2) //in undirected graph, no multiple edge between nodes.
	return 0;
	assert(e1->from >= 0 && e1->from < graph.size ());
	assert(e1->to >= 0 && e1->to < graph.size ());
	assert(e2->to >= 0 && e2->to < graph.size ());

	for (Vertex::edge_iterator it = graph[e2->to].edge.begin();
			it != graph[e2->to].edge.end(); ++it) {
		if (history.hasEdge(it->id))
		continue;

		if ((it->to == e1->from)
				&& ((e1->elabel < it->elabel)
						|| (e1->elabel == it->elabel)
						&& (graph[e1->to].label <= graph[e2->to].label))) {
			return &(*it);
		}
	}

	return 0;
}
#endif
}

