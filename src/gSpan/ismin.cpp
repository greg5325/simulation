/*
 $Id: ismin.cpp,v 1.5 2004/05/21 05:50:13 taku-ku Exp $;

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

namespace GSPAN {

/*
 * the main idea behind is_min()
 *
 * use the graph of DFS to build a minimum DFScode, and in the building process,
 * if we find the DFS code maybe not minimum, we return back early.
 *
 *
 */

bool gSpan::is_min() {
#ifdef SIMULATION
	if (DFS_CODE.size() == 1) {
		if (DFS_CODE[0].fromlabel > DFS_CODE[0].tolabel) {
			assert("false");
			return false;
		} else
			return (true);
	}
#else
	if (DFS_CODE.size() == 1)
	return true;
#endif

	DFS_CODE.toGraph(GRAPH_IS_MIN);
	DFS_CODE_IS_MIN.clear();
#ifdef DEBUG1
//	TRACE("report graph_is_min\n");
//	GRAPH_IS_MIN.write(std::cout);
//	TRACE("report graph_is_min end\n");
#endif
#ifdef DIRECTED
	Projected_map4 root4;
	//Projected_map3 root; // (fromVlabel, elabel, toVlabel) ==> Edge e
	EdgeList edges;

	for (unsigned int from = 0; from < GRAPH_IS_MIN.size(); ++from) {
		if (get_forward_root(GRAPH_IS_MIN, GRAPH_IS_MIN[from], edges)) {
			for (EdgeList::iterator it = edges.begin(); it != edges.end();
					++it) {
				if (GRAPH_IS_MIN[from].label <= GRAPH_IS_MIN[(*it)->to].label)
					root4[GRAPH_IS_MIN[from].label][(*it)->elabel][GRAPH_IS_MIN[(*it)->to].label]['l'].push(
							0, *it, 0);
				else
					root4[GRAPH_IS_MIN[(*it)->to].label][(*it)->elabel][GRAPH_IS_MIN[from].label]['r'].push(
							0, *it, 0);
			}
		}
	}
	Projected_iterator4 fromlabel = root4.begin();
	Projected_iterator3 elabel = fromlabel->second.begin();
	Projected_iterator2 tolabel = elabel->second.begin();
	Projected_iterator1 srcIterator = tolabel->second.begin();
	DFS_CODE_IS_MIN.push(0, 1, fromlabel->first, elabel->first, tolabel->first,
			srcIterator->first);
	// TODO: consider early termination here!
	return (project_is_min(srcIterator->second));
#else
	Projected_map3 root; // (fromVlabel, elabel, toVlabel) ==> Edge e
	EdgeList edges;

	for (unsigned int from = 0; from < GRAPH_IS_MIN.size(); ++from)
	if (get_forward_root(GRAPH_IS_MIN, GRAPH_IS_MIN[from], edges))
	for (EdgeList::iterator it = edges.begin(); it != edges.end(); ++it)
	root[GRAPH_IS_MIN[from].label][(*it)->elabel][GRAPH_IS_MIN[(*it)->to].label].push(
			0, *it, 0);

	Projected_iterator3 fromlabel = root.begin();
	Projected_iterator2 elabel = fromlabel->second.begin();
	Projected_iterator1 tolabel = elabel->second.begin();
	DFS_CODE_IS_MIN.push(0, 1, fromlabel->first, elabel->first, tolabel->first);
	return (project_is_min(tolabel->second));
#endif

}

// TODO: if we could tell which edge should be choose to build the minimum dfscode in this step, how could we solve this problem?
bool gSpan::project_is_min(Projected &projected) {
	const RMPath rmpath = DFS_CODE_IS_MIN.buildRMPath();
	int minlabel = DFS_CODE_IS_MIN[0].fromlabel;
	int maxtoc = DFS_CODE_IS_MIN[rmpath[0]].to;


	{//backward Edge Add.

		Projected_map2 root;

		bool flg = false; //if new backward edge can be added to DFS_CODE_IS_MIN

		int newto = 0;
		int newtolabel = 0;


		for (int i = rmpath.size() - 1; !flg && i >= 0; --i) {
			for (unsigned int n = 0; n < projected.size(); ++n) {
				PDFS *cur = &projected[n];
				History history(GRAPH_IS_MIN, cur);
#ifdef DIRECTED
				int v1;
				if (DFS_CODE_IS_MIN[rmpath[i]].src == 'l') {
					v1 = history[rmpath[i]]->from;
				} else if (DFS_CODE_IS_MIN[rmpath[i]].src == 'r') {
					v1 = history[rmpath[i]]->to;
				} else
					assert(false);
				int v2;
				if (DFS_CODE_IS_MIN[rmpath[0]].src == 'l') {
					v2 = history[rmpath[0]]->to;
				} else if (DFS_CODE_IS_MIN[rmpath[0]].src == 'r') {
					v2 = history[rmpath[0]]->from;
				} else
					assert(false);
				//TRACE("report (v1=%d,v2=%d)\n",v1,v2);

				Edge *e = get_backward(GRAPH_IS_MIN, history[rmpath[i]],
						history[rmpath[0]], history, v1, v2);
#else
				Edge *e = get_backward(GRAPH_IS_MIN, history[rmpath[i]],
						history[rmpath[0]], history);
#endif
				if (e) {
#ifdef DIRECTED
					//we have to assign the direction of the backward edge.
					char src = 'l';
					if (e->from == v2)
						src = 'l';
					else if (e->from == v1)
						src = 'r';
					else
						assert(false);
#endif
					root[e->elabel][src].push(0, e, cur);
					newto = DFS_CODE_IS_MIN[rmpath[i]].from;
					newtolabel = DFS_CODE_IS_MIN[rmpath[i]].fromlabel;
					flg = true;
				}
			}
		}
//		if (flg) {
//			TRACE("found a backward edge add\n");
//		} else {
//			TRACE("found no backward edge add\n");
//		}

		if (flg) {
			Projected_iterator2 elabel = root.begin();
			Projected_iterator1 src=elabel->second.begin();
//			assert(elabel->second.size()==1);
#ifdef DIRECTED

#ifdef DEBUG3
			DFS_CODE_IS_MIN.push(maxtoc, newto,-1, elabel->first,-1, src->first);
#else
			DFS_CODE_IS_MIN.push(maxtoc, newto,
					DFS_CODE_IS_MIN[rmpath[0]].tolabel, elabel->first,
					newtolabel, src->first);
#endif

#else
			DFS_CODE_IS_MIN.push(maxtoc, newto, -1, elabel->first, -1);
#endif
			if (DFS_CODE[DFS_CODE_IS_MIN.size() - 1]
					!= DFS_CODE_IS_MIN[DFS_CODE_IS_MIN.size() - 1]) {
#ifdef DEBUG1
				TRACE("not min after adding backward edge, Report\n");
				printf("dfs_min:");
				outputdfscode(DFS_CODE_IS_MIN);
				printf("dfs    :");
				outputdfscode(DFS_CODE);
				TRACE("not min after adding backward edge, Report End\n");
#endif
				return false;
			}
			return project_is_min(src->second);
		}
	}

	{ //forward edge Add.
		bool flg = false;
		int newfrom = 0;
		int newfromlabel = 0;
#ifdef DIRECTED
		Projected_map3 root;
#else
		Projected_map2 root;
#endif
		EdgeList edges;

		//	forward_pure...
#ifdef DIRECTED
		for (unsigned int n = 0; n < projected.size(); ++n) {
			PDFS *cur = &projected[n];
			History history(GRAPH_IS_MIN, cur);

			int v;
			if (DFS_CODE_IS_MIN[rmpath[0]].src == 'l') {
				v = history[rmpath[0]]->to;
			} else if (DFS_CODE_IS_MIN[rmpath[0]].src == 'r') {
				v = history[rmpath[0]]->from;
			} else
				assert(false);

			if (get_forward_pure(GRAPH_IS_MIN, v, minlabel, history, edges)) {
				flg = true;
				newfrom = maxtoc;
				newfromlabel = DFS_CODE_IS_MIN[rmpath[0]].tolabel;
				for (EdgeList::iterator it = edges.begin(); it != edges.end();
						++it)
					if (v == (*it)->from) {
						root[(*it)->elabel][GRAPH_IS_MIN[(*it)->to].label]['l'].push(
								0, *it, cur);
					} else if (v == (*it)->to) {
						root[(*it)->elabel][GRAPH_IS_MIN[(*it)->from].label]['r'].push(
								0, *it, cur);
					} else
						assert(false);
			}
		}
#else
		for (unsigned int n = 0; n < projected.size(); ++n) {
			PDFS *cur = &projected[n];
			History history(GRAPH_IS_MIN, cur);
			if (get_forward_pure(GRAPH_IS_MIN, history[rmpath[0]], minlabel,
							history, edges)) {
				flg = true;
				newfrom = maxtoc;
				for (EdgeList::iterator it = edges.begin(); it != edges.end();
						++it)
				root[(*it)->elabel][GRAPH_IS_MIN[(*it)->to].label].push(0,
						*it, cur);
			}
		}
#endif

		//	forward ....

#ifdef DIRECTED
		for (int i = 0; !flg && i < (int) rmpath.size(); ++i) {
			for (unsigned int n = 0; n < projected.size(); ++n) {
				PDFS *cur = &projected[n];
				History history(GRAPH_IS_MIN, cur);
				int v1, v2;
				if (DFS_CODE_IS_MIN[rmpath[i]].src == 'l') {
					v1 = history[rmpath[i]]->from;
					v2 = history[rmpath[i]]->to;
				} else if (DFS_CODE_IS_MIN[rmpath[i]].src == 'r') {
					v1 = history[rmpath[i]]->to;
					v2 = history[rmpath[i]]->from;
				} else
					assert(false);

				if (get_forward_rmpath(GRAPH_IS_MIN, v1, v2, history[rmpath[i]],
						minlabel, history, edges)) {
					flg = true;
					newfrom = DFS_CODE_IS_MIN[rmpath[i]].from;
					newfromlabel = DFS_CODE_IS_MIN[rmpath[i]].fromlabel;
					for (EdgeList::iterator it = edges.begin();
							it != edges.end(); ++it)
						if ((*it)->from == v1)
							root[(*it)->elabel][GRAPH_IS_MIN[(*it)->to].label]['l'].push(
									0, *it, cur);
						else if ((*it)->to == v1)
							root[(*it)->elabel][GRAPH_IS_MIN[(*it)->from].label]['r'].push(
									0, *it, cur);
						else
							assert(false);
				}
			}
		}
#else
		for (int i = 0; !flg && i < (int) rmpath.size(); ++i) {
			for (unsigned int n = 0; n < projected.size(); ++n) {
				PDFS *cur = &projected[n];
				History history(GRAPH_IS_MIN, cur);

				if (get_forward_rmpath(GRAPH_IS_MIN, history[rmpath[i]],
								minlabel, history, edges)) {
					flg = true;
					newfrom = DFS_CODE_IS_MIN[rmpath[i]].from;
					for (EdgeList::iterator it = edges.begin();
							it != edges.end(); ++it)
					root[(*it)->elabel][GRAPH_IS_MIN[(*it)->to].label].push(
							0, *it, cur);
				}
			}
		}
#endif

#ifdef DIRECTED
		if (flg) {

			Projected_iterator3 elabel = root.begin();
			Projected_iterator2 tolabel = elabel->second.begin();
			Projected_iterator1 src = tolabel->second.begin();

#ifdef DEBUG3
			DFS_CODE_IS_MIN.push(newfrom, maxtoc + 1, -1, elabel->first,
					tolabel->first, src->first);
#else
			DFS_CODE_IS_MIN.push(newfrom, maxtoc + 1, newfromlabel,
					elabel->first, tolabel->first, src->first);
#endif

			if (DFS_CODE[DFS_CODE_IS_MIN.size() - 1]
					!= DFS_CODE_IS_MIN[DFS_CODE_IS_MIN.size() - 1]) {
#ifdef DEBUG1
				TRACE("not min after adding forward edge, Report\n");
				printf("dfs_min:");
				outputdfscode(DFS_CODE_IS_MIN);
				printf("dfs    :");
				outputdfscode(DFS_CODE);
				TRACE("not min after adding forward edge, Report End\n");
#endif
				return false;
			}
			return project_is_min(src->second);
		}
#else
		if (flg) {
			Projected_iterator2 elabel = root.begin();
			Projected_iterator1 tolabel = elabel->second.begin();
			DFS_CODE_IS_MIN.push(newfrom, maxtoc + 1, -1, elabel->first,
					tolabel->first);
			if (DFS_CODE[DFS_CODE_IS_MIN.size() - 1]
					!= DFS_CODE_IS_MIN[DFS_CODE_IS_MIN.size() - 1])
			return false;
			return project_is_min(tolabel->second);
		}
#endif

	}

	return true;
}
}
