/*
 $Id: dfs.cpp,v 1.3 2004/05/21 05:50:13 taku-ku Exp $;

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
#include <cstring>
#include <assert.h>
#include <string>
#include <iterator>
#include <set>
#include <map>

namespace GSPAN {

/* Build a DFS code from a given graph.
 *
 * no call to this function!
 *
 * FIXME: wrong coding process.
 *
 * warning!
 *
 */
/*
 void DFSCode::fromGraph(Graph &g) {
 clear();

 EdgeList edges;
 for (unsigned int from = 0; from < g.size(); ++from) {
 if (get_forward_root(g, g[from], edges) == false)
 continue;

 for (EdgeList::iterator it = edges.begin(); it != edges.end(); ++it)
 push(from, (*it)->to, g[(*it)->from].label, (*it)->elabel,
 g[(*it)->to].label);
 }
 }*/

bool DFSCode::toGraph(Graph &g) {
	g.clear();

	for (DFSCode::iterator it = begin(); it != end(); ++it) {
		g.resize(std::max(it->from, it->to) + 1);

#ifdef DIRECTED
//		TRACE("a new code\n");
//		printf("(%d,%d,%c,%d,%c,%c)\n",it->from,it->to,it->fromlabel,it->elabel,it->tolabel,it->src);
//		TRACE("a new code end\n");
		if (it->src == 'l') {
			if (it->fromlabel != -1)
				g[it->from].label = it->fromlabel;
			if (it->tolabel != -1)
				g[it->to].label = it->tolabel;
			g[it->from].push(it->from, it->to, it->elabel);
		} else if (it->src == 'r') {
			if (it->fromlabel != -1)
				g[it->from].label = it->fromlabel;
			if (it->tolabel != -1)
				g[it->to].label = it->tolabel;
			g[it->to].push(it->to, it->from, it->elabel);
		} else {
			assert(false);
		}
#else
		if (it->fromlabel != -1)
		g[it->from].label = it->fromlabel;
		if (it->tolabel != -1)
		g[it->to].label = it->tolabel;

		g[it->from].push(it->from, it->to, it->elabel);
		if (g.directed == false)
		g[it->to].push(it->to, it->from, it->elabel);
#endif
	}

	g.buildEdge();

	return (true);
}

unsigned int DFSCode::nodeCount(void) {
	unsigned int nodecount = 0;

	for (DFSCode::iterator it = begin(); it != end(); ++it)
		nodecount = std::max(nodecount,
				(unsigned int) (std::max(it->from, it->to) + 1));

	return (nodecount);
}

unsigned int DFSCode::labelCount(void) {
	 std::map<unsigned int ,unsigned int >::iterator l_it;
	unsigned int labelnum = 0;
	unsigned int nodecount = 0;
//	for (DFSCode::iterator it = begin(); it != end(); ++it)
//		nodecount = std::max(nodecount,(unsigned int) (std::max(it->from, it->to) + 1));
	nodecount=DFSCode::nodeCount();
	std::map<unsigned int, int> nodeID;
	for (unsigned int i = 0; i < nodecount; i++) {
//		nodeID.insert(i, 0);
		nodeID[i]=0;
	}
//	printf("nodecount=%o\n",nodecount);
	std::map<unsigned int, unsigned int> labelcount;
	for (DFSCode::iterator it = begin(); it != end(); ++it) {
		if (nodeID[it->from] == 0) {
			nodeID[it->from]++;
			l_it=labelcount.find(it->fromlabel);
			if (l_it==labelcount.end()) {
				labelcount[it->fromlabel]=1;
//				labelcount.insert(it->fromlabel, 1);
//				labelcount.insert(std::pair<unsigned int,unsigned int>(it->fromlabel,1));
			} else {
				labelcount[it->fromlabel]++;
			}
		}
		if (nodeID[it->to] == 0) {
			nodeID[it->to]++;
			l_it=labelcount.find(it->tolabel);
			if (l_it==labelcount.end()) {
				labelcount[it->tolabel]=1;
//				labelcount.insert(it->tolabel, 1);
//				labelcount.insert(std::pair<unsigned int,unsigned int>(it->tolabel,1));
			} else {
				labelcount[it->tolabel]++;
			}
		}
	}
//	printf("size of labelcount=%o\n",labelcount.size());
	std::map<unsigned int,unsigned int>::iterator iter;
	iter = labelcount.begin();
//	printf("size of labelnum=%o",labelnum);
	while(iter != labelcount.end()) {
		labelnum=std::max(labelnum,(unsigned int)iter->second);
		iter++;
	}
//	printf("size of labelnum2=%o",labelnum);
	return (labelnum);
}

std::ostream &DFSCode::write(std::ostream &os) {
	if (size() == 0)
		return os;

	os << "(" << (*this)[0].fromlabel << ") " << (*this)[0].elabel << " (0f"
			<< (*this)[0].tolabel << ")";

	for (unsigned int i = 1; i < size(); ++i) {
		if ((*this)[i].from < (*this)[i].to) {
			os << " " << (*this)[i].elabel << " (" << (*this)[i].from << "f"
					<< (*this)[i].tolabel << ")";
		} else {
			os << " " << (*this)[i].elabel << " (b" << (*this)[i].to << ")";
		}
	}

	return os;
}
}

