/*
 $Id: graph.cpp,v 1.4 2004/05/21 05:50:13 taku-ku Exp $;

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
#include <string>
#include <iterator>
#include <strstream>
#include <set>
#include <stdio.h>

#include <assert.h>

namespace GSPAN {

template<class T, class Iterator>
void tokenize(const char *str, Iterator iterator) {
	std::istrstream is(str, std::strlen(str));
	std::copy(std::istream_iterator<T>(is), std::istream_iterator<T>(),
			iterator);
}
/**
 * assign id for edges and assign edge_size:
 *
 * for undirected graph, (v,u,l) and (u,v,l) have same id,
 * while for directed graph, their ids are different
 *
 * assign edge_size_ with the largest id value.
 *
 */
void Graph::buildEdge() {
	char buf[512];
	std::map<std::string, unsigned int> tmp;

	unsigned int id = 0;
	for (int from = 0; from < (int) size(); ++from) {
		for (Vertex::edge_iterator it = (*this)[from].edge.begin();
				it != (*this)[from].edge.end(); ++it) {
			if (directed || from <= it->to)
				sprintf(buf, "%d %d %d", from, it->to, it->elabel);
//				sprintf(buf, "%d %d %d", from, it->to, 1);
			else
				sprintf(buf, "%d %d %d", it->to, from, it->elabel);
//				sprintf(buf, "%d %d %d", it->to, from, 1);

			// Assign unique id's for the edges.
			if (tmp.find(buf) == tmp.end()) {
				it->id = id;
				tmp[buf] = id;
				++id;
			} else {
				it->id = tmp[buf];
			}
		}
	}

	edge_size_ = id;
}


std::istream &Graph::read(std::istream &is) {
	std::vector<std::string> result;
	char line[1024];

	clear();

	while (true) {

		unsigned int pos = is.tellg();
		if (!is.getline(line, 1024))
			break;
		result.clear();
		tokenize<std::string>(line, std::back_inserter(result));

		if (result.empty()) {
			// do nothing
		} else if (result[0] == "t") {
			if (!empty()) { // use as delimiter
//				TRACE("reach a new t, exit\n");
				is.seekg(pos, std::ios_base::beg);
				break;
			} else {
//				TRACE("begin build a new t\n");
				/*
				 * y = atoi (result[3].c_str());
				 */
			}
		} else if (result[0] == "v" && result.size() >= 3) {//v line
//			TRACE("read v line\n");
			unsigned int id = atoi(result[1].c_str());
			this->resize(id + 1);
			(*this)[id].label = atoi(result[2].c_str());
		} else if (result[0] == "e" && result.size() >= 4) {//e line
//			TRACE("read e line\n");
			int from = atoi(result[1].c_str());
			int to = atoi(result[2].c_str());
			int elabel = atoi(result[3].c_str());

			if ((int) size() <= from || (int) size() <= to) {
				std::cerr << "Format Error:  define vertex lists before edges"
						<< std::endl;
				exit(-1);
			}

			(*this)[from].push(from, to, elabel);
			if (directed == false)
				(*this)[to].push(to, from, elabel);
		}
	}

	buildEdge();

	return is;
}

std::ostream &Graph::write(std::ostream &os) {
	char buf[512];
	std::set<std::string> tmp;

	for (int from = 0; from < (int) size(); ++from) {
#ifdef little
		os << "v " << from << " " << (char)(*this)[from].label << std::endl;
#else

		os << "v " << from << " " << (*this)[from].label << std::endl;
#endif

		for (Vertex::edge_iterator it = (*this)[from].edge.begin();
				it != (*this)[from].edge.end(); ++it) {
			if (directed || from <= it->to) {
				sprintf(buf, "%d %d %d", from, it->to, it->elabel);
//				sprintf(buf, "%d %d %d", from, it->to, 1);
			} else {
				sprintf(buf, "%d %d %d", it->to, from, it->elabel);
//				sprintf(buf, "%d %d %d", it->to, from, 1);
			}
			tmp.insert(buf);
		}
	}

	for (std::set<std::string>::iterator it = tmp.begin(); it != tmp.end();
			++it) {
		os << "e " << *it << std::endl;
	}

	return os;
}

void Graph::check(void) {
	/* Check all indices
	 */
	for (int from = 0; from < (int) size(); ++from) {
		//mexPrintf ("check vertex %d, label %d\n", from, (*this)[from].label);

		for (Vertex::edge_iterator it = (*this)[from].edge.begin();
				it != (*this)[from].edge.end(); ++it) {
			//mexPrintf ("   check edge from %d to %d, label %d\n", it->from, it->to, it->elabel);
			assert(it->from >= 0 && it->from < size ());
			assert(it->to >= 0 && it->to < size ());
		}
	}
}

}

