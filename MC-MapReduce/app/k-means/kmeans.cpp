/* The MIT License

   Copyright (C) 2013 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#include <unistd.h>
#include <getopt.h>
#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <algorithm>
#include <ulib/mc_runtime.h>
#include <ulib/util_log.h>
#include <ulib/util_timer.h>
#include "kmeans.h"

using namespace std;
using namespace ulib;
using namespace mapcombine;
using namespace kmeans;

static volatile bool g_stablized = false;
static bool g_verbose = false;

const char *g_usage =
	"Parallel Kmeans 0.1 by Zilong Tan (eric.zltan@gmail.com)\n"
	"usage: %s [options] [point_file]\n"
	"options:\n"
	"  -c <cluster>  - number of clusters, the default is 1\n"
	"  -d <dim>      - dimension, the default is 3\n"
	"  -g <grid>     - grid size for generating random points, the default is 100.0\n"
	"  -r <num>      - use random points\n"
	"  -s <slot>     - MHT slot number, default is NCPU^2\n"
	"  -t <task>     - number of concurrent tasks, default is NCPU\n"
	"  -f            - use fixed initial means\n"
	"  -p            - print point set\n"
	"  -v            - be verbose\n"
	"  -h            - show this message\n";

// use array_chunk instead of vector here since we may update the
// input point data as we go.
typedef array_chunk<point> point_chunk;
typedef vector<cluster> cluster_chunk;

namespace kmeans {

// three dimensional by default
int g_dim = 3;

}

// the resulting means
cluster_chunk g_means;

volatile long point::_u, point::_v, point::_w;

class kmeans_splitter : public splitter<point_chunk> {
public:
	kmeans_splitter(point *from, point *end) : _from(from), _end(end) { }

	int
	split(size_t nchunk)
	{
		_segments.clear();
		if (nchunk == 0)
			return 0;  // zero chunk is okay
		size_t chunk_size = std::max((_end - _from + nchunk) / nchunk, 1ul);
		//ULIB_DEBUG("%zu point(s) in all, %zu point(s) per chunk", _end - _from, chunk_size);
		for (point *p = _from; p < _end;) {
			point *q = p + chunk_size;
			if (q >= _end) {
				_segments.push_back(std::pair<point *, point *>(p, _end));
				//ULIB_DEBUG("added segment [%zu, %zu)", p - _from, _end - _from);
				return 0;
			}
			_segments.push_back(std::pair<point *, point *>(p, q));
			//ULIB_DEBUG("added segment [%zu, %zu)", p - _from, q - _from);
			p = q;
		}
		//ULIB_DEBUG("total %zu segment(s)", _segments.size());
		return 0;
	}

	size_t
	size() const
	{ return _segments.size(); }

	point_chunk
	chunk(size_t n) const
	{ return point_chunk(_segments[n].first, _segments[n].second); }

private:
	point *_from;
	point *_end;
	std::vector< std::pair<point *, point *> > _segments;
};

template<typename _Storage>
struct kmeans_mapper : public mc_mapper<_Storage, point, int, cluster> {
	kmeans_mapper(_Storage &stor)
		: mc_mapper<_Storage, point, int, cluster>(stor) { }

	void
	operator() (const point &pt)
	{
		if (g_means.size()) {
			int min_idx = 0;
			float min_dist = g_means[0].sq_dist(pt);
			for (size_t i = 1; i < g_means.size(); ++i) {
				float dist = g_means[i].sq_dist(pt);
				if (dist < min_dist) {
					min_dist = dist;
					min_idx = i;
				}
			}
			if (min_idx != pt.cid) {
				g_stablized = false;
				pt.cid = min_idx;
			}
			this->emit(min_idx, cluster(pt.prj));
		}
	}
};

struct kmeans_reducer : public combiner<cluster> {
	void
	operator()(cluster &sum, const cluster &value) const
	{
		if (sum.prj == NULL)
			sum = value;
		else {
			for (int i = 0; i < g_dim; ++i)
				sum.prj[i] += value.prj[i];
			sum.weight += value.weight;
		}
	}
};

typedef multi_hash_runtime<
	kmeans_splitter, int, cluster, kmeans_mapper,
	simple_partition<int>, kmeans_reducer > kmeans_runtime;

int read_point(const char *file, float **pbuf, size_t *pnum)
{
	FILE *fp = fopen(file, "r");
	if (fp == NULL) {
		ULIB_FATAL("cannot open point file: %s", file);
		return -1;
	}
	float x;
	float *buf = NULL;
	size_t num = 0;
	size_t pos = 0;
	while (fscanf(fp, "%f", &x) == 1) {
		if (buf == NULL) {
			buf = (float *)malloc(sizeof(float));
			if (buf == NULL) {
				ULIB_FATAL("cannot allocate point vector");
				fclose(fp);
				return -1;
			}
			num = 1;
		}
		if (pos == num) {
			float *larger = (float *)realloc(buf, sizeof(float) * (num << 1));
			if (larger == NULL) {
				ULIB_FATAL("realloc point vector failed");
				free(buf);
				fclose(fp);
				return -1;
			}
			buf = larger;
			num <<= 1;
		}
		buf[pos++] = x;
	}
	fclose(fp);
	*pbuf = buf;
	*pnum = pos;
	return 0;
}

void rand_seed()
{
	timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts)) {
		ULIB_DEBUG("use clock() instead of CLOCK_REALTIME");
		ts.tv_nsec = clock();
	}
	RAND_INT3_MIX64(ts.tv_nsec);
	RAND_NR_INIT(point::_u, point::_v, point::_w, ts.tv_nsec);
	ULIB_DEBUG("use random seeds: u=%016lx, v=%016lx, w=%016lx", point::_u, point::_v, point::_w);
}

int generate_means(int ncluster, float grid)
{
	float *buf = (float *)malloc(sizeof(float) * g_dim * ncluster);
	if (buf == NULL) {
		ULIB_FATAL("cannot allocate initial means");
		return -1;
	}
	for (int i = 0; i < ncluster; ++i) {
		cluster cl(&buf[i * g_dim]);
		point pt(-1, &buf[i * g_dim]);
		pt.generate(grid);
		g_means.push_back(cl);
		if (g_verbose)
			cl.dump();
	}
	return 0;
}

int init_fixed_means(int ncluster, point *pts)
{
	float *buf = (float *)malloc(sizeof(float) * g_dim * ncluster);
	if (buf == NULL) {
		ULIB_FATAL("cannot allocate initial means");
		return -1;
	}
	for (int i = 0; i < ncluster; ++i) {
		cluster cl(&buf[i * g_dim]);
		cl.from(pts[i]);
		g_means.push_back(cl);
		if (g_verbose)
			cl.dump();
	}
	return 0;
}

void destroy_means()
{
	// buffer head starts at g_means[0].prj
	free(g_means[0].prj);
}

void print_points(point *pts, size_t num)
{
	for (size_t i = 0; i < num; ++i) {
		putchar('(');
		for (int j = 0; j < g_dim; ++j)
			printf("%f%s", pts[i].prj[j], j == g_dim - 1? ") ": ",");
	}
	putchar('\n');
}

int main(int argc, char *argv[])
{
	int oc;
	int ncluster   = 1;
        // ncpu^2 slots for the multiple hash map
	float grid     = 1000.0;
	size_t rand_pt = 0;
	size_t nslot   = sysconf(_SC_NPROCESSORS_ONLN);
	nslot *= nslot;
	int ntask      = sysconf(_SC_NPROCESSORS_ONLN);
	bool fixed     = false;
	bool ppt       = false;

	while ((oc = getopt(argc, argv, "c:d:g:r:s:t:fpvh")) != EOF) {
		switch (oc) {
		case 'c': ncluster = atoi(optarg); break;
		case 'd': g_dim = atoi(optarg); break;
		case 'g': grid = atof(optarg); break;
		case 'r': rand_pt = strtoul(optarg, 0, 10); break;
		case 's': nslot = strtoul(optarg, 0, 10); break;
		case 't': ntask = atoi(optarg); break;
		case 'f': fixed = true; break;
		case 'p': ppt = true;
		case 'v': g_verbose = true; break;
		case 'h': printf(g_usage, argv[0]); exit(EXIT_SUCCESS);
		default : fprintf(stderr, g_usage, argv[0]); exit(EXIT_FAILURE);
		}
	}
	if (optind != argc - !rand_pt) {
		fprintf(stderr, g_usage, argv[0]);
		exit(EXIT_FAILURE);
	}
	if (ncluster <= 0 || g_dim <= 0 || grid <= 0 || ntask <= 0) {
		ULIB_FATAL("do not accept negative values or zeroes");
		exit(EXIT_FAILURE);
	}

	rand_seed();

	float *buf;
	size_t npt;
	if (rand_pt) {
		npt = rand_pt;
		buf = (float *)malloc(sizeof(float) * g_dim * rand_pt);
		if (buf == NULL) {
			ULIB_FATAL("cannot allocate points");
			exit(EXIT_FAILURE);
		}
	} else {
		size_t num;
		ULIB_DEBUG("read points from file ...");
		if (read_point(argv[optind], &buf, &num)) {
			ULIB_FATAL("read point failed");
			exit(EXIT_FAILURE);
		}
		if (num % g_dim) {
			ULIB_FATAL("point number(%zu) isn't an integral multiple of dimension(%d)",
				num, g_dim);
			exit(EXIT_FAILURE);
		}
		npt = num / g_dim;
	}
	if (npt < (size_t)ncluster) {
		ULIB_FATAL("insufficient points to fit into %d cluster(s)", ncluster);
		exit(EXIT_FAILURE);
	}
	point *pts = new point [npt];
	for (size_t i = 0; i < npt; ++i)
		pts[i].prj = &buf[g_dim * i];
	if (rand_pt) {
		ULIB_DEBUG("generate %zu point(s), grid=%f", rand_pt, grid);
		for (size_t i = 0; i < npt; ++i)
			pts[i].generate(grid);
	}

	if (fixed) {
		ULIB_DEBUG("use fixed initial means ...");
		if (init_fixed_means(ncluster, pts)) {
			ULIB_FATAL("initialize fixed means failed");
			exit(EXIT_FAILURE);
		}
	} else {
		ULIB_DEBUG("generate initial means ...");
		if (generate_means(ncluster, grid)) {
			ULIB_FATAL("generate initial means failed");
			exit(EXIT_FAILURE);
		}
	}

	if (ppt) {
		printf("point set:");
		print_points(pts, npt);
	}

	ULIB_DEBUG("setup MapCombine environment ...");
	kmeans_splitter my_splitter(pts, pts + npt);
	kmeans_runtime::storage_type my_storage(nslot);
	kmeans_runtime my_runtime(my_splitter, my_storage);

	// initialize the storage with the universe, providing buffers
	// for the results
	float *res = (float *)malloc(sizeof(float) * g_dim * ncluster);
	for (int i = 0; i < ncluster; ++i) {
		cluster cl(&res[i * g_dim]);
		cl.zero();
		my_storage.insert(i, cl);
	}

	ULIB_NOTICE("begin KMeans iteration ...");
	ulib_timer_t timer;
	timer_start(&timer);
	while (!g_stablized) {
		g_stablized = true;
		my_runtime.run(ntask);
		assert(my_storage.size() == (size_t)ncluster);
		for (int i = 0; i < ncluster; ++i)
			g_means[i].zero();
		for (typename kmeans_runtime::storage_type::iterator it = my_storage.begin();
		     it != my_storage.end(); ++it) {
			g_means[it.key().key()].add(it.value());
			it.value().zero();  // clear last results
		}
		for (int i = 0; i < ncluster; ++i)
			g_means[i].normalize();
		if (g_verbose) {
			printf("Current iteration means:\n");
			for (size_t i = 0; i < g_means.size(); ++i)
				g_means[i].dump();
		}
	}
	float elapsed = timer_stop(&timer);
	ULIB_NOTICE("task done with %d task(s), %zu slot(s); %f sec elapsed",
		    ntask, nslot, elapsed);

	ULIB_NOTICE("process done, the means are as follows");
	for (size_t i = 0; i < g_means.size(); ++i) {
		if (g_means[i].weight == 0) {
			ULIB_FATAL("an empty cluster was detected, this might be a result of "
				   "using random means; try it again with fixed means");
			continue;
		}
		assert(g_means[i].weight == 1);
		g_means[i].dump();
	}

	my_storage.clear();
	delete [] pts;
	free(res);
	free(buf);
	destroy_means();
	return 0;
}
