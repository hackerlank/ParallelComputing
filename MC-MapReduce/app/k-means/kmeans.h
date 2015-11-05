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

#ifndef _KMEANS_H
#define _KMEANS_H

#include <stddef.h>
#include <ulib/math_rand_prot.h>

namespace kmeans {

extern int g_dim;

struct point {
	mutable int cid;
	float *prj;  // projections

	point() : cid(-1), prj(NULL) { }
	point(int id, float *buf) : cid(id), prj(buf) { }

	// generate projections within the range [0, grid)
	void
	generate(float grid)
	{
		for (int i = 0; i < g_dim; ++i)
			prj[i] = RAND_NR_DOUBLE(RAND_NR_NEXT(_u, _v, _w)) * grid;
	}

	// RNG context
	static volatile long _u, _v, _w;
};

struct cluster {
	float *prj;
	size_t weight;

	// implicit initialization assumes zero point
	cluster() : prj(NULL), weight(0) { }

	cluster(float *buf, size_t w = 1) : prj(buf), weight(w) { }

	void zero()
	{
		for (int i = 0; i < g_dim; ++i)
			prj[i] = 0;
		weight = 0;
	}

	void
	dump() const
	{
		for (int i = 0; i < g_dim; ++i)
			printf("%f%c", prj[i], i == g_dim - 1? '\n': '\t');
	}

	void from(const point &pt)
	{
		for (int i = 0; i < g_dim; ++i)
			prj[i] = pt.prj[i];
	}

	void
	normalize()
	{
		if (weight) {
			for (int i = 0; i < g_dim; ++i)
				prj[i] /= weight;
			weight = 1;
		}
	}

	float
	sq_dist(const point &pt) const
	{
		float sum = 0;
		float diff;
		for (int i = 0; i < g_dim; ++i) {
			diff = prj[i] - pt.prj[i];
			sum += diff * diff;
		}
		return sum;
	}

	void
	add(const cluster &other)
	{
		weight += other.weight;
		for (int i = 0; i < g_dim; ++i)
			prj[i] += other.prj[i];
	}
};

}  // namespace kmeans

#endif
