/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

   Permission is hereby granted, free of charge, to any person
   obtaining a copy of this software and associated documentation
   files (the "Software"), to deal in the Software without
   restriction, including without limitation the rights to use, copy,
   modify, merge, publish, distribute, sublicense, and/or sell copies
   of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <vector>
#include <utility>
#include <ulib/util_timer.h>
#include <ulib/util_algo.h>
#include <ulib/math_rng_zipf.h>
#include <ulib/hash_open.h>
#include <ulib/hash_multi_r.h>
#include <ulib/mc_runtime.h>

static const char *usage =
	"The MapCombine Framework Testing\n"
	"Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)\n"
	"usage: %s [options]\n"
	"options:\n"
	"  -t<ntask>   - number of tasks, default is ncpu\n"
	"  -k<nslot>   - number of slots, default is ncpu^2\n"
	"  -n<size>    - dataset size in elements, default is 10000000\n"
	"  -r<range>   - the range of value, default is 0x10000\n"
	"  -s<exp>     - Zipf dataset parameter, default is 0\n"
	"  -w<file>    - output data set to file\n"
	"  -z	       - correctness check\n"
	"  -h	       - print this message\n";

using namespace std;
using namespace ulib;
using namespace ulib::mapcombine;

template<typename _Storage>
struct wc_mapper : public mc_mapper<_Storage, int, size_t, size_t> {
	wc_mapper(_Storage &stor)
		: mc_mapper<_Storage, int, size_t, size_t>(stor) { }

	void
	operator()(const int &rec)
	{ this->emit(rec, 1); }
};

class wc_chunk {
public:
	typedef int value_type;

	wc_chunk(int *start, int *end)
		: _start(start), _end(end) { }

	struct iterator {
		iterator(int *p = 0)
			: _pos(p)
		{ }

		int &
		operator *()
		{ return *_pos; }

		iterator
		operator +(size_t dist)
		{ return iterator(_pos + dist); }

		iterator &
		operator++()
		{
			++_pos;
			return *this;
		}

		iterator
		operator++(int)
		{
			iterator old = *this;
			++*this;
			return old;
		}

		bool
		operator!=(const iterator &other) const
		{ return _pos != other._pos; }

		int *_pos;
	};

	struct const_iterator {
		const_iterator(const int *p = 0)
			: _pos(p)
		{ }

		const_iterator(const iterator &other)
			: _pos(other._pos)
		{ }

		const int &
		operator *()
		{ return *_pos; }

		const_iterator
		operator +(size_t dist)
		{ return const_iterator(_pos + dist); }

		const_iterator &
		operator++()
		{
			++_pos;
			return *this;
		}

		const_iterator
		operator++(int)
		{
			const_iterator old = *this;
			++*this;
			return old;
		}

		bool
		operator!=(const const_iterator &other) const
		{ return _pos != other._pos; }

		const int *_pos;
	};

	iterator
	begin()
	{ return iterator(_start); }

	const_iterator
	begin() const
	{ return const_iterator(_start); }

	iterator
	end()
	{ return iterator(_end); }

	const_iterator
	end() const
	{ return const_iterator(_end); }

	size_t
	size() const
	{ return _end - _start; }

private:
	int * _start;
	int * _end;
};

// We can simply use the following alternatives:
// typedef array_chunk<int> wc_chunk;
// typedef vector<int> wc_chunk;

class wc_splitter : public splitter<wc_chunk> {
public:
	// range: range for every element
	// s: distribution parameter -- the exponent
	wc_splitter(size_t size, size_t range, float s)
		: _buf(new int[size]), _size(size)
	{
		zipf_rng rng;
		zipf_rng_init(&rng, range, s);
		for (size_t i = 0; i < size; ++i)
			_buf[i] = zipf_rng_next(&rng);
	}

	~wc_splitter()
	{ delete [] _buf; }

	// split into nchunk chunks, possibly less
	int
	split(size_t nchunk)
	{
		size_t len = _size / nchunk;
		_parts.clear();
		for (size_t i = 0; i < nchunk - 1; ++i)
			_parts.push_back(pair<int*,int*>(_buf + i * len, _buf + (i + 1) * len));
		_parts.push_back(pair<int*,int*>(_buf + (nchunk - 1) * len, _buf + _size));
		return 0;
	}

	size_t
	size() const
	{ return _parts.size(); }

	wc_chunk
	chunk(size_t n) const
	{ return wc_chunk(_parts[n].first, _parts[n].second); }

private:
	int    *_buf;
	size_t	_size;
	vector< pair<int*,int*> > _parts;
};

int main(int argc, char *argv[])
{
	int    oc;
	size_t ntask = sysconf(_SC_NPROCESSORS_ONLN);
	size_t nslot = sysconf(_SC_NPROCESSORS_ONLN);
	int    range = 0x10000;
	float  s     = 0.0;
	size_t size  = 10000000;
	bool   check = false;
	char  * file = NULL;

	nslot *= nslot;

	while ((oc = getopt(argc, argv, "t:k:n:r:s:w:zh")) != -1) {
		switch (oc) {
		case 't':
			ntask = std::min((size_t)strtoul(optarg, 0, 10), ntask);
			break;
		case 'k':
			nslot = strtoul(optarg, 0, 10);
			break;
		case 'n':
			size  = strtoul(optarg, 0, 10);
			break;
		case 'r':
			range = atoi(optarg);
			break;
		case 's':
			s     = atof(optarg);
			break;
		case 'w':
			file = optarg;
			break;
		case 'z':
			check = true;
			break;
		case 'h':
			printf(usage, argv[0]);
			exit(EXIT_SUCCESS);
		default:
			exit(EXIT_FAILURE);
		}
	}

	typedef wc_splitter Splitter;

	typedef multi_hash_runtime<
		wc_splitter, size_t, size_t, wc_mapper, mapcombine::simple_partition<size_t> > Runtime;

	typedef Runtime::storage_type Storage;

	// for verification
	typedef open_hash_map<Storage::key_type, Storage::value_type> Counter;

	// three elements of a computation
	Splitter splitter(size, range, s);
	Storage	 storage(nslot);
	Runtime	 runtime(splitter, storage);

	timespec timer;
	timer_start(&timer);
	runtime.run(ntask);
	float elapsed = timer_stop(&timer);

	printf("nslot=%lu, range=%d, s=%f, size=%lu, elapsed=%f\n",
	       (unsigned long)nslot, range, s, (unsigned long)size, elapsed);

	splitter.split(1);
	Splitter::chunk_type chunk = splitter.chunk(0);

	if (file) {
		FILE *fp = fopen(file, "wb");
		if (fp == NULL) {
			fprintf(stderr, "cannot open %s\n", file);
			exit(EXIT_FAILURE);
		}
		for (Splitter::chunk_type::const_iterator it = chunk.begin();
		     it != chunk.end(); ++it) {
			Splitter::chunk_type::value_type r = *it;
			fwrite(&r, sizeof(r), 1, fp);
		}
		fclose(fp);
	}

	if (check) {
		Counter counter;
		timer_start(&timer);
		for (Splitter::chunk_type::iterator it = chunk.begin();
		     it != chunk.end(); ++it)
			++counter[*it];
		elapsed = timer_stop(&timer);
		fprintf(stderr, "build counter successfully: %f sec\n", elapsed);
		for (Counter::const_iterator it = counter.begin(); it != counter.end(); ++it) {
			if (it.value() != storage[it.key()]) {
				fprintf(stderr, "expect %zu, actual %zu for key %zu\n",
					storage[it.key()], it.value(), it.key().key());
				exit(EXIT_FAILURE);
			}
		}
		fprintf(stderr, "backward check OK\n");
		for (Storage::const_iterator it = storage.begin(); it != storage.end(); ++it) {
			if (it.value() != counter[it.key()]) {
				fprintf(stderr, "expect %zu, actual %zu for key %zu\n",
					counter[it.key()], it.value(), it.key().key());
				exit(EXIT_FAILURE);
			}
		}
		fprintf(stderr, "forward check OK\n");
	}

	return 0;
}
