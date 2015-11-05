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

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <new>
#include <algorithm>
#include <ulib/util_log.h>
#include <ulib/util_timer.h>
#include <ulib/math_rand_prot.h>
#include <ulib/hash_open.h>
#include <ulib/mc_runtime.h>

static const char *usage =
	"The WordCount Testing\n"
	"Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)\n"
	"usage: %s file\n"
	"options:\n"
	"  -t<ntask>   - number of tasks, defailt is ncpu\n"
	"  -k<nslot>   - number of slots, default is ntask^2\n"
	"  -p	       - whether or not print the result\n"
	"  -z	       - perform correctness check\n"
	"  -h	       - print this message\n";

using namespace std;
using namespace ulib;
using namespace ulib::mapcombine;

struct word {
	const char *str;
	size_t	    len;

	word() { }

	word(const char *s, size_t n)
		: str(s), len(n) { }

	bool
	operator== (const word &other) const
	{ return len == other.len && memcmp(str, other.str, len) == 0; }

	operator size_t () const
	{
		size_t h = 0;
		const unsigned char *p = (const unsigned char *)str;
		const unsigned char *q = p + len;
		while (p < q)
			h = (h << 5) - h + *p++;
		return h;
	}
};

template<typename _Pipeline>
struct wc_mapper : public psm_mapper<_Pipeline, text_chunk::value_type, word, size_t> {
	wc_mapper(_Pipeline &pipe)
		: psm_mapper<_Pipeline, text_chunk::value_type, word, size_t>(pipe) { }

	void
	operator ()(const text_chunk::value_type &rec)
	{
		const char *p = rec.str;
		const char *q = rec.len + p;
		while (p < q && !isalpha(*p))
			++p;
		const char *s;
		for (s = p; s < q;) {
			if (!isalpha(*s)) {
				this->emit(word(p, s - p), 1);
				while (s < q && !isalpha(*s))
					++s;
				p = s;
			} else
				++s;
		}
		if (s > p)
			this->emit(word(p, s - p), 1);
	}
};

typedef psm_runtime<text_splitter, word, size_t, wc_mapper,
		    simple_partition<word> > wc_runtime;

typedef wc_runtime::pipeline_type wc_pipeline;

void prt_res(const wc_pipeline &pipeline)
{
	printf("\n===== Computation Results =====\n");
	for (wc_pipeline::const_iterator it = pipeline.begin();
	     it != pipeline.end(); ++it) {
		const char *s = it.key().key().str;
		size_t len = it.key().key().len;
		for (size_t k = 0; k < len; ++k)
			fprintf(stderr, "%c", s[k]);
		fprintf(stderr, "\t%zu\n", it.key().value());
	}
	printf("===============================\n\n");
}

void chk_res(const char *fmap, size_t size, const wc_pipeline &pipeline,
	     const wc_runtime &runtime)
{
	ulib_timer_t timer;
	open_hash_map<word, size_t> counter;

	timer_start(&timer);
	const char *p = fmap;
	const char *q = fmap + size;
	while (p < q && !isalpha(*p))
		++p;
	const char *s;
	for (s = p; s < q;) {
		if (!isalpha(*s)) {
			++counter[word(p, s - p)];
			while (s < q && !isalpha(*s))
				++s;
			p = s;
		} else
			++s;
	}
	if (s > p)
		++counter[word(p, s - p)];
	float elapsed = timer_stop(&timer);
	ULIB_NOTICE("built counter successfully, %f sec elapsed, %zu key(s)",
		    elapsed, counter.size());
	for (open_hash_map<word, size_t>::const_iterator it = counter.begin();
	     it != counter.end(); ++it) {
		wc_pipeline::const_iterator sit = runtime.find(it.key());
		if (sit == pipeline.end() || it.value() != sit.key().value()) {
			ULIB_FATAL("counter --> pipeline checking failed, %zu -- %zu",
				   it.value(), sit.key().value());
			return;
		}
	}
	ULIB_NOTICE("counter --> pipeline checking succeeded");
	for (wc_pipeline::const_iterator it = pipeline.begin();
	     it != pipeline.end(); ++it) {
		if (it.key().value() != counter[it.key().key()]) {
			ULIB_FATAL("pipeline --> counter checking failed, %zu -- %zu",
				   it.key().value(), counter[it.key().key()]);
			return;
		}
	}
	ULIB_NOTICE("pipeline --> counter checking succeeded");
}

int main(int argc, char *argv[])
{
	int    oc;
	size_t ntask = sysconf(_SC_NPROCESSORS_ONLN);
	size_t nslot = 0;
	bool   print = false;
	bool   check = false;
	char  * file = NULL;

	while ((oc = getopt(argc, argv, "t:k:pzh")) != -1) {
		switch (oc) {
		case 't': ntask = std::min((size_t)strtoul(optarg, 0, 10), ntask); break;
		case 'k': nslot = strtoul(optarg, 0, 10); break;
		case 'p': print = true; break;
		case 'z': check = true; break;
		case 'h': printf(usage, argv[0]); return 0;
		default:  return -1;
		}
	}
	if (optind >= argc) {
		printf(usage, argv[0]);
		return -1;
	}
	if (nslot == 0)
		nslot = ntask * ntask;
	file = argv[optind];

	struct stat fs;
	if (stat(file, &fs)) {
		ULIB_FATAL("retrieve file status failed, file=%s", file);
		return -1;
	}
	ULIB_DEBUG("load file %s, size=%zu", file, (size_t)fs.st_size);
	int fd = open(file, O_RDONLY);
	if (fd == -1) {
		ULIB_FATAL("open file %s failed", file);
		return -1;
	}
	const char *fmap =
		(const char *)mmap(NULL, fs.st_size, PROT_READ,
				   MAP_PRIVATE | MAP_POPULATE, fd, 0);
	if (fmap == (const char *)-1) {
		ULIB_FATAL("cannot map file");
		close(fd);
		return -1;
	}

	ULIB_DEBUG("prepare MapCombine components ...");
	text_splitter splitter(fmap, fmap + fs.st_size);
	wc_pipeline   pipeline(nslot);
	wc_runtime    runtime(splitter, pipeline);

	ULIB_DEBUG("start MapCombine ...");
	timespec timer;
	timer_start(&timer);
	runtime.run(ntask);
	float elapsed = timer_stop(&timer);
	ULIB_NOTICE("task done with %zu task(s), %zu slot(s); %f sec elapsed, %zu key(s)",
		    ntask, nslot, elapsed, pipeline.size());

	if (print)
		prt_res(pipeline);

	if (check)
		chk_res(fmap, fs.st_size, pipeline, runtime);

	munmap((void *)fmap, fs.st_size);
	close(fd);

	return 0;
}
