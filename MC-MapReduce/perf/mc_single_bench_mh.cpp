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
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <ulib/util_timer.h>
#include <ulib/math_rand_prot.h>
#include <ulib/hash_multi_r.h>

using namespace std;
using namespace ulib;

template<typename T>
struct additive_combiner : public do_nothing_combiner<T> {
	void
	operator () (T &sum, const T &val) const
	{ sum += val; }
};

struct word {
	const char *str;
	size_t len;
	size_t hash;

	word() { }

	word(const char *s, size_t n)
		: str(s), len(n)
	{
		uint64_t h = 0;
		const unsigned char *p = (const unsigned char *)str;
		const unsigned char *q = p + len;
		while (p < q)
			h = (h << 5) - h + *p++;
		hash = RAND_INT3_MIX64(h);
	}

	bool
	operator== (const word &other) const
	{ return hash == other.hash && len == other.len && memcmp(str, other.str, len) == 0; }

	operator size_t () const
	{ return hash; }
};

int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "usage: %s file\n", argv[0]);
		return -1;
	}

	ulib_timer_t timer;

	multi_hash_map< word, size_t, ulib_except, additive_combiner<size_t> >
		counter(4);

	struct stat fs;
	stat(argv[1], &fs);
	int fd = open(argv[1], O_RDONLY);
	if (fd == -1) {
		fprintf(stderr, "cannot open %s\n", argv[1]);
		return -1;
	}
	char *fmap = (char *)malloc(fs.st_size);
	if (fmap == NULL) {
		close(fd);
		return -1;
	}
	ssize_t size = 0;
	while (size < fs.st_size)
		size += pread(fd, fmap + size, fs.st_size - size, size);

	timer_start(&timer);
	const char *p = fmap;
	const char *q = fmap + size;
	while (p < q && !isalpha(*p))
		++p;
	const char *s;
	for (s = p; s < q;) {
		if (!isalpha(*s)) {
			counter.combine(word(p, s - p), 1);
			while (s < q && !isalpha(*s))
				++s;
			p = s;
		} else
			++s;
	}
	if (s > p)
		counter.combine(word(p, s - p), 1);

	printf("%f sec elapsed\n", timer_stop(&timer));

	printf("total %zu keys\n", counter.size());

	free(fmap);
	close(fd);

	return 0;
}
