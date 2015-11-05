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

#ifndef _ULIB_MC_TASK_H
#define _ULIB_MC_TASK_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <pthread.h>

#include <unistd.h>
#include <ulib/os_thread.h>
#include <ulib/mc_typedef.h>

namespace ulib {

namespace mapcombine {

// Parallel task prototype.
// Each task owns a unique data chunk and works on that chunk.
template<typename _Chunk, typename _Pipeline, typename _Mapper>
class task : public _Mapper, public thread
{
public:
	typedef _Chunk	  chunk_type;
	typedef _Pipeline pipeline_type;
	typedef _Mapper	  mapper_type;

	task(int cpuid, const chunk_type &chunk, pipeline_type &pipe)
		: mapper_type(pipe), _cpuid(cpuid), _chunk(chunk) { }

	virtual
	~task()
	{ stop_and_join(); }

private:
	int
	setup()
	{
		// each task is assigned to a unique processor
		cpu_set_t cpu_set;
		CPU_ZERO(&cpu_set);
		CPU_SET(_cpuid, &cpu_set);
		return pthread_setaffinity_np(thread::_tid, sizeof(cpu_set), &cpu_set);
	}

	int
	run()
	{
		// iteratively process the chunk
		for (typename chunk_type::iterator it = _chunk.begin(); it != _chunk.end(); ++it)
			(*this)(*it);
		return 0;
	}

	int	   _cpuid;
	chunk_type _chunk;
};

}  // namespace mapcombine

}  // namespace ulib

#endif
