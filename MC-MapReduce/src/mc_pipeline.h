/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)

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

// This file implements the Proxy Synchronization Model (PSM) pipelines.
// An array of psm queues will be created with each queue processing a
// portion of the loads. The loads are first shuffled and then passed
// to those queues.

#ifndef _ULIB_MC_PIPELINE_H
#define _ULIB_MC_PIPELINE_H

#include <stdint.h>
#include <ulib/math_bit.h>
#include <ulib/util_class.h>
#include <ulib/mc_typedef.h>
#include <ulib/mc_set.h>
#include <ulib/mc_sync.h>

namespace ulib {

namespace mapcombine {

template< typename _Node, typename _Combiner = additive_combiner<_Node> >
class psm_pipeline : public multi_hash_set<_Node, ulib_except, _Combiner>
{
public:
	typedef _Node node_type;
	typedef typename _Node::data_type data_type;
	typedef multi_hash_set<_Node, ulib_except, _Combiner> set_type;

	psm_pipeline(size_t min)
		: set_type(min)
	{
		assert(min);
		_mask = set_type::bucket_count() - 1;
		_queues = new psm_queue<data_type> [_mask + 1];
	}

	virtual
	~psm_pipeline()
	{
		delete [] _queues;
		// Note that the following is necessary because
		// open hashing does not destruct its elements;
		// otherwise it can be ignored.
		for (typename set_type::iterator it = this->begin();
		     it != this->end(); ++it)
			typename set_type::key_type key = it.key();
	}

	// Note that this implementation relies on the hash value is
	// cached; otherwise it needs to be calculated twice which
	// greatly affect the performance.
	virtual void
	process(const data_type &d)
	{ psm_process_fas(_queues[(size_t)d & _mask], d, *this); }

	// The pipeline capacity is the number of queues.
	virtual size_t
	pipeline_capacity() const
	{ return _mask + 1; }

protected:
	size_t _mask;
	psm_queue<data_type> *_queues;

private:
	psm_pipeline(const psm_pipeline &) { }

	psm_pipeline &
	operator= (const psm_pipeline &)
	{ return *this; }
};

}  // namespace mapcombine

}  // namespace ulib

#endif	/* _ULIB_MC_PIPELINE_H */
