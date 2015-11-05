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

// This file implements the Proxy Synchronization Model.
// When two or more threads try to modify the same critical
// section, the first arriving thread is always responsible for
// processing the load queue until it finishes all the load and marks
// the queue as done. If other threads arrive while the section is in
// use, load from the other threads will be delivered to the working
// thread. Thus the working thread acts as a 'proxy'.

#ifndef _ULIB_MC_SYNC_H
#define _ULIB_MC_SYNC_H

#include <stddef.h>
#include <ulib/os_atomic_intel64.h>

namespace ulib {

namespace mapcombine {

// PSM data node
template<typename T>
struct psm_node {
	psm_node(const T &d) : next(NULL), data(d) { }
	psm_node *next;
	T data;
};

// PSM synchronization queue.
template<typename T>
struct psm_queue {
	psm_queue() : tail(NULL) { }
	psm_node<T> *tail;
};

// Process the queued data.
//     q: the psm queue
//     data: new data to append to the queue
//     set: the set to combine the data
// Queued data will be combined into the set.
template<typename T, typename S>
static inline void psm_process_cas(psm_queue<T> &q, const T &data, S &set)
{
	psm_node<T> *node = new psm_node<T>(data);
	psm_node<T> *pred = (psm_node<T> *)atomic_fetchstore64(&q.tail, (int64_t)node);

	if (pred) {
		pred->next = node;
		return;
	}

	// flush the queue
	for (;;) {
		typename S::key_type key(node);  // automatically reclaim memory
		set.combine(key);
		if (node->next == NULL) {  // seemingly no successor
			if (atomic_cmpswp64(&q.tail, (int64_t)node, 0) == (int64_t)node)
				return;
			// got successors, wait for them to appear
			while (node->next == NULL)
				atomic_cpu_relax();
		}
		node = node->next;
	}
}

// Another version using FAS instead of CAS
//     q: the psm queue
//     data: new data to append to the queue
//     set: the set to combine the data
// Queued data will be combined into the set.
template<typename T, typename S>
static inline void psm_process_fas(psm_queue<T> &q, const T &data, S &set)
{
	psm_node<T> *node = new psm_node<T>(data);
	psm_node<T> *pred = (psm_node<T> *)atomic_fetchstore64(&q.tail, (int64_t)node);

	if (pred) {
		pred->next = node;
		return;
	}

	// flush the queue
	for (;;) {
		typename S::key_type key(node);  // automatically reclaim memory
		set.combine(key);
		if (node->next == NULL) {  // seemingly no successor
			pred = (psm_node<T> *)atomic_fetchstore64(&q.tail, 0);
			if (pred == node)
				return;
			psm_node<T> *succ = (psm_node<T> *)
				atomic_fetchstore64(&q.tail, (int64_t)pred);
			// got successors, wait for them to appear
			while (node->next == NULL)
				atomic_cpu_relax();
			if (succ) {
				succ->next = node->next;
				return;
			}
		}
		node = node->next;
	}
}

}  // namespace mapcombine

}  // namespace mapcombine

#endif
