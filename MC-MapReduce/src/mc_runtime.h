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

#ifndef _ULIB_MC_RUNTIME_H
#define _ULIB_MC_RUNTIME_H

#include <stdint.h>
#include <unistd.h>
#include <ulib/util_class.h>
#include <ulib/math_rand_prot.h>
#include <ulib/hash_chain_r.h>
#include <ulib/hash_multi_r.h>
#include <ulib/mc_splitter.h>
#include <ulib/mc_typedef.h>
#include <ulib/mc_task.h>
#include <ulib/mc_pipeline.h>

namespace ulib {

namespace mapcombine {

template<
	typename _Splitter,
	typename _Key,
	typename _Val,
	template<typename _Pipeline> class _Mapper,
	typename _Partition,
	typename _Combiner = additive_combiner<_Val> >
class psm_runtime {
public:
	typedef _Splitter  splitter_type;
	typedef _Key	   key_type;
	typedef _Val	   value_type;
	typedef _Partition partition_type;
	typedef _Combiner  combiner_type;

	// Intermediate pair type.
	// The pair is linked as to be sorted easily in the final
	// stage.
	struct interm_pair {
		struct data_type : public std::pair<_Key, _Val> {
			data_type(const _Key &key, const _Val &val)
				: std::pair<_Key, _Val>(key, val)
			{
				partition_type part;
				hash = part(this->first);
				RAND_INT3_MIX64(hash);
			}

			operator size_t () const
			{ return hash; }

			// the hash value is cached for better
			// performance
			size_t hash;
		};

		// the PSM node holds the data and link
		typedef psm_node<data_type> node_type;

		interm_pair(node_type *n)
			: node(n) { }

		// keep only one copy of the PSM node
		interm_pair(const interm_pair &other)
		{
			node = other.node;
			other.node = NULL;
		}

		virtual
		~interm_pair()
		{ delete node; }

		interm_pair &
		operator =(const interm_pair &other)
		{
			node = other.node;
			other.node = NULL;
			return *this;
		}

		_Key &
		key()
		{ return node->data.first; }

		const _Key &
		key() const
		{ return node->data.first; }

		_Val &
		value()
		{ return node->data.second; }

		const _Val &
		value() const
		{ return node->data.second; }

		operator size_t () const
		{ return node->data.hash; }

		bool
		operator==(const interm_pair &other) const
		{
			// first compare the hash value because it is
			// generally much more efficient than
			// comparing the keys.
			return node->data.hash == other.node->data.hash &&
				node->data.first == other.node->data.first;
		}

		mutable node_type *node;
	};

	struct interm_value_combiner : public combiner<interm_pair> {
		// only combine the values of intermediate pairs
		void
		operator()(interm_pair &sum, const interm_pair &value) const
		{ combiner(sum.value(), value.value());	}

		combiner_type combiner;
	};

	typedef psm_pipeline<interm_pair, interm_value_combiner> pipeline_type;

	typedef _Mapper<pipeline_type> mapper_type;

	typedef task<typename splitter_type::chunk_type, pipeline_type, mapper_type> task_type;

	psm_runtime(splitter_type &sp, pipeline_type &pl)
		: _splitter(sp), _pipeline(pl)
	{ _ncpu = sysconf(_SC_NPROCESSORS_ONLN); }

	void
	run(size_t ntask = 0)
	{
		if (ntask == 0)
			ntask = _ncpu;
		task_type *tasks[ntask];
		int ret = _splitter.split(ntask);
		if (ret) {
			ULIB_FATAL("split failed with nchunk=%zu", ntask);
			return;
		}
		int t = 0;
		for (; t < (int)_splitter.size(); ++t) {
			if (t >= _ncpu) {
				ULIB_FATAL("too many chunks, no greater than %d expected", _ncpu);
				while (--t >= 0)
					delete tasks[t];
				return;
			}
			tasks[t] = new task_type(t, _splitter.chunk(t), _pipeline);
			tasks[t]->start();
		}
		for (int i = 0; i < t; ++i)
			delete tasks[i];
	}

	typename pipeline_type::iterator
	find(const key_type &key)
	{
		typename interm_pair::data_type data(key, value_type());
		typename interm_pair::node_type node(data);
		interm_pair pair(&node);
		typename pipeline_type::iterator it = _pipeline.find(pair);
		// no memory need to be reclaimed
		pair.node = NULL;
		return it;
	}

	typename pipeline_type::const_iterator
	find(const key_type &key) const
	{
		typename interm_pair::data_type data(key, value_type());
		typename interm_pair::node_type node(data);
		interm_pair pair(&node);
		typename pipeline_type::const_iterator it = _pipeline.find(pair);
		// no memory need to be reclaimed
		pair.node = NULL;
		return it;
	}

private:
	splitter_type &_splitter;
	pipeline_type &_pipeline;
	int _ncpu;
};

// General mapcombine runtime and the variants.
template<
	typename _Splitter,
	typename _Key,
	typename _Val,
	template<typename _Storage> class _Mapper,
	typename _Partition,
	typename _Combiner = additive_combiner<_Val>,
	template<typename _SKey, typename _SVal, typename _Except,
		 typename _Combiner, typename _RegionLock> class _Storage = multi_hash_map>
class mc_runtime {
public:
	typedef _Splitter  splitter_type;
	typedef _Key	   key_type;
	typedef _Val	   value_type;
	typedef _Partition partition_type;
	typedef _Combiner  combiner_type;

	typedef class storage_key {
	public:
		storage_key(const key_type &key)
			: _key(key)
		{
			partition_type part;
			uint64_t h = part(key);
			_hash = RAND_INT3_MIX64(h);
		}

		operator size_t () const
		{ return _hash; }

		bool
		operator ==(const storage_key &other) const
		{ return _hash == other._hash && _key == other.key(); }

		const key_type &
		key() const
		{ return _key; }

	private:
		key_type _key;
		size_t	 _hash;
	} storage_key_type;

	typedef _Storage<storage_key, value_type, ulib_except,
			 combiner_type, region_rwlock<ticket_rwlock_t> > storage_type;
	typedef _Mapper<storage_type> mapper_type;

	typedef task<typename splitter_type::chunk_type, storage_type, mapper_type> task_type;

	mc_runtime(splitter_type &sp, storage_type &stor)
		: _splitter(sp), _storage(stor)
	{ _ncpu = sysconf(_SC_NPROCESSORS_ONLN); }

	void
	run(size_t ntask = 0)
	{
		if (ntask == 0)
			ntask = _ncpu;
		task_type *tasks[ntask];
		int ret = _splitter.split(ntask);
		if (ret) {
			ULIB_FATAL("split failed with nchunk=%zu", ntask);
			return;
		}
		int t = 0;
		for (; t < (int)_splitter.size(); ++t) {
			if (t >= _ncpu) {
				ULIB_FATAL("too many chunks, no greater than %d expected", _ncpu);
				while (--t >= 0)
					delete tasks[t];
				return;
			}
			tasks[t] = new task_type(t, _splitter.chunk(t), _storage);
			tasks[t]->start();
		}
		for (int i = 0; i < t; ++i)
			delete tasks[i];
	}

private:
	splitter_type &_splitter;
	storage_type  &_storage;
	int	       _ncpu;
};

template<
	typename _Splitter,
	typename _Key,
	typename _Val,
	template<typename _Storage> class _Mapper,
	typename _Partition,
	typename _Combiner = additive_combiner<_Val> >
class multi_hash_runtime :
		public mc_runtime<_Splitter, _Key, _Val, _Mapper,
				  _Partition, _Combiner, multi_hash_map> {
public:
	typedef mc_runtime<_Splitter, _Key, _Val, _Mapper, _Partition, _Combiner, multi_hash_map> runtime_type;

	multi_hash_runtime(typename runtime_type::splitter_type &sp,
			   typename runtime_type::storage_type &stor)
		: runtime_type(sp, stor) { }
};

template<
	typename _Splitter,
	typename _Key,
	typename _Val,
	template<typename _Storage> class _Mapper,
	typename _Partition,
	typename _Combiner = additive_combiner<_Val> >
class chain_hash_runtime :
		public mc_runtime<_Splitter, _Key, _Val, _Mapper,
			       _Partition, _Combiner, chain_hash_map_r> {
public:
	typedef mc_runtime<_Splitter, _Key, _Val, _Mapper, _Partition, _Combiner, chain_hash_map_r> runtime_type;

	chain_hash_runtime(typename runtime_type::splitter_type &sp,
			   typename runtime_type::storage_type &stor)
		: runtime_type(sp, stor) { }
};

}  // namespace mapcombine

}  // namespace ulib

#endif
