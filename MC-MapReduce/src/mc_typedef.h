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

#ifndef _ULIB_MC_TYPEDEF_H
#define _ULIB_MC_TYPEDEF_H

#include <ulib/math_rand_prot.h>

namespace ulib {

namespace mapcombine {

//
// Value combiners.
// Merges the values into an aggregate sum and discarding those
// values.
//

// This is the combiner prototype. Any combiner implementation is
// recommended to inherit from this class.
template<typename _Val>
struct combiner {
	typedef _Val value_type;

	// the right 'value' should be const; otherwise the compiler
	// would give lvalue compiling error.
	virtual void
	operator()(_Val &sum, const _Val &value) const = 0;
};

// This is a simple combiner that uses += operator of the value. The
// sum is calculated by repeatly calling the += operator.
template<typename _Val>
struct additive_combiner : public combiner<_Val> {
	void
	operator()(_Val &sum, const _Val &value) const
	{ sum += value; }
};

// The mapper prototype.
//     _Pipeline: intermediate runtime context, users need not care
//     about the meaning of it.
//     _Record: data set record type
//     _Key: the type of key to emit
//     _Val: the type of value to emit
//
// Multiple emits are allowed for a record.
template<typename _Pipeline, typename _Record, typename _Key, typename _Val>
class psm_mapper {
public:
	typedef _Pipeline pipeline_type;
	typedef _Record	  record_type;
	typedef _Key	  key_type;
	typedef _Val	  value_type;

	psm_mapper(_Pipeline &pipe) : _pipeline(pipe) { }

	virtual void
	operator()(const _Record &rec) = 0;

	void
	emit(const _Key &key, const _Val &value)
	{ _pipeline.process(typename pipeline_type::data_type(key, value)); }

protected:
	pipeline_type &_pipeline;
};

template<typename _Storage, typename _Record, typename _Key, typename _Val>
class mc_mapper {
public:
	typedef _Storage storage_type;
	typedef _Record	 record_type;
	typedef _Key	 key_type;
	typedef _Val	 value_type;

	mc_mapper(_Storage &stor)
		: _storage(stor) { }

	virtual void
	operator()(const _Record &rec) = 0;

	void
	emit(const _Key &key, const _Val &value)
	{ _storage.combine(key, value); }

protected:
	storage_type &_storage;
};

// The key partition prototype.
// The key is provided in the mapper.
template<typename _Key>
struct partition {
	typedef _Key key_type;

	virtual size_t
	operator ()(const _Key& key) const = 0;
};

// Simple partition.
// This is exactly a mapper for the key, it does nothing except
// calling the key's size_t method.
template<typename _Key>
struct simple_partition : public partition<_Key> {
	size_t
	operator ()(const _Key& key) const
	{ return key; }
};

// Simple integer partition.
// It relies on the size_t method of the key type.
// An enhancement hash function is used to achieve better hash value
// distribution, which is critical to multi_hash_set performance.
template<typename _Key>
struct int_partition : public partition<_Key> {
	size_t
	operator ()(const _Key& key) const
	{
		// explicitly use uint64_t because the following
		// transformation assumes it.
		uint64_t h = key;
		return RAND_INT3_MIX64(h);
	}
};

} // namespace mapcombine

} // namespace ulib

#endif
