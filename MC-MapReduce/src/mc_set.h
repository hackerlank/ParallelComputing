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

#ifndef _ULIB_MC_SET_H
#define _ULIB_MC_SET_H

#include <assert.h>
#include <ulib/util_class.h>
#include <ulib/hash_open.h>
#include <ulib/math_bit.h>

namespace ulib {

namespace mapcombine {

template<typename _Key, typename _Except = ulib_except,
	 typename _Combiner = do_nothing_combiner<_Key> >
class multi_hash_set
{
public:
	typedef open_hash_set<_Key, _Except>	  hash_set_type;
	typedef typename hash_set_type::key_type  key_type;
	typedef typename hash_set_type::size_type size_type;

	multi_hash_set(size_t mhash)
	{
		assert(mhash > 0);
		assert(sizeof(mhash) == 4 || sizeof(mhash) == 8);
		if (sizeof(mhash) == 8)
			ROUND_UP64(mhash);
		else
			ROUND_UP32(mhash);
		_mask = mhash - 1;
		_ht = new hash_set_type [mhash];
	}

	virtual
	~multi_hash_set()
	{ delete [] _ht; }

	struct iterator
	{
		typedef typename multi_hash_set<_Key, _Except, _Combiner>::key_type  key_type;
		typedef typename multi_hash_set<_Key, _Except, _Combiner>::size_type size_type;

		iterator(size_t id, size_t nht, hash_set_type *ht,
			 const typename hash_set_type::iterator &itr)
			: _hid(id), _nht(nht), _ht(ht), _cur(itr) { }

		iterator() { }

		key_type &
		key() const
		{ return _cur.key(); }

		bool
		value() const
		{ return _cur.value(); }

		bool
		operator*() const
		{ return value(); }

		iterator&
		operator++()
		{
			if (_hid < _nht) {
				++_cur;
				if (_cur == _ht[_hid].end()) {
					while (++_hid < _nht && _ht[_hid].size() == 0)
						;
					if (_hid < _nht)
						_cur = _ht[_hid].begin();
					else
						_cur = _ht[_nht - 1].end();
				}
			}
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
		operator==(const iterator &other) const
		{ return _hid == other._hid && _cur == other._cur; }

		bool
		operator!=(const iterator &other) const
		{ return _hid != other._hid || _cur != other._cur; }

		size_t _hid;
		size_t _nht;
		hash_set_type *_ht;
		typename hash_set_type::iterator _cur;
	};

	struct const_iterator
	{
		typedef const typename multi_hash_set<_Key, _Except, _Combiner>::key_type key_type;
		typedef typename multi_hash_set<_Key, _Except, _Combiner>::size_type	  size_type;

		const_iterator(size_t id, size_t nht, const hash_set_type *ht,
			       const typename hash_set_type::const_iterator &itr)
			: _hid(id), _nht(nht), _ht(ht), _cur(itr) { }

		const_iterator() { }

		const_iterator(const iterator &it)
			: _hid(it._hid), _nht(it._nht), _ht(it._ht), _cur(it._cur) { }

		key_type &
		key() const
		{ return _cur.key(); }

		bool
		value() const
		{ return _cur.value(); }

		bool
		operator*() const
		{ return value(); }

		const_iterator &
		operator++()
		{
			if (_hid < _nht) {
				++_cur;
				if (_cur == _ht[_hid].end()) {
					while (++_hid < _nht && _ht[_hid].size() == 0)
						;
					if (_hid < _nht)
						_cur = _ht[_hid].begin();
					else
						_cur = _ht[_nht - 1].end();
				}
			}
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
		operator==(const const_iterator &other) const
		{ return _hid == other._hid && _cur == other._cur; }

		bool
		operator!=(const const_iterator &other) const
		{ return _hid != other._hid || _cur != other._cur; }

		size_t _hid;
		size_t _nht;
		const hash_set_type *_ht;
		typename hash_set_type::const_iterator _cur;
	};

	iterator
	begin()
	{
		size_t hid = 0;
		while (hid <= _mask && _ht[hid].size() == 0)
			++hid;
		if (hid <= _mask)
			return iterator(hid, _mask + 1, _ht, _ht[hid].begin());
		else
			return end();
	}

	iterator
	end()
	{ return iterator(_mask + 1, _mask + 1, _ht, _ht[_mask].end()); }

	const_iterator
	begin() const
	{
		size_t hid = 0;
		while (hid <= _mask && _ht[hid].size() == 0)
			++hid;
		if (hid <= _mask)
			return const_iterator(hid, _mask + 1, _ht, _ht[hid].begin());
		else
			return end();
	}

	const_iterator
	end() const
	{ return const_iterator(_mask + 1, _mask + 1, _ht, _ht[_mask].end()); }

	iterator
	insert(const _Key &key)
	{
		size_t m = (size_t)key & _mask;
		typename hash_set_type::iterator it =
			_ht[m].insert(key);
		return it == _ht[m].end()? end(): iterator(m, _mask + 1, _ht, it);
	}

	bool
	contain(const _Key &key) const
	{
		size_t m = (size_t)key & _mask;
		return _ht[m].contain(key);
	}

	bool
	operator[](const _Key &key) const
	{ return contain(key); }

	iterator
	find(const _Key &key)
	{
		size_t m = (size_t)key & _mask;
		typename hash_set_type::iterator it = _ht[m].find(key);
		return it == _ht[m].end()? end(): iterator(m, _mask + 1, _ht, it);
	}

	const_iterator
	find(const _Key &key) const
	{
		size_t m = (size_t)key & _mask;
		typename hash_set_type::const_iterator it = _ht[m].find(key);
		return it == _ht[m].end()? end(): const_iterator(m, _mask + 1, _ht, it);
	}

	void
	combine(const _Key &key)
	{
		size_t m = (size_t)key & _mask;
		typename hash_set_type::iterator it = _ht[m].find(key);
		if (it == _ht[m].end())
			_ht[m].insert(key);
		else
			_combiner(it.key(), key);
	}

	void
	erase(const _Key &key)
	{ _ht[(size_t)key & _mask].erase(key); }

	void
	erase(const iterator &it)
	{ _ht[it._hid].erase(it._cur); }

	void
	clear()
	{
		for (size_t i = 0; i <= _mask; ++i)
			_ht[i].clear();
	}

	size_t
	bucket_count() const
	{ return _mask + 1; }

	size_t
	size() const
	{
		size_t n = 0;
		for (size_t i = 0; i <= _mask; ++i)
			n += _ht[i].size();
		return n;
	}

private:
	multi_hash_set(const multi_hash_set &other) { }

	multi_hash_set &
	operator= (const multi_hash_set &other)
	{ return *this; }

	size_t _mask;
	hash_set_type *_ht;
	_Combiner _combiner;
};

}  // namespace mapcombine

}  // namespace ulib

#endif	/* _ULIB_MC_SET_H */
