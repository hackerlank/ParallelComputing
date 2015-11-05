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

#ifndef _ULIB_MC_SPLITTER_H
#define _ULIB_MC_SPLITTER_H

#include <stddef.h>
#include <vector>
#include <utility>
#include <algorithm>
#include <ulib/util_log.h>

namespace ulib {

namespace mapcombine {

// The splitter prototype illustrating the essential methods.
template<typename _Chunk>
struct splitter {
	// The chunk should implement a STL-compatible iterator, by
	// which the record can be accessed.
	typedef _Chunk chunk_type;

	// split into nchunk chunks, possibly LESS. This is not
	// necessarily a problem though, because it can only happen
	// when the data set is small.
	virtual int
	split(size_t nchunk) = 0;

	// Byte size of the data set.
	virtual size_t
	size() const = 0;

	// Get the n-th chunk.
	virtual _Chunk
	chunk(size_t n) const = 0;
};

// A demo array chunk implementation.
template<typename _Record>
class array_chunk {
public:
	typedef _Record value_type;

	array_chunk(_Record *start, _Record *end) : _start(start), _end(end) { }

	struct iterator {
		iterator(_Record *pos = NULL) : _pos(pos) { }

		_Record &
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

		_Record *_pos;
	};

	struct const_iterator {
		const_iterator(const _Record *pos = NULL) : _pos(pos) { }
		const_iterator(const iterator &other) : _pos(other._pos) { }

		const _Record &
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

		const _Record *_pos;
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
	_Record * _start;
	_Record * _end;
};

// A demo text chunk implementation.
class text_chunk {
public:
	typedef struct record {
		const char * str;
		size_t	     len;

		// s is a pointer to the start of a line
		record(const char *s, const char *end)
		{
			if (s < end) {
				str = s;
				s = (const char *)memchr(s, '\n', end - s);
				len = s? s - str: end - str;
			} else {
				str = end;
				len = 0;
			}
		}

		// find the start of the next line
		static const char *
		next(const char *from, const char *end)
		{
			if (from < end) {
				from = (const char *)memchr(from, '\n', end - from);
				return from? from + 1: end;
			}
			return end;
		}
	} value_type;

	text_chunk(const char *from, const char *end)
		: _from(from), _end(end) { }

	struct iterator {
		iterator() { }

		iterator(const char *from, const char *end)
			: _pos(from), _end(end), _off(0) { }

		record
		operator *() const
		{
			record rec(_pos, _end);
			_off = rec.len;
			return rec;
		}

		iterator &
		operator++()
		{
			_pos = record::next(_pos + _off, _end);
			_off = 0;
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

		const char *   _pos;
		const char *   _end;
		mutable size_t _off;
	};

	struct const_iterator {
		const_iterator() { }

		const_iterator(const char *from, const char *end)
			: _pos(from), _end(end), _off(0) { }

		const_iterator(const iterator &other)
			: _pos(other._pos), _end(other._end), _off(other._off) { }

		const record
		operator *() const
		{
			record rec(_pos, _end);
			_off = rec.len;
			return rec;
		}

		const_iterator &
		operator++()
		{
			_pos = record::next(_pos + _off, _end);
			_off = 0;
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

		const char *   _pos;
		const char *   _end;
		mutable size_t _off;
	};

	iterator
	begin()
	{ return iterator(_from, _end); }

	const_iterator
	begin() const
	{ return const_iterator(_from, _end); }

	iterator
	end()
	{ return iterator(_end, _end); }

	const_iterator
	end() const
	{ return const_iterator(_end, _end); }

private:
	const char * _from;
	const char * _end;
};

// A demo text block splitter.
// Used with the text_chunk.
class text_splitter : public splitter<text_chunk> {
public:
	text_splitter(const char *from, const char *end)
		: _from(from), _end(end) { }

	int
	split(size_t nchunk)
	{
		_segments.clear();
		if (nchunk == 0)
			return 0;  // zero chunk is okay
		size_t step = std::max((_end - _from + nchunk) / nchunk, 1ul);
		ULIB_DEBUG("total length = %zu, approximate segment size = %zu", _end - _from, step);
		const char *p = _from;
		while (p < _end) {
			const char *q = p + step;
			if (q >= _end || (q = (const char *)memchr(q, '\n', _end - q)) == NULL) {
				_segments.push_back(std::pair<const char *, const char *>(p, _end));
				ULIB_DEBUG("added segment [%zu,%zu)", p - _from, _end - _from);
				return 0;
			}
			_segments.push_back(std::pair<const char *, const char *>(p, q));
			ULIB_DEBUG("added segment [%zu,%zu)", p - _from, q - _from);
			p = q + 1;
		}
		ULIB_DEBUG("total %zu segment(s)", _segments.size());
		return 0;
	}

	size_t
	size() const
	{ return _segments.size(); }

	text_chunk
	chunk(size_t n) const
	{ return text_chunk(_segments[n].first, _segments[n].second); }

private:
	const char *_from;
	const char *_end;
	std::vector< std::pair<const char *, const char *> > _segments;
};

}  // namesapce mapcombine

}  // ulib

#endif
