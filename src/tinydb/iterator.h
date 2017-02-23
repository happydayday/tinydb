
#ifndef __SRC_TINYDB_ITERATOR_H_
#define __SRC_TINYDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace tinydb
{

class Iterator
{
public:
	enum Direction
    {
		FORWARD, BACKWARD
	};

public:
	Iterator( leveldb::Iterator * it,
			  const std::string & end,
			  uint64_t limit,
			  Direction direction = Iterator::FORWARD );
	~Iterator();
	bool skip(uint64_t offset);
	bool next();

    leveldb::Slice key()
    {
		return it->key();
	}

    leveldb::Slice val()
    {
		return it->value();
	}

private:
	leveldb::Iterator *it;
	std::string end;
	uint64_t limit;
	bool is_first;
	int direction;
};

}
#endif
