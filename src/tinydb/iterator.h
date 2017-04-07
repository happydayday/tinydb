
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
	virtual ~Iterator();
	bool skip( uint64_t offset );
	bool next();

    leveldb::Slice key()
    {
		return m_It->key();
	}

    leveldb::Slice val()
    {
		return m_It->value();
	}

private:
	leveldb::Iterator *     m_It;
	std::string             m_End;
	uint64_t                m_Limit;
	bool                    m_First;
	int32_t                 m_Direction;
};

}
#endif
