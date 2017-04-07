
#include "iterator.h"

namespace tinydb
{

Iterator::Iterator( leveldb::Iterator *it,
		const std::string &end,
		uint64_t limit,
		Direction direction )
    : m_It( it ),
      m_End( end ),
      m_Limit( limit ),
      m_First( true ),
      m_Direction( direction )
{}

Iterator::~Iterator()
{
    if ( m_It != NULL )
    {
        delete m_It;
    }
}

bool Iterator::skip( uint64_t offset )
{
	while( offset-- > 0 )
    {
		if( this->next() == false )
        {
			return false;
		}
	}

	return true;
}

bool Iterator::next()
{
	if( m_Limit == 0 )
    {
		return false;
	}

	if( m_First )
    {
		m_First = false;
	}
    else
    {
		if( m_Direction == FORWARD )
        {
			m_It->Next();
		}
        else
        {
			m_It->Prev();
		}
	}

	if( !m_It->Valid() )
    {
		// make next() safe to be called after previous return false.
		m_Limit = 0;
		return false;
	}
	if( m_Direction == FORWARD )
    {
		if( !m_End.empty() && m_It->key().ToString() > m_End )
        {
			m_Limit = 0;
			return false;
		}
	}
    else
    {
		if( !m_End.empty() && m_It->key().ToString() < m_End )
        {
			m_Limit = 0;
			return false;
		}
	}

	m_Limit --;
	return true;
}

}
