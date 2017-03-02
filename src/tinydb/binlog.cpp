
#include <map>
#include <unistd.h>

#include "utils/integer.h"
#include "utils/utility.h"

#include "base.h"
#include "types.h"

#include "syncbackend.h"
#include "dataserver.h"
#include "binlog.h"

/* Binlog */
namespace tinydb
{

Binlog::Binlog( uint64_t seq, char cmd, const std::string & key )
{
    m_Buf.append( (char *)(&seq), sizeof(uint64_t) );
	m_Buf.push_back(cmd);
	m_Buf.append( key.c_str(), key.size() );
}

uint64_t Binlog::seq() const
{
	return *( (uint64_t *)( m_Buf.data() ) );
}

char Binlog::cmd() const
{
	return m_Buf[ sizeof(uint64_t) ];
}

const Slice Binlog::key() const
{
	return Slice( m_Buf.data() + HEADER_LEN, m_Buf.size() - HEADER_LEN );
}

int Binlog::load( const leveldb::Slice & value )
{
	if( value.size() < HEADER_LEN )
    {
		return -1;
	}

    m_Buf.assign( value.data(), value.size() );
	return 0;
}

std::string Binlog::dumps() const
{
	std::string str;
	if( m_Buf.size() < HEADER_LEN )
    {
		return str;
	}

    str.append( utils::Integer().toString( this->seq() ) );

	switch(this->cmd())
    {
		case BinlogCommand::NONE:
			str.append("none ");
			break;
		case BinlogCommand::SET:
			str.append("set ");
			break;
		case BinlogCommand::DEL:
			str.append("del ");
			break;
	}

    Slice key = this->key();
	str.append( utils::Utility::hexmem( key.data(), key.size() ) );
	return str;
}


/* SyncLogQueue */
static inline std::string encode_seq_key( uint64_t seq )
{
	seq = utils::Utility::bigEndian(seq);
	std::string ret;
	ret.push_back(DataType::SYNCLOG);
	ret.append((char *)&seq, sizeof(seq));
	return ret;
}

static inline uint64_t decode_seq_key( const leveldb::Slice & key )
{
	uint64_t seq = 0;
	if( key.size() == (sizeof(uint64_t) + 1) && key.data()[0] == DataType::SYNCLOG )
    {
		seq = *((uint64_t *)(key.data() + 1));
		seq = utils::Utility::bigEndian(seq);
	}

	return seq;
}

BinlogQueue::BinlogQueue( LevelDBEngine * engine )
{
    this->m_Engine = engine;
	this->m_MinSeq = 0;
	this->m_LastSeq = 0;
	this->m_TranSeq = 0;
	this->m_Capacity = LOG_QUEUE_SIZE;

	Binlog log;
	if( this->findLast( &log ) == 1 )
    {
		this->m_LastSeq = log.seq();
	}

	if( this->m_LastSeq > LOG_QUEUE_SIZE )
    {
		this->m_MinSeq = this->m_LastSeq - LOG_QUEUE_SIZE;
	}
    else
    {
		this->m_MinSeq = 0;
	}

	// TODO: use binary search to find out min_seq
	if( this->findNext( this->m_MinSeq, &log ) == 1 )
    {
		this->m_MinSeq = log.seq();
	}

	LOG_INFO( "binlogs capacity: %d, min: %lu, max: %lu\n", m_Capacity, m_MinSeq, m_LastSeq );
}

BinlogQueue::~BinlogQueue()
{
	m_Engine = NULL;
	LOG_DEBUG( "BinlogQueue finalized.\n" );
}

void BinlogQueue::begin()
{
    m_Engine->start();
	m_TranSeq = m_LastSeq;
	m_Engine->txn()->Clear();
}

void BinlogQueue::rollback()
{
	m_TranSeq = 0;
}

bool BinlogQueue::commit()
{
    bool ret = m_Engine->commit();
    if ( ret )
    {
        // 即时同步给备机
        std::vector<uint64_t> slavesids;
        BackendSync * backend = CDataServer::getInstance().getBackendSync();
        if ( backend != NULL )
        {
            backend->getSlaveSids( slavesids );
            if ( !slavesids.empty() )
            {
                Binlog binlog;
                if ( this->get( m_TranSeq, &binlog ) == 1 )
                {
                    for ( size_t i = 0; i < slavesids.size(); ++i )
                    {
                        backend->send( slavesids[i], binlog );
                        LOG_DEBUG( "BinlogQueue::commit(sid:%llu, lastseq:%llu).\n", slavesids[i], m_TranSeq );
                    }
                }
            }
        }

        m_LastSeq = m_TranSeq;
        m_TranSeq = 0;
    }

    // 判断binlog区间
    if ( m_LastSeq > m_MinSeq + LOG_QUEUE_SIZE )
    {
        int ret = this->del( m_MinSeq );
        if ( ret == 0 )
        {
            m_MinSeq += 1;
        }
    }

	return ret;
}

void BinlogQueue::addLog( char cmd, const std::string & key )
{
	m_TranSeq ++;
	Binlog log( m_TranSeq, cmd, key );
	m_Engine->set( encode_seq_key(m_TranSeq), log.repr() );
}

// leveldb put
void BinlogQueue::Put( const std::string & key, const std::string & value )
{
    m_Engine->set( key, value );
}

// leveldb delete
void BinlogQueue::Delete( const std::string & key )
{
    m_Engine->del( key );
}

int BinlogQueue::findNext( uint64_t next_seq, Binlog *log ) const
{
	if( this->get( next_seq, log ) == 1 )
    {
		return 1;
	}

	uint64_t ret = 0;
	std::string key_str = encode_seq_key( next_seq );
	leveldb::ReadOptions iterate_options;
	leveldb::Iterator *it = m_Engine->getDatabase()->NewIterator( iterate_options );
	it->Seek( key_str );
	if( it->Valid() )
    {
		leveldb::Slice key = it->key();
		if( decode_seq_key( key ) != 0 )
        {
			leveldb::Slice val = it->value();
			if( log->load( val ) == -1 )
            {
				ret = -1;
			}
            else
            {
				ret = 1;
			}
		}
	}

	delete it;
	return ret;
}

int BinlogQueue::findLast( Binlog *log ) const
{
	uint64_t ret = 0;
	std::string key_str = encode_seq_key(UINT64_MAX);
	leveldb::ReadOptions iterate_options;
	leveldb::Iterator *it = m_Engine->getDatabase()->NewIterator( iterate_options );
	it->Seek(key_str);
	if( !it->Valid() )
    {
		// Iterator::prev requires Valid, so we seek to last
		it->SeekToLast();
	}
    else
    {
		// UINT64_MAX is not used
		it->Prev();
	}

    if( it->Valid() )
    {
		leveldb::Slice key = it->key();
		if( decode_seq_key( key ) != 0 )
        {
			leveldb::Slice val = it->value();
			if( log->load( val ) == -1 )
            {
				ret = -1;
			}
            else
            {
				ret = 1;
			}
		}
	}

	delete it;
	return ret;
}

int BinlogQueue::get( uint64_t seq, Binlog *log ) const
{
	std::string value;
    if ( m_Engine->get( encode_seq_key(seq), value ) )
    {
        if ( log->load( value ) != -1 )
        {
            return 1;
        }
    }

    return 0;
}

int BinlogQueue::update( uint64_t seq, char cmd, const std::string &key )
{
    Binlog log( seq, cmd, key );
    leveldb::Status s = m_Engine->getDatabase()->Put( leveldb::WriteOptions(), encode_seq_key(seq), log.repr() );
    if( s.ok() )
    {
        return 0;
    }

    return -1;
}

int BinlogQueue::del( uint64_t seq )
{
    leveldb::Status s = m_Engine->getDatabase()->Delete( leveldb::WriteOptions(), encode_seq_key(seq) );
    if( !s.ok() )
    {
        return -1;
    }

    return 0;
}

void BinlogQueue::flush()
{
	delRange( this->m_MinSeq, this->m_LastSeq );
}

int BinlogQueue::delRange( uint64_t start, uint64_t end )
{
	while( start <= end )
    {
		leveldb::WriteBatch batch;
		for( int count = 0; start <= end && count < 1000; start++, count++ )
        {
			batch.Delete( encode_seq_key(start) );
		}

		leveldb::Status s = m_Engine->getDatabase()->Write( leveldb::WriteOptions(), &batch );
		if( !s.ok() )
        {
			return -1;
		}
	}

	return 0;
}

};
