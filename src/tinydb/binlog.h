
#ifndef __SRC_TINYDB_BINLOG_H__
#define __SRC_TINYDB_BINLOG_H__

#include <string>
#include <pthread.h>

#include "leveldbengine.h"
#include "utils/slice.h"

namespace tinydb
{

class Binlog
{
public:
    Binlog(){}
    Binlog( uint64_t seq, char cmd, const std::string & key );

    int load( const leveldb::Slice & value );
    uint64_t seq() const;
    char cmd() const;
    const Slice key() const;
    const char* data() const { return m_Buf.data(); }
    size_t size() const { return m_Buf.size(); }
    const std::string repr() const { return this->m_Buf;}
    std::string dumps() const;

private:
    std::string     m_Buf;
    static const unsigned int HEADER_LEN = sizeof(uint64_t) + 1;
};

// circular queue
class BinlogQueue
{
public :
#ifdef NDEBUG
    static const int LOG_QUEUE_SIZE  = 10 * 1000 * 1000;
#else
    static const int LOG_QUEUE_SIZE  = 10000;
#endif

public :
    BinlogQueue( LevelDBEngine * engine );
    ~BinlogQueue();

    void begin();
    void rollback();
    bool commit();

    // leveldb put
    void Put( const std::string & key, const std::string & value );
    // leveldb delete
    void Delete( const std::string & key );

    void addLog( char cmd, const std::string & key );

    int get( uint64_t seq, Binlog *log ) const;
    int update( uint64_t seq, char cmd, const std::string &key );

    void flush();

    /** @returns
1 : log.seq greater than or equal to seq
0 : not found
-1: error
*/
    int findNext( uint64_t seq, Binlog *log ) const;
    int findLast( Binlog *log ) const;

private :
    static void* logCleanThreadFunc(void *arg);
    int del(uint64_t seq);
    // [start, end] includesive
    int delRange(uint64_t start, uint64_t end);

    void merge();

public:
    pthread_mutex_t m_Lock;

private:
    LevelDBEngine * m_Engine;
    uint64_t        m_MinSeq;
    uint64_t        m_LastSeq;
    uint64_t        m_TranSeq;
    uint32_t        m_Capacity;

    volatile bool   m_Quit;
};

class Transaction
{
public:
	Transaction( BinlogQueue *logs )
    {
		this->m_Logs = logs;
        pthread_mutex_lock( &m_Logs->m_Lock );
		m_Logs->begin();
	}

	~Transaction()
    {
		// it is safe to call rollback after commit
		m_Logs->rollback();
	    pthread_mutex_unlock( &m_Logs->m_Lock );
	}

private:
	BinlogQueue *       m_Logs;
};

}
#endif
