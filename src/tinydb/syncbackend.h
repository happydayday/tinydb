
#ifndef __SRC_TINYDB_SYNCBACKEND_H_
#define __SRC_TINYDB_SYNCBACKEND_H_

#include <vector>
#include <string>
#include <map>

#include "utils/thread.h"

#include "types.h"

#include "binlog.h"

namespace tinydb
{

class Iterator;

class BackendSync
{
public :
	BackendSync();
	~BackendSync();

public :
    void process( uint64_t sid, uint64_t lastseq, const std::string & lastkey );

    // 处理备机断开连接
    void shutdown( uint64_t sid );

    // 获即时同步的备机
    void getSlaveSids( std::vector<uint64_t> & sids );

    // 同步数据
    void send( uint64_t sid, const Binlog & log );

    Iterator* iterator( const std::string & start, const std::string & end, uint64_t limit ) const;

private :
    void send( uint64_t sid, const char method, const std::string & log, const std::string & value = "" );

    static void* sync_backend( void *arg );

private:
	struct Client;

    struct run_arg
    {
		uint64_t            sid;
        uint64_t            lastseq;
        std::string         lastkey;
        const BackendSync * backend;
	};

    utils::Mutex                    m_WorkerMutex;
    volatile bool                   m_ThreadQuit;
    std::map<uint64_t, uint8_t>     m_Workers;
};

struct BackendSync::Client
{
	static const int INIT = 0;
	static const int OUT_OF_SYNC = 1;
	static const int COPY = 2;
	static const int SYNC = 4;

	int                     status;
	uint64_t                sid;
    uint64_t                lastseq;
	uint64_t                lastnoopseq;
	std::string             lastkey;
	BackendSync *           backend;
	Iterator *              iter;

	Client( BackendSync *backend, int64_t sid, uint64_t lastseq, const std::string & lastkey );
	~Client();
	void init();
	void reset();
	void noop();
	int copy();
    int sync( const BinlogQueue *logs );
    void send( const char method, const std::string & log, const std::string & value = "" );
};

class Lock
{
public :
    Lock( utils::Mutex * mutex )
    {
        m_Mutex = mutex;
        m_Mutex->lock();
    }

    virtual ~Lock()
    {
        m_Mutex->unlock();
    }

private :
    // No copying allowed
    Lock( const Lock& ) {}
    void operator=( const Lock& ) {}

private :
    utils::Mutex * m_Mutex;
};

#define g_BackendSync CDataServer::getInstance().getBackendSync()

}
#endif
