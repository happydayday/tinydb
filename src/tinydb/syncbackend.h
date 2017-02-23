
#ifndef __SRC_TINYDB_SYNCBACKEND_H_
#define __SRC_TINYDB_SYNCBACKEND_H_

#include <vector>
#include <string>
#include <map>

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
    void process( int64_t sid, uint64_t lastseq, const std::string & lastkey );

    // 处理备机断开连接
    void shutdown( int64_t sid );

private :
    static void* sync_backend(void *arg);

private:
	struct Client;

    struct run_arg
    {
		int64_t     sid;
        uint64_t    lastseq;
        std::string lastkey;
        const BackendSync *backend;
	};

private:
	std::vector<Client *> clients;
	std::vector<Client *> clients_tmp;

    volatile bool                   m_ThreadQuit;
	pthread_mutex_t                 m_Lock;
	std::map<pthread_t, int64_t>    m_Workers;
};

struct BackendSync::Client
{
	static const int INIT = 0;
	static const int OUT_OF_SYNC = 1;
	static const int COPY = 2;
	static const int SYNC = 4;

	int                     status;
	int64_t                 sid;
    uint64_t                lastseq;
	uint64_t                lastnoopseq;
	std::string             lastkey;
	const BackendSync *     backend;
	Iterator *              iter;

	Client( const BackendSync *backend, int64_t sid, uint64_t lastseq, const std::string & lastkey );
	~Client();
	void init();
	void reset();
	void noop();
	int copy();
    int sync( BinlogQueue *logs );
    void send( const char method, const std::string & log, const std::string & value = "" );
};

}
#endif
