
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include <string>
#include <unistd.h>

#include "utils/utility.h"

#include "base.h"

#include "protocol.h"
#include "dataserver.h"
#include "masterservice.h"
#include "iterator.h"

#include "syncbackend.h"

namespace tinydb
{

BackendSync::BackendSync()
    : m_ThreadQuit( false )
{
    pthread_mutex_init( &m_Lock, NULL );
}

BackendSync::~BackendSync()
{
	m_ThreadQuit = true;
	int retry = 0;
	int MAX_RETRY = 100;
	while( retry++ < MAX_RETRY )
    {
		// there is something wrong that sleep makes other threads
		// unable to acquire the mutex
		{
            pthread_mutex_lock( &m_Lock );
            if( m_Workers.empty() )
            {
	            pthread_mutex_unlock( &m_Lock );
                break;
			}

            pthread_mutex_unlock( &m_Lock );
		}

		usleep(50 * 1000);
	}

	if( retry >= MAX_RETRY )
    {
		LOG_INFO( "Backend worker not exit expectedly.\n" );
	}

    pthread_mutex_destroy( &m_Lock );

	LOG_DEBUG( "BackendSync finalized.\n" );
}

void BackendSync::process( int64_t sid, uint64_t lastseq, const std::string & lastkey )
{
	LOG_INFO( "sid: %lld, accept sync client.\n", sid );

	struct run_arg *arg = new run_arg();
	arg->sid = sid;
    arg->lastseq = lastseq;
    arg->lastkey = lastkey;
    arg->backend = this;

	pthread_t tid;
	int err = pthread_create( &tid, NULL, &BackendSync::sync_backend, arg );
	if(err != 0)
    {
		LOG_ERROR( "can't create thread: %s.\n", strerror(err) );
	    // TODO:关闭链接

    }

    pthread_mutex_lock( &m_Lock );
	m_Workers.insert( std::make_pair( tid, sid ) );
    pthread_mutex_unlock( &m_Lock );
}

void BackendSync::shutdown( int64_t sid )
{
    pthread_mutex_lock( &m_Lock );
	std::map<pthread_t, int64_t>::iterator it;
    for ( it = m_Workers.begin(); it != m_Workers.end(); )
    {
        if ( it->second == sid )
        {
            m_Workers.erase( it++ );
            break;
        }
        else
        {
            ++it;
        }
    }
    pthread_mutex_unlock( &m_Lock );
}

void* BackendSync::sync_backend( void *arg )
{
    struct run_arg *p = (struct run_arg*)arg;
    BackendSync *backend = (BackendSync *)p->backend;
    int64_t sid = p->sid;
    uint64_t lastseq = p->lastseq;
    std::string lastkey = p->lastkey;
    delete p;

    BinlogQueue *logs = CDataServer::getInstance().getBinlog();

    Client client( backend, sid, lastseq, lastkey );
    client.init();

    // sleep longer to reduce logs.find
#define TICK_INTERVAL_MS	300
#define NOOP_IDLES			(3000/TICK_INTERVAL_MS)

    int idle = 0;
    while( !backend->m_ThreadQuit )
    {
        // TODO: test
        //usleep(2000 * 1000);

        if( client.status == Client::OUT_OF_SYNC )
        {
            client.reset();
            continue;
        }

        bool is_empty = true;
        // WARN: MUST do first sync() before first copy(), because
        // sync() will refresh last_seq, and copy() will not
        if( client.sync(logs) )
        {
            is_empty = false;
        }
        if( client.status == Client::COPY )
        {
            if( client.copy() )
            {
                is_empty = false;
            }
        }
        if( is_empty )
        {
            if( idle >= NOOP_IDLES )
            {
                idle = 0;
                client.noop();
            }
            else
            {
                idle ++;
                usleep( TICK_INTERVAL_MS * 1000 );
            }
        }
        else
        {
            idle = 0;
        }

        // TODO : 同步速度
        // TODO : 备机断开连接时
        pthread_mutex_lock( &backend->m_Lock );
        if ( backend->m_Workers.end() ==
                backend->m_Workers.find( pthread_self() ) )
        {
            pthread_mutex_unlock( &backend->m_Lock );
            break;
        }
        pthread_mutex_unlock( &backend->m_Lock );

    }

    LOG_INFO( "Sync Client quit.\n " );

    pthread_mutex_lock( &backend->m_Lock );
    if ( backend->m_Workers.end() !=
            backend->m_Workers.find( pthread_self() ) )
    {
        backend->m_Workers.erase( pthread_self() );
    }
    pthread_mutex_unlock( &backend->m_Lock );

    return (void *)NULL;
}


/* Client */

BackendSync::Client::Client( const BackendSync *backend, int64_t sid, uint64_t lastseq, const std::string & lastkey )
{
	this->status = Client::INIT;
	this->sid = sid;
    this->backend = backend;
	this->lastseq = lastseq;
    this->lastnoopseq = 0ULL;
	this->lastkey = lastkey;
    iter = NULL;
}

BackendSync::Client::~Client()
{
	if(iter)
    {
		delete iter;
		iter = NULL;
	}
}

void BackendSync::Client::init()
{
	if( lastkey == "" && lastseq != 0 )
    {
		this->status = Client::SYNC;
	}
    else
    {
		// a slave must reset its last_key when receiving 'copy_end' command
		this->status = Client::COPY;
	}
}

void BackendSync::Client::reset()
{
	LOG_INFO( "copy begin.\n" );
	this->status = Client::COPY;
	this->lastseq = 0;
	this->lastkey = "";

	Binlog log( this->lastseq, BinlogCommand::BEGIN, "" );
    this->send( BinlogType::COPY, log.repr() );
}

void BackendSync::Client::noop()
{
	uint64_t seq;
	if( this->status == Client::COPY && this->lastkey.empty() )
    {
		seq = 0;
	}
    else
    {
		seq = this->lastseq;
		this->lastnoopseq = this->lastseq;
	}

    Binlog log( seq, BinlogCommand::NONE, "" );
    this->send( BinlogType::NOOP, log.repr() );
}

int BackendSync::Client::copy()
{
    if( this->iter == NULL )
    {
        LOG_INFO( "new iterator, lastkey: '%s'.\n", utils::Utility::hexmem(lastkey.data(), lastkey.size()).c_str() );
        std::string key = this->lastkey;
        if( this->lastkey.empty() )
        {
            key.push_back( DataType::KV );
        }

        this->iter = CDataServer::getInstance().iterator( key, "", -1 );
        LOG_INFO( "iterator created, lastkey: '%s'.\n", utils::Utility::hexmem(lastkey.data(), lastkey.size()).c_str());
    }

    int ret = 0;
    int iterate_count = 0;
    while( true )
    {
        // Prevent copy() from blocking too long
        if( ++iterate_count > 1000 )
        {
            break;
        }

        if( !iter->next() )
        {
            goto copy_end;
        }

        leveldb::Slice key = iter->key();
        if( key.size() == 0 )
        {
            continue;
        }
        // finish copying all valid data types
        if( key.data()[0] > DataType::KV )
        {
            goto copy_end;
        }

        leveldb::Slice val = iter->val();

        char cmd = 0;
        char data_type = key.data()[0];
        if( data_type == DataType::KV )
        {
            cmd = BinlogCommand::SET;
        }
        else
        {
            continue;
        }

        ret = 1;

        // 重置lastkey
        this->lastkey = key.ToString();

        Binlog log( this->lastseq, cmd, key.ToString() );
        this->send( BinlogType::COPY, log.repr(), val.ToString() );
    }

    return ret;

copy_end:
    this->status = Client::SYNC;
    delete this->iter;
    this->iter = NULL;

    Binlog log( this->lastseq, BinlogCommand::END, "" );
    this->send( BinlogType::COPY, log.repr() );

    return 1;
}

int BackendSync::Client::sync( BinlogQueue *logs )
{
	Binlog log;
	while(1)
    {
		int ret = 0;
		uint64_t expect_seq = this->lastseq + 1;
		if( this->status == Client::COPY && this->lastseq == 0 )
        {
			ret = logs->findLast( &log );
		}
        else
        {
			ret = logs->findNext( expect_seq, &log );
		}
		if( ret == 0 )
        {
			return 0;
		}
		if( this->status == Client::COPY && log.key().ToString() > this->lastkey )
        {
			this->lastseq = log.seq();
			// WARN: When there are writes behind last_key, we MUST create
			// a new iterator, because iterator will not know this key.
			// Because iterator ONLY iterates throught keys written before
			// iterator is created.
			if( this->iter )
            {
				delete this->iter;
				this->iter = NULL;
			}
			continue;
		}
		if( this->lastseq != 0 && log.seq() != expect_seq )
        {
			LOG_WARN( "OUT_OF_SYNC! log.seq: %llu, expect_seq: %llu.\n",log.seq(), expect_seq );
			this->status = Client::OUT_OF_SYNC;
			return 1;
		}

		// update last_seq
		this->lastseq = log.seq();
		break;
	}

	bool rc = false;
	std::string val;
	switch( log.cmd() )
    {
		case BinlogCommand::SET:
			rc = CDataServer::getInstance().getMainDB()->get( log.key().ToString(), val );
			if( !rc)
            {
			    LOG_ERROR( "get key=%s error.\n", log.key().ToString().c_str() );
			}
            else
            {
                this->send( BinlogType::SYNC, log.repr(), val );
            }
			break;
		case BinlogCommand::DEL:
            {
                this->send( BinlogType::SYNC, log.repr() );
            }
			break;
	}

    return 1;
}

void BackendSync::Client::send( const char method, const std::string & log, const std::string & value )
{
    SyncResponse response;
    response.method = method;
    response.binlog = log;
    response.value = value;
    g_MasterService->send( sid, &response );
}
}
