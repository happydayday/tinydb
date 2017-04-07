
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"
#include "version.h"

#include "utils/utility.h"
#include "utils/timeutils.h"
#include "utils/slice.h"

#include "message/message.h"
#include "dataserver.h"
#include "dataservice.h"
#include "middleware.h"
#include "binlog.h"
#include "dumpbackend.h"

#include "clientproxy.h"

namespace tinydb
{

struct LeveldbFetcher
{
    LeveldbFetcher( std::string & data ) : response(data) {}
    ~LeveldbFetcher() {}

    bool operator () ( const std::string & key, const std::string & value )
    {
        std::string prefix;
        utils::Utility::snprintf( prefix, key.size()+512,
                "VALUE %s 0 %d\r\n", key.c_str(), value.size() );
        response += prefix;
        response += value;
        response += "\r\n";
        return true;
    }

    std::string &   response;
};

static inline std::string encode_kv_key( const Slice & key )
{
    std::string buf;
    buf.append( 1, DataType::KV );
    buf.append( key.data(), key.size() );
    return buf;
}

CClientProxy::CClientProxy( int32_t percision, LevelDBEngine * engine )
    : m_Percision( percision ),
      m_Engine( engine ),
      m_Binlogs( NULL ),
      m_DumpThread( 0 )
{
    pthread_mutex_init( &m_QueueLock, NULL );
}

CClientProxy::~CClientProxy()
{
    // 销毁队列锁
    pthread_mutex_destroy( &m_QueueLock );
}

bool CClientProxy::start()
{
    // 创建binlog处理对象
    m_Binlogs = new BinlogQueue( m_Engine );
    assert( m_Binlogs != NULL && "CClientProxy::start new BinlogQueue failed." );

    return true;
}

void CClientProxy::run()
{

    int32_t used_msecs = 0;
    int64_t now = utils::TimeUtils::now();

    {
        // 处理逻辑
        this->execute();
    }

    used_msecs = (int32_t)(utils::TimeUtils::now() - now);
    if ( used_msecs >= 0 && used_msecs < m_Percision )
    {
        utils::TimeUtils::sleep( m_Percision - used_msecs );
    }
}

void CClientProxy::stop()
{
    // 处理全部
    this->execute();

    if ( m_Binlogs != NULL )
    {
        delete m_Binlogs;
        m_Binlogs = NULL;
    }

    LOG_INFO( "CClientProxy Stoped .\n" );
}

void CClientProxy::post( int32_t type, void * task )
{
    pthread_mutex_lock( &m_QueueLock );
    Task tmp( type, task );
    m_TaskQueue.push_back( tmp );
    pthread_mutex_unlock( &m_QueueLock );
}

void CClientProxy::execute()
{
    std::deque<Task> taskqueue;

    // swap
    pthread_mutex_lock( &m_QueueLock );
    std::swap( taskqueue, m_TaskQueue );
    pthread_mutex_unlock( &m_QueueLock );

    // loop, and process
    std::deque<Task>::iterator iter;
    for ( iter = taskqueue.begin(); iter != taskqueue.end(); ++iter )
    {
        this->doTask( *iter );
    }
}

void CClientProxy::doTask( const Task & t )
{
    // 根据类型处理不同的请求
    switch( t.type )
    {
        case eTaskType_Client :
            {
                CacheMessage * msg = static_cast<CacheMessage *>(t.task);
                this->process( msg );
                delete msg;
            }
            break;

        case eTaskType_Middleware :
            {
                IMiddlewareTask * msg = static_cast<IMiddlewareTask *>(t.task);
                msg->process();
                delete msg;
            }
            break;

        default :
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

static const char * MEMCACHED_RESPONSE_STORED       = "STORED\r\n";
static const char * MEMCACHED_RESPONSE_NOT_STORED   = "NOT_STORED\r\n";
static const char * MEMCACHED_RESPONSE_UNKNOWN      = "ERROR unknown command:";
static const char * MEMCACHED_RESPONSE_VALUES_END   = "END\r\n";
static const char * MEMCACHED_RESPONSE_DELETED      = "DELETED\r\n";
static const char * MEMCACHED_RESPONSE_NOT_FOUND    = "NOT_FOUND\r\n";
static const char * MEMCACHED_RESPONSE_VERSION      = "VERSION";
static const char * MEMCACHED_RESPONSE_ERROR        = "ERROR\r\n";
static const char * MEMCACHED_RESPONSE_CLIENTERROR  = "CLIENT_ERROR";
static const char * MEMCACHED_RESPONSE_SERVERERROR  = "SERVER_ERROR";

void CClientProxy::process( CacheMessage * message )
{
    // TODO:  确保消息中的key确实是在本线程中处理

    if ( message->getItem() != NULL )
    {
        if ( message->isCommand( "add" ) )
        {
            this->add( message );
        }
        else if ( message->isCommand( "set" ) )
        {
            this->set( message );
        }
        else if ( message->isCommand( "delete" ) )
        {
            this->del( message );
        }
        else if ( message->isCommand( "incr" ) )
        {
            this->calc( message, 1 );
        }
        else if ( message->isCommand( "decr" ) )
        {
            this->calc( message, -1 );
        }
        // TODO: 增加memcache协议
        else
        {
            this->error( message );
        }
    }
    else
    {
        if ( message->isCommand( "get" ) || message->isCommand( "gets" ) )
        {
            this->gets( message );
        }
        else if ( message->isCommand( "version" ) )
        {
            this->version( message );
        }
        else if ( message->isCommand( "stats" ) )
        {
            this->stat( message );
        }
        else if ( message->isCommand( "dump" ) )
        {
            this->dump( message );
        }
        // TODO: 增加memcache协议
        else
        {
            this->error( message );
        }
    }

}

void CClientProxy::add( CacheMessage * message )
{
    bool rc = false;

    Transaction trans( m_Binlogs );
    std::string key = encode_kv_key( message->getItem()->getKey() );
    m_Binlogs->Put( key, message->getItem()->getValue() );
    m_Binlogs->addLog( BinlogCommand::SET, key );
    rc = m_Binlogs->commit();
    if( rc )
    {
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_STORED, strlen(MEMCACHED_RESPONSE_STORED) );
    }
    else
    {
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_STORED, strlen(MEMCACHED_RESPONSE_NOT_STORED) );
        LOG_ERROR( "CDataServer::add(KEY:'%s') failed .\n", message->getItem()->getKey().c_str() );
    }
}

void CClientProxy::set( CacheMessage * message )
{
    bool rc = false;

    Transaction trans( m_Binlogs );
    std::string key = encode_kv_key( message->getItem()->getKey() );
    m_Binlogs->Put( key, message->getItem()->getValue() );
    m_Binlogs->addLog( BinlogCommand::SET, key );
    rc = m_Binlogs->commit();
    if ( rc )
    {
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_STORED, strlen(MEMCACHED_RESPONSE_STORED) );
    }
    else
    {
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_STORED, strlen(MEMCACHED_RESPONSE_NOT_STORED) );
        LOG_ERROR( "CDataServer::set(KEY:'%s') failed .\n", message->getItem()->getKey().c_str() );
    }

    m_ServerStatus.addSetOps();
}

void CClientProxy::del( CacheMessage * message )
{
    bool rc = false;

    Transaction trans( m_Binlogs );

    std::string key = encode_kv_key( message->getItem()->getKey() );
    m_Binlogs->begin();
    m_Binlogs->Delete( key );
    m_Binlogs->addLog( BinlogCommand::DEL, key );
    rc = m_Binlogs->commit();
    if ( rc )
    {
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_DELETED, strlen(MEMCACHED_RESPONSE_DELETED) );
    }
    else
    {
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_FOUND, strlen(MEMCACHED_RESPONSE_NOT_FOUND) );
        LOG_ERROR( "CDataServer::del(KEY:'%s') failed .\n", message->getItem()->getKey().c_str() );
    }
}

void CClientProxy::gets( CacheMessage * message )
{
    std::string response;
    CacheMessage::Keys::iterator iter;

    for ( iter = message->getKeyList().begin(); iter != message->getKeyList().end(); ++iter )
    {
        std::string key = encode_kv_key( *iter );
        int32_t npos = key.find( "*" );
        if ( npos != -1 )
        {
            std::string prefix = key.substr( 0, npos );

            LeveldbFetcher fetcher( response );
            CDataServer::getInstance().getStorageEngine()->foreach( prefix, fetcher );

            continue;
        }

        Value value;
        bool rc = CDataServer::getInstance().getStorageEngine()->get( key, value );
        if ( rc )
        {
            std::string prefix;
            utils::Utility::snprintf( prefix, key.size()+512,
                    "VALUE %s 0 %d\r\n", key.c_str(), value.size() );
            response += prefix;
            response += value;
            response += "\r\n";
        }

        m_ServerStatus.addGetOps();
    }

    response += MEMCACHED_RESPONSE_VALUES_END;
    CDataServer::getInstance().getService()->send( message->getSid(), response );
}

void CClientProxy::stat( CacheMessage * message )
{
    char data[ 512 ];
    std::string response;
    uint64_t sec = 0, usec = 0;

    m_ServerStatus.refresh();

    sprintf( data, "STAT pid %u\r\n", m_ServerStatus.getPid() );
    response += data;

    sprintf( data, "STAT uptime %ld\r\n", m_ServerStatus.getNowTime() - m_ServerStatus.getStartTime() );
    response += data;

    sprintf( data, "STAT time %ld\r\n", m_ServerStatus.getNowTime() );
    response += data;

    m_ServerStatus.getUserUsage( sec, usec );
    sprintf( data, "STAT rusage_user %ld.%06ld\r\n", sec, usec );
    response += data;

    m_ServerStatus.getSystemUsage( sec, usec );
    sprintf( data, "STAT rusage_system %ld.%06ld\r\n", sec, usec );
    response += data;

    response += "STAT curr_items 0\r\n";
    response += "STAT total_items 0\r\n";

    sprintf( data, "STAT cmd_get %ld\r\n", m_ServerStatus.getGetOps() );
    response += data;
    sprintf( data, "STAT cmd_set %ld\r\n", m_ServerStatus.getSetOps() );
    response += data;

    response += "STAT get_hits 0\r\n";
    response += "STAT get_misses 0\r\n";
    response += "END\r\n";

    CDataServer::getInstance().getService()->send( message->getSid(), response );
}

void CClientProxy::error( CacheMessage * message )
{
    std::string err = MEMCACHED_RESPONSE_UNKNOWN;
    err += message->getCmd();
    err += "\r\n";

    CDataServer::getInstance().getService()->send( message->getSid(), err );
}

void CClientProxy::version( CacheMessage * message )
{
    std::string version = MEMCACHED_RESPONSE_VERSION;

    version += " ";
    version += __APPVERSION__;
    version += "\r\n";

    CDataServer::getInstance().getService()->send( message->getSid(), version );
}

void CClientProxy::calc( CacheMessage * message, int32_t value )
{
    Value v;
    bool rc = false;
    char strvalue[ 64 ] = { 0 };

    rc = CDataServer::getInstance().getStorageEngine()->get( message->getItem()->getKey(), v );
    if ( !rc )
    {
        // 未找到
        CDataServer::getInstance().getService()->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_FOUND, strlen(MEMCACHED_RESPONSE_NOT_FOUND) );
        return;
    }

    if ( message->getDelta() == 0 )
    {
        strncpy( strvalue, v.c_str(), 63 );
    }
    else
    {
        uint64_t rawvalue = (uint64_t)atoll( v.c_str() );
        int64_t change = value * (int64_t)message->getDelta();

        if ( (change>0 && rawvalue+change<rawvalue)
                || (change<0 && rawvalue<(uint64_t)(~change+1)) )
        {
            std::string err = MEMCACHED_RESPONSE_CLIENTERROR;
            err += " ";
            err += "cannot increment or decrement non-numeric value";
            err += "\r\n";
            CDataServer::getInstance().getService()->send( message->getSid(), err );
            return;
        }

        rawvalue += change;
        sprintf( strvalue, "%lu", rawvalue );

        Transaction trans( m_Binlogs );
        std::string key = encode_kv_key( message->getItem()->getKey() );
        m_Binlogs->Put( key, std::string( strvalue ) );
        m_Binlogs->addLog( BinlogCommand::SET, key );
        rc = m_Binlogs->commit();
        if ( !rc )
        {
            // 未存档
            CDataServer::getInstance().getService()->send( message->getSid(),
                    MEMCACHED_RESPONSE_ERROR, strlen(MEMCACHED_RESPONSE_ERROR) );
            return;
        }
    }

    std::string response;
    response += strvalue;
    response += "\r\n";
    CDataServer::getInstance().getService()->send( message->getSid(), response );
}

void CClientProxy::dump( CacheMessage * message )
{
    std::string response;

    if ( m_DumpThread != 0 && utils::IThread::check( m_DumpThread ) )
    {
        response += MEMCACHED_RESPONSE_SERVERERROR;
        response += " ";
        response += "the dump of the thread already exists";
        response += "\r\n";
        CDataServer::getInstance().getService()->send( message->getSid(), response );
        return;
    }

    DumpThreadArgs * args = new DumpThreadArgs;
    args->sid = message->getSid();

    if ( pthread_create( &m_DumpThread, NULL, tinydb::dump_backend, args ) != 0 )
    {
        response += MEMCACHED_RESPONSE_SERVERERROR;
        response += " ";
        response += "create the dump of the thread failed";
        CDataServer::getInstance().getService()->send( message->getSid(), response );
        delete args;
    }
}

bool CClientProxy::checkDiskUsage()
{
    if ( CDataServer::getInstance().getStorageEngine() != NULL )
    {
        return CDataServer::getInstance().getStorageEngine()->check( 5 );
    }

    return true;
}

}
