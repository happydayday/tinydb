
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"
#include "version.h"

#include "utils/utility.h"
#include "utils/timeutils.h"
#include "utils/slice.h"

#include "config.h"
#include "message.h"
#include "dataservice.h"
#include "masterservice.h"
#include "slaveclient.h"
#include "dumpbackend.h"
#include "dataserver.h"
#include "binlog.h"
#include "middleware.h"
#include "slave.h"
#include "syncbackend.h"
#include "iterator.h"

#include "leveldbengine.h"

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

CDataServer::CDataServer()
    : m_DataService( NULL ),
      m_MasterService( NULL ),
      m_SlaveClient( NULL ),
      m_DumpThread( 0 ),
      m_MainDB( NULL ),
      m_MetaDB( NULL ),
      m_Binlogs( NULL ),
      m_Slave( NULL ),
      m_BackendSync( NULL )
{}

CDataServer::~CDataServer()
{}

bool CDataServer::onStart()
{
    // 读取配置
    uint16_t port = CDatadConfig::getInstance().getListenPort();
    const char * host = CDatadConfig::getInstance().getBindHost();

    // 初始化数据库
    std::string main_db_path = CDatadConfig::getInstance().getStorageLocation() + "/data";

    // 数据数据库
    m_MainDB = new LevelDBEngine( main_db_path );
    if ( m_MainDB == NULL )
    {
        return false;
    }

    m_MainDB->setCacheSize( CDatadConfig::getInstance().getCacheSize() );
    if ( !m_MainDB->initialize() )
    {
        return false;
    }

    // DataService
    m_DataService = new CDataService(
            eDataService_ThreadsCount,
            eDataService_SessionsCount );
    if ( m_DataService == NULL )
    {
        return false;
    }

    if ( !m_DataService->listen( host, port ) )
    {
        LOG_FATAL( "CDataService(%d, %d) listen (%s::%d) failure .\n",
                eDataService_ThreadsCount, eDataService_SessionsCount, host, port );
        return false;
    }

    LOG_INFO( "CDataService(%d, %d) listen (%s::%d) succeed .\n",
            eDataService_ThreadsCount, eDataService_SessionsCount, host, port );

    // 数据库同步
    m_BackendSync = new BackendSync();

    // 双机热备
    if ( !startReplicationService() )
    {
        return false;
    }

    return true;
}

void CDataServer::onStop()
{
    this->cleanup();

    if ( m_DataService != NULL )
    {
        m_DataService->stop();
        delete m_DataService;
        m_DataService = NULL;
    }

    if ( m_MasterService != NULL )
    {
        m_MasterService->stop();
        delete m_MasterService;
        m_MasterService= NULL;
    }

    if ( m_SlaveClient != NULL )
    {
        m_SlaveClient->shutdown( m_SlaveClient->getSid() );

        m_SlaveClient->stop();
        delete m_SlaveClient;
        m_SlaveClient = NULL;
    }

    if ( m_Binlogs != NULL )
    {
        delete m_Binlogs;
        m_Binlogs = NULL;
    }

    if ( m_Slave != NULL )
    {
        delete m_Slave;
        m_Slave = NULL;
    }

    if ( m_BackendSync != NULL )
    {
        delete m_BackendSync;
        m_BackendSync = NULL;
    }

    if ( m_MainDB != NULL )
    {
        m_MainDB->finalize();
        delete m_MainDB;
        m_MainDB = NULL;
    }

    if ( m_MetaDB != NULL )
    {
        m_MetaDB->finalize();
        delete m_MetaDB;
        m_MetaDB = NULL;
    }

    LOG_INFO( "CDataServer Stoped .\n" );
}

void CDataServer::onTask( int32_t type, void * task )
{
    // 根据类型处理不同的请求
    switch( type )
    {
        case eTaskType_Client :
            {
                CacheMessage * t = static_cast<CacheMessage *>(task);
                this->processClient( t );
                delete t;
            }
            break;

        case eTaskType_DataSlave :
            {
                SSMessage * t = static_cast<SSMessage *>( task );
                this->processSlave( t );
                delete t;
            }
            break;

        case eTaskType_DataMaster :
            {
                SSMessage * t = static_cast<SSMessage *>( task );
                if ( m_Slave != NULL )
                {
                    m_Slave->process( t );
                }
                delete t;
            }
            break;

        case eTaskType_Middleware :
            {
                IMiddlewareTask * t = static_cast<IMiddlewareTask *>(task);
                t->process();
                delete t;
            }
            break;

        default :
            break;
    }
}

bool CDataServer::startReplicationService()
{
    ReplicationConfig * config = CDatadConfig::getInstance().getReplicationConfig();
    std::string meta_db_path = CDatadConfig::getInstance().getStorageLocation() + "/meta";

    // 主数据服
    if ( config->type == 0 )
    {
        // 主机
        m_MasterService = new CMasterService( 1, 32 );
        assert( m_MasterService != NULL && "create CMasterService failed" );

        // 设置超时时间
        m_MasterService->setTimeoutSeconds( config->timeoutseconds );

        // 打开服务器
        if ( !m_MasterService->listen(
                    config->endpoint.host.c_str(), config->endpoint.port ) )
        {
            LOG_FATAL( "CMasterService(1, 32) listen (%s::%d) failed .\n",
                    config->endpoint.host.c_str(), config->endpoint.port );
            return false;
        }

        LOG_INFO( "CMasterService(1, 32) listen (%s::%d) succeed .\n",
                config->endpoint.host.c_str(), config->endpoint.port );

        // binlog
        m_Binlogs = new BinlogQueue( m_MainDB );

    }
    else if ( config->type == 1 )
    {
        // 从库索引数据库
        m_MetaDB = new LevelDBEngine( meta_db_path );
        if ( m_MetaDB == NULL )
        {
            return false;
        }

        m_MetaDB->setCacheSize( CDatadConfig::getInstance().getCacheSize() );

        if ( !m_MetaDB->initialize() )
        {
            return false;
        }

        // 从机
        m_SlaveClient = new CSlaveClient( config->keepaliveseconds, config->timeoutseconds );
        assert( m_SlaveClient != NULL && "create CSlaveClient failed" );

        // 连接主机
        if ( !m_SlaveClient->connect( config->endpoint.host.c_str(), config->endpoint.port, 10 ) )
        {
            LOG_FATAL( "CSlaveClient() connect 2 CMasterService(%s::%d) failed .\n",
                    config->endpoint.host.c_str(), config->endpoint.port );
            return false;
        }

        m_Slave = new Slave( m_MetaDB );
    }

    return true;
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

void CDataServer::processClient( CacheMessage * message )
{
    // TODO: spriteray, 确保消息中的key确实是在本线程中处理
    // CDataServer::getShardIndex( const char * key )

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

// 处理来自备机的消息
void CDataServer::processSlave( SSMessage * msg )
{

    switch ( msg->head.cmd )
    {
        case eSSCommand_SyncRequest :
            {
                if ( m_BackendSync == NULL )
                {
                    return;
                }

                SyncRequest * request = (SyncRequest *)msg;
                m_BackendSync->process( request->sid, request->lastseq, request->lastkey );
            }
            break;

        default :
            break;
    }
}

void CDataServer::add( CacheMessage * message )
{
    bool rc = false;

    Transaction trans( m_Binlogs );
    std::string key = encode_kv_key( message->getItem()->getKey() );
    m_Binlogs->Put( key, message->getItem()->getValue() );
    m_Binlogs->addLog( BinlogCommand::SET, key );
    rc = m_Binlogs->commit();
    if( rc )
    {
        m_DataService->send( message->getSid(),
                MEMCACHED_RESPONSE_STORED, strlen(MEMCACHED_RESPONSE_STORED) );
    }
    else
    {
        m_DataService->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_STORED, strlen(MEMCACHED_RESPONSE_NOT_STORED) );
        LOG_ERROR( "CDataServer::add(KEY:'%s') failed .\n", message->getItem()->getKey().c_str() );
    }
}

void CDataServer::set( CacheMessage * message )
{
    bool rc = false;

    Transaction trans( m_Binlogs );
    std::string key = encode_kv_key( message->getItem()->getKey() );
    m_Binlogs->Put( key, message->getItem()->getValue() );
    m_Binlogs->addLog( BinlogCommand::SET, key );
    rc = m_Binlogs->commit();
    if ( rc )
    {
        m_DataService->send( message->getSid(),
                MEMCACHED_RESPONSE_STORED, strlen(MEMCACHED_RESPONSE_STORED) );
    }
    else
    {
        m_DataService->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_STORED, strlen(MEMCACHED_RESPONSE_NOT_STORED) );
        LOG_ERROR( "CDataServer::set(KEY:'%s') failed .\n", message->getItem()->getKey().c_str() );
    }

    m_ServerStatus.addSetOps();
}

void CDataServer::del( CacheMessage * message )
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
        m_DataService->send( message->getSid(),
                MEMCACHED_RESPONSE_DELETED, strlen(MEMCACHED_RESPONSE_DELETED) );
    }
    else
    {
        m_DataService->send( message->getSid(),
                MEMCACHED_RESPONSE_NOT_FOUND, strlen(MEMCACHED_RESPONSE_NOT_FOUND) );
        LOG_ERROR( "CDataServer::del(KEY:'%s') failed .\n", message->getItem()->getKey().c_str() );
    }
}

void CDataServer::gets( CacheMessage * message )
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
            m_MainDB->foreach( prefix, fetcher );

            continue;
        }

        Value value;
        bool rc = m_MainDB->get( key, value );
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
    m_DataService->send( message->getSid(), response );
}

void CDataServer::stat( CacheMessage * message )
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

    m_DataService->send( message->getSid(), response );
}

void CDataServer::error( CacheMessage * message )
{
    std::string err = MEMCACHED_RESPONSE_UNKNOWN;
    err += message->getCmd();
    err += "\r\n";

    m_DataService->send( message->getSid(), err );
}

void CDataServer::version( CacheMessage * message )
{
    std::string version = MEMCACHED_RESPONSE_VERSION;

    version += " ";
    version += __APPVERSION__;
    version += "\r\n";

    m_DataService->send( message->getSid(), version );
}

void CDataServer::calc( CacheMessage * message, int32_t value )
{
    Value v;
    bool rc = false;
    char strvalue[ 64 ] = { 0 };

    rc = m_MainDB->get( message->getItem()->getKey(), v );
    if ( !rc )
    {
        // 未找到
        m_DataService->send( message->getSid(),
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
            m_DataService->send( message->getSid(), err );
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
            m_DataService->send( message->getSid(),
                    MEMCACHED_RESPONSE_ERROR, strlen(MEMCACHED_RESPONSE_ERROR) );
            return;
        }
    }

    std::string response;
    response += strvalue;
    response += "\r\n";
    m_DataService->send( message->getSid(), response );
}

void CDataServer::dump( CacheMessage * message )
{
    std::string response;

    if ( m_DumpThread != 0 && utils::IThread::check( m_DumpThread ) )
    {
        response += MEMCACHED_RESPONSE_SERVERERROR;
        response += " ";
        response += "the dump of the thread already exists";
        response += "\r\n";
        m_DataService->send( message->getSid(), response );
        return;
    }

    DumpThreadArgs * args = new DumpThreadArgs;
    args->server = this;
    args->sid = message->getSid();

    if ( pthread_create( &m_DumpThread, NULL, tinydb::dump_backend, args ) != 0 )
    {
        response += MEMCACHED_RESPONSE_SERVERERROR;
        response += " ";
        response += "create the dump of the thread failed";
        m_DataService->send( message->getSid(), response );
        delete args;
    }
}

Iterator* CDataServer::iterator( const std::string & start, const std::string & end, uint64_t limit ) const
{
    leveldb::Iterator *it;
    leveldb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = m_MainDB->getDatabase()->NewIterator(iterate_options);
    it->Seek(start);
    if( it->Valid() && it->key() == start )
    {
        it->Next();
    }

    return new Iterator( it, end, limit );
}

}
