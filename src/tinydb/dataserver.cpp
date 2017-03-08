
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

#include "utils/utility.h"
#include "utils/timeutils.h"

#include "config.h"
#include "dataservice.h"
#include "masterservice.h"
#include "slaveclient.h"
#include "dumpbackend.h"
#include "dataserver.h"
#include "clientproxy.h"
#include "masterproxy.h"
#include "slaveproxy.h"
#include "syncbackend.h"

#include "leveldbengine.h"

namespace tinydb
{

CDataServer::CDataServer()
    : m_DataService( NULL ),
      m_MasterService( NULL ),
      m_SlaveClient( NULL ),
      m_ClientProxy( NULL ),
      m_MasterProxy( NULL ),
      m_SlaveProxy( NULL ),
      m_StorageEngine( NULL ),
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
    m_StorageEngine = new LevelDBEngine( main_db_path );
    if ( m_StorageEngine == NULL )
    {
        return false;
    }

    m_StorageEngine->setCacheSize( CDatadConfig::getInstance().getCacheSize() );
    if ( !m_StorageEngine->initialize() )
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

    // 客户端代理
    m_ClientProxy = new CClientProxy( eClientService_EachFrameSeconds, m_StorageEngine );
    if ( !m_ClientProxy->start() )
    {
        return false;
    }

    // 数据库同步
    m_BackendSync = new BackendSync();
    if ( m_BackendSync == NULL )
    {
        return false;
    }

    // 双机热备
    if ( !startReplicationService() )
    {
        return false;
    }

    return true;
}

void CDataServer::onExecute()
{
    g_ClientProxy->run();
}

void CDataServer::onStop()
{
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

    if ( m_ClientProxy != NULL )
    {
        m_ClientProxy->stop();
        delete m_ClientProxy;
        m_ClientProxy = NULL;
    }

    if ( m_MasterProxy != NULL )
    {
        m_MasterProxy->stop();
        delete m_MasterProxy;
        m_MasterProxy = NULL;
    }

    if ( m_SlaveProxy != NULL )
    {
        m_SlaveProxy->stop();
        delete m_SlaveProxy;
        m_SlaveProxy = NULL;
    }

    if ( m_BackendSync != NULL )
    {
        delete m_BackendSync;
        m_BackendSync = NULL;
    }

    if ( m_StorageEngine != NULL )
    {
        m_StorageEngine->finalize();
        delete m_StorageEngine;
        m_StorageEngine = NULL;
    }

    LOG_INFO( "CDataServer Stoped .\n" );
}

bool CDataServer::startReplicationService()
{
    ReplicationConfig * config = CDatadConfig::getInstance().getReplicationConfig();
    std::string meta_db_path = CDatadConfig::getInstance().getStorageLocation() + "/meta";

    // 主数据服
    if ( config->type == 0 )
    {
        // 代理
        m_MasterProxy = new CMasterProxy( eMasterService_EachFrameSeconds );
        if ( !m_MasterProxy->start() )
        {
            return false;
        }

        // 服务
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

    }
    else if ( config->type == 1 )
    {
        //代理
        m_SlaveProxy = new CSlaveProxy( eSlaveService_EachFrameSeconds, m_StorageEngine );
        if ( !m_SlaveProxy->start() )
        {
            return false;
        }

        // 服务
        m_SlaveClient = new CSlaveClient( config->keepaliveseconds, config->timeoutseconds );
        assert( m_SlaveClient != NULL && "create CSlaveClient failed" );

        // 连接主机
        if ( !m_SlaveClient->connect( config->endpoint.host.c_str(), config->endpoint.port, 10 ) )
        {
            LOG_FATAL( "CSlaveClient() connect 2 CMasterService(%s::%d) failed .\n",
                    config->endpoint.host.c_str(), config->endpoint.port );
            return false;
        }
    }

    return true;
}

}
