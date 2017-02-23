
#include "base.h"

#include "utils/timeutils.h"
#include "utils/streambuf.h"

#include "message.h"
#include "protocol.h"
#include "dataserver.h"
#include "middleware.h"

#include "slaveclient.h"

namespace tinydb
{

CSlaveClientSession::CSlaveClientSession( CSlaveClient * c )
    : m_SlaveClient( c )
{}

CSlaveClientSession::~CSlaveClientSession()
{}

int32_t CSlaveClientSession::onStart()
{
    // 设置超时时间
    setTimeout( m_SlaveClient->getTimeoutSeconds() );
    // 设置保活时间
    setKeepalive( m_SlaveClient->getKeepaliveSeconds() );

    CSlaveConnectTask * task = new CSlaveConnectTask();
    bool result = CDataServer::getInstance().post(
            eTaskType_Middleware, static_cast<void *>(task) );

    if ( !result )
    {
        delete task;
    }

    return 0;
}

int32_t CSlaveClientSession::onProcess( const char * buffer, uint32_t nbytes )
{
    int32_t nprocess = 0;

    for ( ;; )
    {
        uint32_t nleft = nbytes - nprocess;
        const char * buf = buffer + nprocess;

        // 数据包太小
        if ( nleft < sizeof(SSHead) )
        {
            break;
        }

        SSHead head;
        StreamBuf unpack( buf, nleft );
        unpack.decode( head.cmd );
        unpack.decode( head.size );

        // 合法的数据包
        head.size += sizeof(SSHead);
        if ( nleft < head.size )
        {
            break;
        }

        // 解析数据
        SSMessage * msg = GeneralDecoder(
                id(), head, Slice( buf+sizeof(SSHead), head.size ) );
        if ( msg != NULL )
        {
            CDataServer::getInstance().post( eTaskType_DataMaster, static_cast<void *>(msg) );
        }

        nprocess += head.size;
    }

    return nprocess;
}

int32_t CSlaveClientSession::onTimeout()
{
    LOG_ERROR( "CSlaveClientSession::onTimeout(%llu) .\n", id() );
    return 0;
}

int32_t CSlaveClientSession::onKeepalive()
{
    // 发送心跳包
    return 0;
}

int32_t CSlaveClientSession::onError( int32_t result )
{
    LOG_ERROR( "CSlaveClientSession::onError(%llu) : 0x%08x .\n", id(), result );
    return 0;
}

void CSlaveClientSession::onShutdown( int32_t way )
{}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

CSlaveClient::CSlaveClient( int32_t keepalive_seconds, int32_t timeout_seconds )
    : IIOService( eSlaveClient_ThreadsCount, eSlaveClient_ClientsCount ),
      m_SlaveClientSid( 0ULL ),
      m_TimeoutSeconds( timeout_seconds ),
      m_KeepaliveSeconds( keepalive_seconds )
{
}

CSlaveClient::~CSlaveClient()
{
}

IIOSession * CSlaveClient::onConnectSucceed( sid_t id, const char * host, uint16_t port )
{
    LOG_INFO("CSlaveClient connect to MasterServer(%s::%d) succeed .\n", host, port);

    m_SlaveClientSid = id;
    return new CSlaveClientSession( this );
}

bool CSlaveClient::onConnectFailed( int32_t result, const char * host, uint16_t port )
{
    LOG_ERROR( "CSlaveClient connect to MasterServer(%s::%d) failed, Result: 0x%08x.\n", host, port, result );
    return true;
}

bool CSlaveClient::send( SSMessage * message )
{
    int32_t result = -1;

    // 序列化
    Slice buf = message->encode();

    // 发送
    result = IIOService::send(
            m_SlaveClientSid, buf.data(), buf.size(), true );
    if ( result == 0 )
    {
        message->clear();
    }

    return result;
}

}
