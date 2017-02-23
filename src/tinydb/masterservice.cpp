
#include "base.h"
#include "utils/streambuf.h"

#include "message.h"
#include "protocol.h"
#include "middleware.h"

#include "masterservice.h"

namespace tinydb
{

CSlaveSession::CSlaveSession( CMasterService * s, const char * host, uint16_t port )
    : m_Port( port ),
      m_Host( host ),
      m_MasterService( s )
{}

CSlaveSession::~CSlaveSession()
{}

int32_t CSlaveSession::onStart()
{
    // 设置超时时间
    setTimeout( m_MasterService->getTimeoutSeconds() );
    return 0;
}

int32_t CSlaveSession::onProcess( const char * buffer, uint32_t nbytes )
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
            CDataServer::getInstance().post( eTaskType_DataSlave, static_cast<void *>(msg) );
        }

        nprocess += head.size;
    }

	return nprocess;
}

int32_t CSlaveSession::onTimeout()
{
    LOG_WARN( "CSlaveSession::onTimeout(%llu, %s::%d) .\n",
            id(), m_Host.c_str(), m_Port );
	return -1;
}

int32_t CSlaveSession::onError( int32_t result )
{
    LOG_WARN( "CSlaveSession::onError(%llu, %s::%d) : 0x%08x .\n",
            id(), m_Host.c_str(), m_Port, result );
	return -1;
}

void CSlaveSession::onShutdown( int32_t way )
{
    CSlaveDisconnetTask * task = new CSlaveDisconnetTask();
    bool result = CDataServer::getInstance().post(
            eTaskType_Middleware, static_cast<void *>(task) );

    if ( !result )
    {
        delete task;
    }
}

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------

CMasterService::CMasterService( uint8_t nthreads, uint32_t nclients )
	: IIOService( nthreads, nclients ),
      m_ThreadsCount( nthreads ),
      m_TimeoutSeconds( 0 )
{
}

CMasterService::~CMasterService()
{
}

void * CMasterService::getLocalData( uint8_t index )
{
    assert( index < m_ThreadsCount && "iothread index invalid" );
    return NULL;
}

IIOSession * CMasterService::onAccept( sid_t id, const char * host, uint16_t port )
{
    return new CSlaveSession( this, host, port );
}

int32_t CMasterService::send( sid_t sid, SSMessage * message )
{
    int32_t result = -1;

    // 序列化
    Slice buf = message->encode();

    // 发送
    result = IIOService::send( sid, buf.data(), buf.size(), true );
    if ( result == 0 )
    {
        message->clear();
    }

    return result;
}

}
