
#include "base.h"

#include "utils/timeutils.h"
#include "utils/streambuf.h"

#include "robotclient.h"

namespace tinydb
{

CRobotClientSession::CRobotClientSession( CRobotClient * c )
    : m_Client( c )
{}

CRobotClientSession::~CRobotClientSession()
{}

int32_t CRobotClientSession::onStart()
{
    // 设置超时时间
    setTimeout( m_Client->getTimeoutSeconds() );
    // 设置保活时间
    setKeepalive( m_Client->getKeepaliveSeconds() );

    return 0;
}

int32_t CRobotClientSession::onProcess( const char * buffer, uint32_t nbytes )
{
    int32_t length = 0;

    for ( ;; )
    {
        int32_t nprocess = this->decode( buffer+length, nbytes-length );
        if ( nprocess == 0 )
        {
            break;
        }

        length += nprocess;
    }

    return length;
}

int32_t CRobotClientSession::onTimeout()
{
    LOG_ERROR( "CRobotClientSession::onTimeout(%llu) .\n", id() );
    return 0;
}

int32_t CRobotClientSession::onKeepalive()
{
    return 0;
}

int32_t CRobotClientSession::onError( int32_t result )
{
    LOG_ERROR( "CRobotClientSession::onError(%llu) : 0x%08x .\n", id(), result );
    return 0;
}

void CRobotClientSession::onShutdown( int32_t way )
{}

int32_t CRobotClientSession::decode( const char * buffer, uint32_t nbytes )
{
    int32_t length = 0;

    char * line = this->getline( buffer, nbytes, length );
    if ( line == NULL )
    {
        return 0;
    }

    printf( "%s\n", line );
    free( line );
    return length;
}

char * CRobotClientSession::getline( const char * buffer, uint32_t nbytes, int32_t & length )
{
    char * line = NULL;

    char * pos = (char *)memchr( (void*)buffer, '\n', nbytes );
    if ( pos == NULL )
    {
        return NULL;
    }

    length = pos - buffer + 1;

    line = (char *)malloc( length-1 );
    if ( line == NULL )
    {
        return NULL;
    }

    memcpy( line, buffer, length-2 );
    line[ length-2 ] = 0;

    return line;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

CRobotClient::CRobotClient( int32_t keepalive_seconds, int32_t timeout_seconds )
    : IIOService( eSlaveClient_ThreadsCount, eSlaveClient_ClientsCount ),
      m_ClientSid( 0ULL ),
      m_TimeoutSeconds( timeout_seconds ),
      m_KeepaliveSeconds( keepalive_seconds )
{
}

CRobotClient::~CRobotClient()
{
}

IIOSession * CRobotClient::onConnectSucceed( sid_t id, const char * host, uint16_t port )
{
    LOG_INFO("CRobotClient connect to MasterServer(%s::%d) succeed .\n", host, port);

    m_ClientSid = id;
    return new CRobotClientSession( this );
}

bool CRobotClient::onConnectFailed( int32_t result, const char * host, uint16_t port )
{
    LOG_ERROR( "CRobotClient connect to MasterServer(%s::%d) failed, Result: 0x%08x.\n", host, port, result );
    return true;
}

}
