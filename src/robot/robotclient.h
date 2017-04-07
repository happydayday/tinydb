
#ifndef __SRC_ROBOT_ROBOTCLIENT_H__
#define __SRC_ROBOT_ROBOTCLIENT_H__

#include "io/io.h"

namespace tinydb
{

class CRobotClient;
class CRobotClientSession : public IIOSession
{
public :
    CRobotClientSession( CRobotClient * c );
    virtual ~CRobotClientSession();

    int32_t onStart();
    int32_t onProcess( const char * buffer, uint32_t nbytes );
    int32_t onTimeout();
    int32_t onKeepalive();
    int32_t onError( int32_t result );
    void    onShutdown( int32_t way );

private :
    int32_t decode( const char * buffer, uint32_t nbytes );
    char * getline( const char * buffer, uint32_t nbytes, int32_t & length );

private :
    CRobotClient *   m_Client;
};

class CRobotClient : public IIOService
{
public :
    enum
    {
        eSlaveClient_ThreadsCount = 1,
        eSlaveClient_ClientsCount = 32,
    };

    CRobotClient( int32_t keepalive_seconds, int32_t timeout_seconds );
    virtual ~CRobotClient();

    //
    virtual IIOSession * onConnectSucceed( sid_t id, const char * host, uint16_t port );
    virtual bool onConnectFailed( int32_t result, const char * host, uint16_t port );

public :
    // 获取会话ID
    sid_t getSid() const { return m_ClientSid; }

    //
    int32_t getTimeoutSeconds() const { return m_TimeoutSeconds; }
    int32_t getKeepaliveSeconds() const { return m_KeepaliveSeconds; }

private :
    sid_t               m_ClientSid;
    int32_t             m_TimeoutSeconds;
    int32_t             m_KeepaliveSeconds;
};

#define g_RobotClient         CDataServer::getInstance().getSlaveClient()

}

#endif
