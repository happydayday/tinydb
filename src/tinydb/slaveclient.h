
#ifndef __SRC_TINYDB_SLAVECLIENT_H__
#define __SRC_TINYDB_SLAVECLIENT_H__

#include "io/io.h"

#include "protocol.h"

namespace tinydb
{

class CSlaveClient;
class CSlaveClientSession : public IIOSession
{
public :
    CSlaveClientSession( CSlaveClient * c );
    virtual ~CSlaveClientSession();

    int32_t onStart();
    int32_t onProcess( const char * buffer, uint32_t nbytes );
    int32_t onTimeout();
    int32_t onKeepalive();
    int32_t onError( int32_t result );
    void    onShutdown( int32_t way );

private :
    CSlaveClient *   m_SlaveClient;
};

class CSlaveClient : public IIOService
{
public :
    enum
    {
        eSlaveClient_ThreadsCount = 1,
        eSlaveClient_ClientsCount = 32,
    };

    CSlaveClient( int32_t keepalive_seconds, int32_t timeout_seconds );
    virtual ~CSlaveClient();

    //
    virtual IIOSession * onConnectSucceed( sid_t id, const char * host, uint16_t port );
    virtual bool onConnectFailed( int32_t result, const char * host, uint16_t port );

public :
    // 获取会话ID
    sid_t getSid() const { return m_SlaveClientSid; }

    // 发送
    bool send( SSMessage * message );

    //
    int32_t getTimeoutSeconds() const { return m_TimeoutSeconds; }
    int32_t getKeepaliveSeconds() const { return m_KeepaliveSeconds; }

private :
    sid_t               m_SlaveClientSid;
    int32_t             m_TimeoutSeconds;
    int32_t             m_KeepaliveSeconds;
};

#define g_SlaveClient         CDataServer::getInstance().getSlaveClient()

}

#endif
