
#ifndef __SRC_TINYDB_MASTERSERVICE_H__
#define __SRC_TINYDB_MASTERSERVICE_H__

#include <string>

#include "io/io.h"

#include "message/protocol.h"

#include "dataserver.h"

namespace tinydb
{

class CMasterService;

class CSlaveSession : public IIOSession
{
public :
	CSlaveSession( CMasterService * s, const char * host, uint16_t port );
	virtual ~CSlaveSession();

public :
    virtual int32_t onStart();
	virtual int32_t onProcess( const char * buffer, uint32_t nbytes );
	virtual int32_t onTimeout();
	virtual int32_t onError( int32_t result );
	virtual void    onShutdown( int32_t way );

private :
    uint16_t            m_Port;
    std::string         m_Host;
    CMasterService *    m_MasterService;
};

class CMasterService : public IIOService
{
public :
	CMasterService( uint8_t nthreads, uint32_t nclients );
	virtual ~CMasterService();

	virtual IIOSession * onAccept( sid_t id, const char * host, uint16_t port );

public :
    // 广播
    int32_t broadcast( SSMessage * message );

    // 发送
    int32_t send( sid_t sid, SSMessage * message );

    // 获取/设置超时时间
    int32_t getTimeoutSeconds() const { return m_TimeoutSeconds; }
    void setTimeoutSeconds( int32_t seconds ) { m_TimeoutSeconds = seconds; }

private :
    uint8_t             m_ThreadsCount;
    int32_t             m_TimeoutSeconds;
};

}

#define g_MasterService         CDataServer::getInstance().getMasterService()

#endif
