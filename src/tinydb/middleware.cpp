
#include "dataserver.h"
#include "slave.h"
#include "syncbackend.h"

#include "middleware.h"

namespace tinydb
{

void CSlaveConnectTask::process()
{
    Slave * slave = CDataServer::getInstance().getSlave();
    if ( slave  == NULL )
    {
        return;
    }

    slave->onConnect();
}

void CSlaveDisconnetTask::process()
{
    BackendSync * sync = CDataServer::getInstance().getBackendSync();
    if ( sync == NULL )
    {
        return;
    }

    sync->shutdown( m_Sid );
}

}
