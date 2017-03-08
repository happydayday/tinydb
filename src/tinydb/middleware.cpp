
#include "syncbackend.h"
#include "slaveproxy.h"

#include "middleware.h"

namespace tinydb
{

void CSlaveConnectTask::process()
{
    g_SlaveProxy->onConnect();
}

void CSlaveDisconnetTask::process()
{
    // 通知主服，次服断开连接
    CDataServer::getInstance().getBackendSync()->shutdown( m_Sid );
}

}
