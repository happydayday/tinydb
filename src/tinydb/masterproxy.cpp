
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "middleware.h"
#include "message/protocol.h"

#include "syncbackend.h"
#include "masterservice.h"

#include "masterproxy.h"

namespace tinydb
{

CMasterProxy::CMasterProxy( int32_t percision )
    : m_Percision( percision ),
      m_CurTimeslice( 0LL )
{}

CMasterProxy::~CMasterProxy()
{}

bool CMasterProxy::onStart()
{
    // 获取当前时间片
    m_CurTimeslice = utils::TimeUtils::now();

    return true;
}

void CMasterProxy::onStop()
{
    this->cleanup();

    LOG_INFO( "CMasterProxy Stoped .\n" );
}

void CMasterProxy::onIdle()
{
    // 控制每帧的时间
    int32_t sleep_msecs = 0;
    int64_t now = utils::TimeUtils::now();
    int32_t used_msecs = now - m_CurTimeslice;
    if ( used_msecs >= 0 && used_msecs < m_Percision )
    {
        sleep_msecs = m_Percision-used_msecs;
        utils::TimeUtils::sleep( sleep_msecs );
    }

    // 获取当前时间片
    m_CurTimeslice = now + sleep_msecs;
}

void CMasterProxy::onTask( int32_t type, void * task )
{
    // 根据类型处理不同的请求
    switch( type )
    {
        case eTaskType_DataSlave :
            {
                SSMessage * t = static_cast<SSMessage *>( task );
                this->process( t );
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

void CMasterProxy::process( SSMessage * msg )
{

    switch ( msg->head.cmd )
    {
        case eSSCommand_SyncRequest :
            {
                if ( g_BackendSync == NULL )
                {
                    return;
                }

                SyncRequest * request = (SyncRequest *)msg;
                g_BackendSync->process( request->sid, request->lastseq, request->lastkey );
            }
            break;

        case eSSCommand_Ping :
            {
                PingCommand cmd;
                g_MasterService->send( msg->sid, &cmd );
            }
            break;

        default :
            break;
    }
}

}
