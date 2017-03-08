
#ifndef __SRC_TINYDB_MASTERPROXY_H__
#define __SRC_TINYDB_MASTERPROXY_H__

#include <pthread.h>

#include "base.h"

#include "utils/thread.h"

#include "dataserver.h"

#include "status.h"

namespace tinydb
{

struct SSMessage;

class CMasterProxy : public utils::IWorkThread
{
public :
    CMasterProxy( int32_t percision );
    virtual ~CMasterProxy();

public :
    // 自定义的开启/停止命令
    virtual bool onStart();
    virtual void onStop();

    // 自定义的空闲/任务命令
    virtual void onIdle();
    virtual void onTask( int32_t type, void * task );

private :
    void process( SSMessage * msg );

private :
    int32_t         m_Percision;
    int64_t         m_CurTimeslice;
};

#define g_MasterProxy CDataServer::getInstance().getMasterProxy()

}

#endif
