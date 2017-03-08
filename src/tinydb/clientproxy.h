
#ifndef __SRC_TINYDB_CLIENTPROXY_H__
#define __SRC_TINYDB_CLIENTPROXY_H__

#include <pthread.h>
#include <deque>

#include "base.h"

#include "status.h"

namespace tinydb
{

class LevelDBEngine;
class CacheMessage;
class BinlogQueue;

class CClientProxy
{
public :
    CClientProxy( int32_t percision, LevelDBEngine * engine );
    virtual ~CClientProxy();

public :
    // 初始化/运行/销毁
    bool start();
    void run();
    void stop();

    // 提交请求
    void post( int32_t type, void * task );

public :
    const BinlogQueue * getBinlog() const { return m_Binlogs; }

private :
    // 处理逻辑
    void execute();

    // 消息处理
    void process( CacheMessage * message );

private :
    void add( CacheMessage * msg );
    void set( CacheMessage * msg );
    void del( CacheMessage * msg );
    void gets( CacheMessage * msg );
    void calc( CacheMessage * msg, int32_t value );

    void stat( CacheMessage * msg );
    void error( CacheMessage * msg );
    void version( CacheMessage * msg );

private :
    void dump( CacheMessage * msg );

private :
    struct Task
    {
        int32_t     type;
        void *      task;

        Task( int32_t type, void * task )
        {
            this->type = type;
            this->task = task;
        }
    };

    void doTask( const Task & t );

    pthread_mutex_t                         m_QueueLock;
    std::deque<Task>                        m_TaskQueue;

private :
    int32_t             m_Percision;
    LevelDBEngine *     m_Engine;
    BinlogQueue *       m_Binlogs;
    ServerStatus        m_ServerStatus;
    pthread_t           m_DumpThread;       // 存档线程
};

#define g_ClientProxy CDataServer::getInstance().getClientProxy()

}

#endif
