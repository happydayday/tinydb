
#ifndef __SRC_TINYDB_TINYDB_H__
#define __SRC_TINYDB_TINYDB_H__

#include <pthread.h>

#include "base.h"

#include "utils/thread.h"
#include "utils/singleton.h"

#include "status.h"

namespace tinydb
{

class CDataService;
class CMasterService;
class CSlaveClient;
class CacheMessage;
class LevelDBEngine;
class BinlogQueue;
class Slave;
class BackendSync;
class Iterator;
struct SSMessage;


class CDataServer : public utils::IWorkThread, public Singleton<CDataServer>
{
public :
    enum
    {
        eDataService_ThreadsCount   = 1,            // DataService的线程个数
        eDataService_SessionsCount  = 1000,         // DataService最大会话个数
    };

public :
    CDataServer();
    virtual ~CDataServer();

public :
    // 自定义的开启/停止命令
    virtual bool onStart();
    virtual void onStop();

    // 自定义的空闲/任务命令
    virtual void onIdle() {}
    virtual void onTask( int32_t type, void * task );

public :
    // 获取数据服务
    CDataService * getService() const { return m_DataService; }
    CMasterService * getMasterService() const { return m_MasterService; }
    CSlaveClient * getSlaveClient() const { return m_SlaveClient; }

    // 获取存档服务
    LevelDBEngine * getMainDB() const { return m_MainDB; }
    LevelDBEngine * getMetaDB() const { return m_MetaDB; }

    // 获取binlog
    BinlogQueue * getBinlog() const { return m_Binlogs; }

    // 获取备机处理对象
    Slave * getSlave() const { return m_Slave; }

    // 获取主库同步对象
    BackendSync * getBackendSync() const { return m_BackendSync; }

public :
    Iterator* iterator( const std::string & start, const std::string & end, uint64_t limit ) const;

private :
    // 启动主从备份
    bool startReplicationService();
    void processClient( CacheMessage * msg );
    void processSlave( SSMessage * msg );

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
    CDataService *              m_DataService;
    CMasterService *            m_MasterService;
    CSlaveClient *              m_SlaveClient;

    pthread_t                   m_DumpThread;       // 存档线程
    ServerStatus                m_ServerStatus;
    LevelDBEngine *             m_MainDB;
    LevelDBEngine *             m_MetaDB;

private :
    BinlogQueue *               m_Binlogs;
    Slave *                     m_Slave;
    BackendSync *               m_BackendSync;
};

}

#endif
