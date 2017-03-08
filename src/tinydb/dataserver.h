
#ifndef __SRC_TINYDB_TINYDB_H__
#define __SRC_TINYDB_TINYDB_H__

#include <pthread.h>

#include "base.h"

#include "utils/thread.h"
#include "utils/singleton.h"

namespace tinydb
{

class CDataService;
class CMasterService;
class CSlaveClient;

class CClientProxy;
class CMasterProxy;
class CSlaveProxy;

class LevelDBEngine;
class BackendSync;

class CDataServer : public utils::IThread, public Singleton<CDataServer>
{
public :
    enum
    {
        eDataService_ThreadsCount   = 1,            // DataService的线程个数
        eDataService_SessionsCount  = 1000,         // DataService最大会话个数
    };

    enum
    {
        eClientService_EachFrameSeconds = 20,   // 客户端服务器每帧20ms
        eMasterService_EachFrameSeconds = 12,   // 主服务器每帧12ms
        eSlaveService_EachFrameSeconds  = 20,   // 次服务器每帧20ms
    };

public :
    virtual bool onStart();
    virtual void onExecute();
    virtual void onStop();

public :
    CDataServer();
    virtual ~CDataServer();

public :
    // 获取数据服务
    CDataService * getService() const { return m_DataService; }
    CMasterService * getMasterService() const { return m_MasterService; }
    CSlaveClient * getSlaveClient() const { return m_SlaveClient; }

    // 获取服务器代理
    CClientProxy * getClientProxy() const { return m_ClientProxy; }
    CMasterProxy * getMasterProxy() const { return m_MasterProxy; }
    CSlaveProxy * getSlaveProxy() const { return m_SlaveProxy; }

    // 获取存档服务
    LevelDBEngine * getStorageEngine() const { return m_StorageEngine; }

    // 获取主库同步对象
    BackendSync * getBackendSync() const { return m_BackendSync; }

private :
    // 启动主从备份
    bool startReplicationService();

private :
    CDataService *              m_DataService;
    CMasterService *            m_MasterService;
    CSlaveClient *              m_SlaveClient;

    CClientProxy *              m_ClientProxy;
    CMasterProxy *              m_MasterProxy;
    CSlaveProxy *               m_SlaveProxy;

    LevelDBEngine *             m_StorageEngine;

    BackendSync *               m_BackendSync;      // 数据同步
};

}

#endif
