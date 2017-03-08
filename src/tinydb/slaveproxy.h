
#ifndef __SRC_TINYDB_SLAVEPROXY_H_
#define __SRC_TINYDB_SLAVEPROXY_H_

#include <stdint.h>
#include <string>
#include <pthread.h>
#include <vector>

#include "utils/thread.h"

#include "dataserver.h"

namespace tinydb
{

class CDataServer;
class LevelDBEngine;
class Binlog;
struct SSMessage;

class CSlaveProxy : public utils::IWorkThread
{
public:
	CSlaveProxy( int32_t percision, LevelDBEngine * engine );
	virtual ~CSlaveProxy();

public :
    // 自定义的开启/停止命令
    virtual bool onStart();
    virtual void onStop();

    // 自定义的空闲/任务命令
    virtual void onIdle();
    virtual void onTask( int32_t type, void * task );

public:
    // 备机连接主机成功
    void onConnect();

private :
    // 消息处理
    void process( SSMessage * msg );

    // 数据处理
    int procNoop( const Binlog & log );
	int procSync( char method, const Binlog & log, const std::string & value );
    int procCopy( char method, const Binlog & log, const std::string & value );

    // 加载/保存同步状态
    void loadStatus();
	void saveStatus();

private :
    int32_t                 m_Percision;
    int64_t                 m_CurTimeslice;

private :
    LevelDBEngine *         m_StorageEngine;        // 主库数据库
    LevelDBEngine *         m_MetaEngine;           // 备库状态数据库
    uint64_t                m_LastSeq;
	std::string             m_LastKey;
	uint64_t                m_CopyCount;
	uint64_t                m_SyncCount;
};

#define g_SlaveProxy    CDataServer::getInstance().getSlaveProxy()

}
#endif
