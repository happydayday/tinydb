
#ifndef __SRC_BASE_TYPES_H__
#define __SRC_BASE_TYPES_H__

#include <vector>
#include <string>

#include <stdio.h>
#include <stdint.h>

//==============================================================================
// 框架类型定义
//==============================================================================

#ifndef UINT64_MAX
    #define UINT64_MAX      18446744073709551615ULL
#endif
#ifndef INT64_MAX
    #define INT64_MAX       0x7fffffffffffffffLL
#endif

// 运行状态
enum RunStatus
{
	eRunStatus_Stop         = 0,	// 退出
	eRunStatus_Running      = 1,    // 正在运行
	eRunStatus_Reload       = 2,    // 重新加载
    eRunStatus_FlushLog     = 3,    // 刷新日志
};

struct Endpoint
{
    uint16_t    port;
    std::string host;

    Endpoint()
    {
        clear();
    }

    Endpoint( const std::string & host, uint16_t port )
    {
        this->host = host;
        this->port = port;
    }

    bool operator== ( const Endpoint & endpoint ) const
    {
        return endpoint.host == host && endpoint.port == port;
    }

    void clear()
    {
        port = 0;
        host.clear();
    }
};
typedef std::vector<Endpoint> Endpoints;

// 定义键值对
typedef std::pair<uint32_t, uint32_t>   Pair;

// 时间周期
typedef std::pair<time_t, time_t>       Period;

// 任务类型
enum
{
    eTaskType_Client        = 1,    // 来自客户端的任务
    eTaskType_DataSlave     = 2,    // 来自数据备库任务
    eTaskType_DataMaster    = 3,    // 来自数据主库任务
    eTaskType_Middleware    = 4,    // 中间件任务
};

// 数据类型
class DataType
{
public:
    static const char SYNCLOG   = 1;        // binlog数据
    static const char KV        = 'k';      // 真正数据
};

// 数据操作
class BinlogCommand
{
public:
    static const char NONE      = 0;
    static const char SET       = 1;
    static const char DEL       = 2;
    static const char BEGIN     = 7;
    static const char END       = 8;
};

// 操作类型
class BinlogType
{
public:
    static const char NOOP      = 0;
    static const char SYNC      = 1;
    static const char COPY      = 2;
};

// 备机状态
enum
{
    eSlaveState_Copy            = 1,        // 同步状态
    eSlaveState_Sync            = 2,        // 即时同步状态
};

#endif
