
#ifndef __SRC_TINYDB_CONFIG_H__
#define __SRC_TINYDB_CONFIG_H__

#include <stdint.h>

#include <vector>
#include <string>

#include "utils/file.h"
#include "utils/singleton.h"
#include "types.h"

struct ReplicationConfig
{
    int32_t         type;
    Endpoint        endpoint;
    int32_t         timeoutseconds;
    int32_t         keepaliveseconds;

    ReplicationConfig()
    {
        clear();
    }

    void clear()
    {
        type = 0;
        endpoint.clear();
        timeoutseconds = 0;
        keepaliveseconds = 0;
    }
};

class CDatadConfig : public Singleton<CDatadConfig>
{
public :
    CDatadConfig();
    ~CDatadConfig();

public :
    bool load( const char * path );
    bool reload( const char * path );
    void unload();

public :
    // 日志等级
    uint8_t getLogLevel() const { return m_LogLevel; }

    // 缓存大小
    size_t getCacheSize() const { return m_CacheSize; }
    const std::string & getStorageLocation() const { return m_StorageLocation; }

    uint16_t getListenPort() const { return m_ListenPort; }
    const char * getBindHost() const { return m_BindHost.c_str(); }
    int32_t getTimeoutSeconds() const { return m_TimeoutSeconds; }

    // 主从配置
    ReplicationConfig * getReplicationConfig() { return & m_ReplicationConfig; }

private :
    uint8_t                 m_LogLevel;
    size_t                  m_CacheSize;
    std::string             m_StorageLocation;
    std::string             m_BindHost;             // 绑定的主机地址
    uint16_t                m_ListenPort;
    int32_t                 m_TimeoutSeconds;
    ReplicationConfig       m_ReplicationConfig;    // 主从配置
};

#endif
