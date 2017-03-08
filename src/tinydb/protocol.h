
#ifndef __SRC_TINYDB_PROTOCOL_H__
#define __SRC_TINYDB_PROTOCOL_H__

#include <stdint.h>

#include "utils/slice.h"

#include "message.h"

class CacheMessage;

namespace tinydb
{
class CacheProtocol
{
public :
    CacheProtocol();
    ~CacheProtocol();

public :
    void init();
    void clear();

    // 获取解析得到的消息
    CacheMessage * getMessage() const;

    // 解析消息
    int32_t decode( const char * buffer, uint32_t nbytes );

private :
    char * getline( const char * buffer, uint32_t nbytes, int32_t & length );

private :
    CacheMessage *        m_Message;

};

// 服务器间通信

// 命令
enum
{
    eSSCommand_Invalid      = 0x0000,   // 非法消息

    eSSCommand_Ping         = 0x0001,   // PING包

    eSSCommand_SyncRequest  = 0x0101,   // 同步请求
    eSSCommand_SyncResponse = 0x0102,   // 同步回应
};

// 消息基类
struct SSMessage
{
public :
    SSMessage();
    virtual ~SSMessage();

    virtual Slice encode() = 0;
    // 反序列化包体
    virtual bool decode( const Slice & data ) = 0;

    // 清空
    void clear();

    // 扩展space
    bool make( const char * buffer, uint32_t len );

public :
    sid_t       sid;        // 会话ID
    SSHead      head;       // 消息头
    char *      space;      // 缓存
    uint32_t    length;     // 缓存的长度
};

// 通用解析
SSMessage * GeneralDecoder( sid_t sid, const SSHead & head, const Slice & body );

// PING包
struct PingCommand : SSMessage
{
public :
    PingCommand();
    virtual ~PingCommand();

    virtual Slice encode();
    virtual bool decode( const Slice & data );
};

// 请求同步
struct SyncRequest : SSMessage
{
public :
    SyncRequest();
    virtual ~SyncRequest();

    virtual Slice encode();
    virtual bool decode( const Slice & data );

public :
    uint64_t        lastseq;
    std::string     lastkey;
};

// 同步回应
struct SyncResponse : SSMessage
{
public :
    SyncResponse();
    virtual ~SyncResponse();

    virtual Slice encode();
    virtual bool decode( const Slice & data );

public :
    uint8_t         method;
    std::string     binlog;
    std::string     value;
};

}
#endif
