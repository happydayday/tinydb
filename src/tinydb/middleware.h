
#ifndef __SRC_TINYDB_MIDDLEWARE_H_
#define __SRC_TINYDB_MIDDLEWARE_H_

#include <stdint.h>

namespace tinydb
{

class IMiddlewareTask
{
public :
    IMiddlewareTask() {}
    virtual ~IMiddlewareTask() {}

    virtual void process() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// 备机连接就休
class CSlaveConnectTask : public IMiddlewareTask
{
public :
    CSlaveConnectTask(){}
    virtual ~CSlaveConnectTask() {}

    virtual void process();
};

// 备机断开连接
class CSlaveDisconnetTask : public IMiddlewareTask
{
public :
    CSlaveDisconnetTask()
        : m_Sid( 0LL )
    {}
    virtual ~CSlaveDisconnetTask() {}

    virtual void process();

public :
    void setSid( int64_t sid ) { m_Sid = sid; }

private :
    int64_t         m_Sid;
};

}

#endif
