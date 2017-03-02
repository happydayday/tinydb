
#ifndef __SRC_UTILS_THREAD_H__
#define __SRC_UTILS_THREAD_H__

#include <deque>

#include <stdint.h>
#include <pthread.h>

namespace utils
{

//
// 基本线程
//
class IThread
{

public :
    IThread();
    virtual ~IThread();

    virtual bool onStart() = 0;
    virtual void onExecute() = 0;
    virtual void onStop() = 0;

public :
    bool start();
    void stop();

    static bool check( pthread_t id );

    pthread_t id() const { return m_ThreadID; }
    bool isRunning() const { return m_Status == eRunning; }

    void setDetach() { m_IsDetach = true; }
    void setStackSize( uint32_t size ) { m_StackSize = size; }

protected :
    // onExcute()中停止线程
    void stoping() { m_Status = eStoping; }

private :
    enum
    {
        eReady      = 0,
        eRunning    = 1,
        eStoping    = 2,
        eStoped     = 3,
    };

    void notify();
    static void * threadfunc( void * arg );

private :
    bool            m_IsDetach;
    uint32_t        m_StackSize;

    int8_t          m_Status;
    pthread_t       m_ThreadID;

    pthread_cond_t  m_CtrlCond;
    pthread_mutex_t m_CtrlLock;
};

//
// 工作线程
// 时序图:
// void loop()
// {
//      while( N )
//          this->onTask();
//      this->onIdle();
//  }
//
class IWorkThread : public IThread
{
public :
    IWorkThread();
    virtual ~IWorkThread();

public :
    // 开启的回调
    virtual bool onStart() = 0;

    // 空闲回调
    virtual void onIdle() = 0;

    // 任务回调
    virtual void onTask( int32_t type, void * task ) = 0;

    // 停止的回调
    virtual void onStop() = 0;

public :
    // 提交任务
    bool post( int32_t type, void * task );

    // 清理队列
    void cleanup();

    // 设置每帧处理的任务个数
    void setPeakCount( uint32_t count ) { m_PeakCount = count; }

private :
    // 处理业务
    void onExecute();

private :
    // 队列
    struct Task
    {
        int32_t     type;
        void *      task;
    };

    uint32_t                m_PeakCount;
    pthread_mutex_t         m_Lock;
    std::deque<Task>        m_TaskQueue;
};

#if 0

//
// DB线程
//

class IDBRequest
{
public :
    IDBRequest() {}
    virtual ~IDBRequest() {}

public :
    // 序列化sql
    virtual bool serialize( std::string & sqlcmd ) = 0;

    // 是否需要结果
    virtual bool isResult() = 0;

    // 返回结果
    virtual void onResult( MYSQL_RES * result ) = 0;

    // 销毁请求
    virtual void destroy() = 0;
};

class DBThread : public Thread
{
public :
    DBThread::DBThread(
            const char * host, uint16_t port,
            const char * username, const char * password,
            const char * encodings, const char * dbname );

    virtual ~DBThread();

public :
    bool onStart();
    void onExecute();
    void onStop();

public :
    // 查询数据库
    bool query( IDBRequest * request );

private :
    enum
    {
        eMaxFetchTaskCount         = 32,        // 同时从队列中取出多少个任务
        eMaxWaitForTaskInterval    = 10,        // 最大等待任务的时间间隔(单位是毫秒)
    };

    // 查询
    void doQuery( IDBRequest * request );

private :
    pthread_mutex_t                m_Lock;
    std::deque<IDBRequest *>    m_TaskQueue;

    std::string                    m_Host;
    uint16_t                    m_Port;
    std::string                    m_Username;
    std::string                    m_Password;
    std::string                    m_Encoding;
    std::string                    m_DBname;

    MYSQL *                        m_DBHandler;
};

#endif

class Mutex
{
public:
    Mutex()
    {
        pthread_mutex_init( &mutex, NULL );
    }
    ~Mutex()
    {
        pthread_mutex_destroy( &mutex );
    }
    void lock()
    {
        pthread_mutex_lock( &mutex );
    }
    void unlock()
    {
        pthread_mutex_unlock( &mutex );
    }

private:
    pthread_mutex_t mutex;
};

}


#endif
