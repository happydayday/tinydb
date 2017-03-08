
#include "base.h"
#include "types.h"

#include "utils/slice.h"
#include "utils/utility.h"
#include "utils/timeutils.h"

#include "leveldbengine.h"
#include "config.h"
#include "binlog.h"

#include "slaveclient.h"
#include "protocol.h"
#include "middleware.h"

#include "slaveproxy.h"

namespace tinydb
{

CSlaveProxy::CSlaveProxy( int32_t percision, LevelDBEngine * engine )
    : m_Percision( percision  ),
      m_CurTimeslice( 0LL ),
      m_StorageEngine( engine ),
      m_MetaEngine( NULL ),
      m_LastSeq( 0ULL ),
      m_CopyCount( 0ULL ),
      m_SyncCount( 0ULL )
{}

CSlaveProxy::~CSlaveProxy()
{}

bool CSlaveProxy::onStart()
{
    // 初始化状态数据库
    std::string meta_db_path = CDatadConfig::getInstance().getStorageLocation() + "/meta";

    m_MetaEngine = new LevelDBEngine( meta_db_path );
    if ( m_MetaEngine == NULL )
    {
        return false;
    }

    m_MetaEngine->setCacheSize( CDatadConfig::getInstance().getCacheSize() );

    if ( !m_MetaEngine->initialize() )
    {
        return false;
    }

    // 加载备库状态
    this->loadStatus();

    // 获取当前时间片
    m_CurTimeslice = utils::TimeUtils::now();

    return true;
}

void CSlaveProxy::onStop()
{
    this->cleanup();

    if ( m_MetaEngine != NULL )
    {
        delete m_MetaEngine;
        m_MetaEngine = NULL;
    }

    LOG_INFO( "CSlaveProxy Stoped .\n" );
}

void CSlaveProxy::onIdle()
{
    // 控制每帧的时间
    int32_t sleep_msecs = 0;
    int64_t now = utils::TimeUtils::now();
    int32_t used_msecs = now - m_CurTimeslice;
    if ( used_msecs >= 0 && used_msecs < m_Percision )
    {
        sleep_msecs = m_Percision-used_msecs;
        utils::TimeUtils::sleep( sleep_msecs );
    }

    // 获取当前时间片
    m_CurTimeslice = now + sleep_msecs;
}

void CSlaveProxy::onTask( int32_t type, void * task )
{
    // 根据类型处理不同的请求
    switch( type )
    {
        case eTaskType_DataMaster :
            {
                SSMessage * t = static_cast<SSMessage *>( task );
                this->process( t );
                delete t;
            }
            break;

        case eTaskType_Middleware :
            {
                IMiddlewareTask * t = static_cast<IMiddlewareTask *>(task);
                t->process();
                delete t;
            }
            break;

        default :
            break;
    }
}

void CSlaveProxy::loadStatus()
{
	std::string key = "new.slave.status";
	std::string val;

    bool rc = m_MetaEngine->get( key, val );
    if ( rc )
    {
		if( val.size() < sizeof(uint64_t) )
        {
			LOG_ERROR( "invalid format of status.\n" );
		}
        else
        {
			m_LastSeq = *( (uint64_t *)( val.data() ) );
			m_LastKey.assign( val.data() + sizeof(uint64_t), val.size() - sizeof(uint64_t) );
		}

    }
}

void CSlaveProxy::saveStatus()
{
	std::string key = "new.slave.status";
	std::string val;
	val.append( (char *)&m_LastSeq, sizeof(uint64_t) );
	val.append( m_LastKey );
    m_MetaEngine->set( key, val );
}

void CSlaveProxy::onConnect()
{
    SyncRequest msg;
    msg.lastseq = m_LastSeq;
    msg.lastkey = m_LastKey;

    g_SlaveClient->send( &msg );
}

void CSlaveProxy::process( SSMessage * msg )
{
    switch ( msg->head.cmd )
    {
        case eSSCommand_SyncResponse :
            {
                SyncResponse * request = (SyncResponse *)msg;
                Binlog log;
                if ( log.load( request->binlog ) == -1 )
                {
                    return;
                }

                switch ( request->method )
                {
                    case BinlogType::NOOP :
                        {
                            this->procNoop( log );
                        }
                        break;

                    case BinlogType::COPY :
                        {
                            this->procCopy( request->method, log, request->value );
                        }
                        break;

                    case BinlogType::SYNC :
                        {
                            this->procSync( request->method, log, request->value );
                        }
                        break;
                }
            }
            break;

        default :
            return;
    }

    return;
}

int CSlaveProxy::procNoop( const Binlog & log )
{
    uint64_t seq = log.seq();
    if( this->m_LastSeq != seq )
    {
        LOG_DEBUG( "noop lastseq: %llu, seq: %llu", this->m_LastSeq, seq );
        this->m_LastSeq = seq;
        this->saveStatus();
    }

    return 0;
}

int CSlaveProxy::procCopy( char method, const Binlog & log, const std::string & value )
{
    switch ( log.cmd() )
    {
        case BinlogCommand::BEGIN :
            {
            }
            break;

        case BinlogCommand::END :
            {
                m_LastKey = "";
                this->saveStatus();
            }
            break;

        default :
            {
                this->procSync( method, log, value );
            }
            break;
    }

    return 0;
}

int CSlaveProxy::procSync( char method, const Binlog &log, const std::string & value )
{
	switch( log.cmd() )
    {
		case BinlogCommand::SET:
			{
				if( value.empty() )
                {
					break;
				}

                m_StorageEngine->set( log.key().ToString(), value );
            }
			break;

        case BinlogCommand::DEL:
			{
                m_StorageEngine->del( log.key().ToString() );
			}
			break;

        default:
			LOG_ERROR( "unknown binlog, cmd=%d.\n", log.cmd() );
			break;
	}

    LOG_DEBUG( "Slave::procSync cmd=%c, seq=%llu, key=%s.\n", log.cmd(), log.seq(), log.key().ToString().c_str() );
    m_LastSeq = log.seq();
	if( method == BinlogType::COPY )
    {
		this->m_LastKey = log.key().ToString();
	}

    this->saveStatus();

    return 0;
}

}
