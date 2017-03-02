
#include "base.h"
#include "types.h"

#include "utils/slice.h"
#include "utils/utility.h"

#include "leveldbengine.h"
#include "binlog.h"

#include "slaveclient.h"
#include "dataserver.h"

#include "protocol.h"

#include "slave.h"

namespace tinydb
{

Slave::Slave( LevelDBEngine * engine )
    : m_Engine( engine ),
      m_LastSeq( 0ULL ),
      m_CopyCount( 0ULL ),
      m_SyncCount( 0ULL )
{}

Slave::~Slave()
{}

void Slave::initialize()
{
    this->loadStatus();
}

void Slave::loadStatus(){
	std::string key = "new.slave.status";
	std::string val;

    bool rc = m_Engine->get( key, val );
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

void Slave::saveStatus()
{
	std::string key = "new.slave.status";
	std::string val;
	val.append( (char *)&m_LastSeq, sizeof(uint64_t) );
	val.append( m_LastKey );
    m_Engine->set( key, val );
}

void Slave::onConnect()
{
    SyncRequest msg;
    msg.lastseq = m_LastSeq;
    msg.lastkey = m_LastKey;

    g_SlaveClient->send( &msg );
}

int Slave::process( SSMessage * msg )
{
    switch ( msg->head.cmd )
    {
        case eSSCommand_SyncResponse :
            {
                SyncResponse * request = (SyncResponse *)msg;
                Binlog log;
                if ( log.load( request->binlog ) == -1 )
                {
                    return 0;
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
            return -1;
    }

    return 0;
}

int Slave::procNoop( const Binlog & log )
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

int Slave::procCopy( char method, const Binlog & log, const std::string & value )
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

int Slave::procSync( char method, const Binlog &log, const std::string & value )
{
	switch( log.cmd() )
    {
		case BinlogCommand::SET:
			{
				if( value.empty() )
                {
					break;
				}

                CDataServer::getInstance().getMainDB()->set( log.key().ToString(), value );
            }
			break;

        case BinlogCommand::DEL:
			{
                CDataServer::getInstance().getMainDB()->del( log.key().ToString() );
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
