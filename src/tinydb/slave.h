
#ifndef __SRC_TINYDB_SLAVE_H_
#define __SRC_TINYDB_SLAVE_H_

#include <stdint.h>
#include <string>
#include <pthread.h>
#include <vector>

namespace tinydb
{

class CDataServer;
class LevelDBEngine;
class Binlog;
struct SSMessage;

class Slave
{
public:
	Slave( LevelDBEngine * engine );
	~Slave();

public:
    void onConnect();
    int process( SSMessage * msg );
    int procNoop( const Binlog & log );
	int procSync( char method, const Binlog & log, const std::string & value );
    int procCopy( char method, const Binlog & log, const std::string & value );

private :
	void loadStatus();
	void saveStatus();

private :
    LevelDBEngine *         m_Engine;           // 备库状态数据库
    uint64_t                m_LastSeq;
	std::string             m_LastKey;
	uint64_t                m_CopyCount;
	uint64_t                m_SyncCount;
};

}
#endif
