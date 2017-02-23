
#include <cassert>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "base.h"
#include "config.h"

CDatadConfig::CDatadConfig()
    : m_LogLevel( 0 ),
      m_CacheSize( 0 )
{}

CDatadConfig::~CDatadConfig()
{}

bool CDatadConfig::load( const char * path )
{
    bool rc = false;

    utils::ConfigFile raw_file( path );
    rc = raw_file.open();
    assert( rc && "CDatadConfig::load() failed" );

    // Core
    raw_file.get( "Global", "loglevel", m_LogLevel );

    // Storage
    raw_file.get( "Storage", "location", m_StorageLocation );
    raw_file.get( "Storage", "cachesize", m_CacheSize );

    // Service
    raw_file.get( "Service", "bindhost", m_BindHost );
    raw_file.get( "Service", "listenport", m_ListenPort );
    raw_file.get( "Service", "timeoutseconds", m_TimeoutSeconds );


    // Replication
    raw_file.get( "Replication", "type", m_ReplicationConfig.type );
    raw_file.get( "Replication", "host", m_ReplicationConfig.endpoint.host );
    raw_file.get( "Replication", "port", m_ReplicationConfig.endpoint.port );
    raw_file.get( "Replication", "timeoutseconds", m_ReplicationConfig.timeoutseconds );
    raw_file.get( "Replication", "keepaliveseconds", m_ReplicationConfig.keepaliveseconds );

    LOG_INFO( "CDatadConfig::load('%s') succeed .\n", path );
    raw_file.close();

    return true;
}

bool CDatadConfig::reload( const char * path )
{
    // TODO:
    return true;
}

void CDatadConfig::unload()
{
    m_LogLevel = 0;
    m_StorageLocation.clear();
    m_CacheSize = 0;
    m_ReplicationConfig.clear();
}
