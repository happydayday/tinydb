
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "utils/streambuf.h"

#include "message.h"
#include "protocol.h"

#include "base.h"
#include "version.h"

namespace tinydb
{

CacheProtocol::CacheProtocol()
{}

CacheProtocol::~CacheProtocol()
{}

void CacheProtocol::init()
{
    m_Message = NULL;
}

void CacheProtocol::clear()
{
    m_Message = NULL;
}

CacheMessage * CacheProtocol::getMessage() const
{
    return m_Message;
}

int32_t CacheProtocol::decode( const char * buffer, uint32_t nbytes )
{
    int32_t length = 0;

    if ( m_Message == NULL )
    {
        char * line = getline( buffer, nbytes, length );
        if ( line == NULL )
        {
            return 0;
        }

        char * params = line;
        char * cmd = strsep( &params, " " );

        if ( cmd == NULL )
        {
            return 0;
        }

        m_Message = new CacheMessage;
        m_Message->setCmd( cmd );

        if ( strcasecmp( cmd, "add" ) == 0 || strcasecmp( cmd, "set" ) == 0
                || strcasecmp( cmd, "replace" ) == 0 || strcasecmp( cmd, "cas" ) == 0
                || strcasecmp( cmd, "append" ) == 0 || strcasecmp( cmd, "prepend" ) == 0 )
        {
            // 协议定义
            // [key] [flags] [expire] [bytes] <casunique>\r\n
            // fields说明如下:
            //      0 - key
            //      1 - flags
            //      2 - expire
            //      3 - value size
            //      4 - cas(可选)

            int32_t nfields = 0;
            char * fields[ 5 ] = { 0 };

            const char * sep = " ";
            char * word = NULL, * brkt = NULL;
            for ( word = strtok_r(params, sep, &brkt);
                    word;
                    word = strtok_r(NULL, sep, &brkt) )
            {
                fields[nfields++] = word;
            }

            if ( nfields >= 4
                    && fields[0] != NULL && fields[3] != NULL )
            {
                // key datasize 合法
                m_Message->fetchItem()->setKey( fields[0] );
                m_Message->fetchItem()->setValueCapacity( atoi(fields[3]) + 2 );     // DataChunk\r\n

                if ( fields[ 1 ] != NULL )
                {
                    LOG_WARN( "CacheProtocol::decode(CMD:'%s', KEY:'%s') : the datad not support the Flags feature .\n", cmd, fields[0] );
                }
                if ( fields[ 2 ] != NULL )
                {
                    LOG_WARN( "CacheProtocol::decode(CMD:'%s', KEY:'%s') : this datad-%s not support the ExpireTime feature .\n", cmd, fields[0], __APPVERSION__ );
                }
                if ( nfields == 5 && fields[4] != NULL )
                {
                    LOG_WARN( "CacheProtocol::decode(CMD:'%s', KEY:'%s') : this datad-%s not support the CasUnique feature .\n", cmd, fields[0], __APPVERSION__ );
                }

                // TODO: cas
            }
            else
            {
                m_Message->setError( "CLIENT_ERROR bad command line format" );
            }
        }
        else if ( strcasecmp( cmd, "get" ) == 0 || strcasecmp( cmd, "gets" ) == 0 )
        {
            // [cmd] [key1] [key2] [key3] ... [keyn]

            const char * sep = " ";
            char * word = NULL, *brkt = NULL;
            for ( word = strtok_r(params, sep, &brkt);
                    word;
                    word = strtok_r(NULL, sep, &brkt) )
            {
                m_Message->addKey( word );
            }
        }
        else if ( strcasecmp( cmd, "incr" ) == 0 || strcasecmp( cmd, "decr" ) == 0 )
        {
            char key[ 256 ] = { 0 };
            char bytes[ 16 ] = { 0 };

            int32_t rc = sscanf( params, "%250s %15s", key, bytes );
            if ( rc == 2 && key[0] != '\0' )
            {
                m_Message->fetchItem()->setKey( key );
                m_Message->setDelta( (uint32_t)atoi(bytes) );
            }
            else
            {
                m_Message->setError("CLIENT_ERROR bad command line format");
            }
        }
        else if ( strcasecmp( cmd, "delete" ) == 0 )
        {
            char key[ 256 ] = { 0 };
            char expire[ 16 ] = { 0 };

            sscanf( params, "%250s %15s", key, expire );
            if ( key[0] != '\0' )
            {
                m_Message->fetchItem()->setKey( key );
            }
            else
            {
                m_Message->setError("CLIENT_ERROR bad command line format");
            }
        }

        free ( line );
    }

    if ( m_Message != NULL && m_Message->getError() == NULL && nbytes > (uint32_t)length )
    {
        char * buf = (char *)buffer + length;
        size_t nleft = nbytes - length;
        size_t bytes = m_Message->fetchItem()->getValueCapacity() - m_Message->fetchItem()->getValueSize();

        if ( bytes > 0 )
        {
            bytes = bytes > nleft ? nleft : bytes;
            m_Message->fetchItem()->appendValue( std::string(buf, bytes) );

            length += bytes;

            if ( m_Message->isComplete() )
            {
                // 检查Value换行符
                if ( !m_Message->checkDataChunk() )
                {
                    m_Message->setError("CLIENT_ERROR bad data chunk");
                }
            }
        }
    }

    return length;
}


char * CacheProtocol::getline( const char * buffer, uint32_t nbytes, int32_t & length )
{
    char * line = NULL;

    char * pos = (char *)memchr( (void*)buffer, '\n', nbytes );
    if ( pos == NULL )
    {
        return NULL;
    }

    length = pos - buffer + 1;

    line = (char *)malloc( length-1 );
    if ( line == NULL )
    {
        return NULL;
    }

    memcpy( line, buffer, length-2 );
    line[ length-2 ] = 0;

    return line;
}

/////////////////////////////////////////////////////////////////////////////////

SSMessage * GeneralDecoder( sid_t sid, const SSHead & head, const Slice & body )
{
    SSMessage * msg = NULL;

    switch ( head.cmd )
    {
        case eSSCommand_SyncRequest :
            msg = new SyncRequest();
            break;

        case eSSCommand_SyncResponse :
            msg = new SyncResponse();
            break;
    }

    if ( msg == NULL )
    {
        return NULL;
    }

    msg->sid = sid;
    // 复制head
    msg->head = head;

    // 解析包体
    if ( !msg->decode( body ) )
    {
        delete msg;
        return NULL;
    }

    return msg;
}

///////////////////////////////////////////////////////////////////////////////////

SSMessage::SSMessage()
    : sid( 0 ),
      space( NULL ),
      length( 0 )
{}

SSMessage::~SSMessage()
{
    length = 0;

    if ( space != NULL )
    {
        std::free( space );
        space = NULL;
    }
}

void SSMessage::clear()
{
    space = NULL;
    length = 0;
}

bool SSMessage::make( const char * buffer, uint32_t len )
{
    if ( space != NULL )
    {
        return false;
    }

    space = (char *)std::malloc( len );
    if ( space != NULL )
    {
        length = len;
        std::memcpy( (void *)space, (void *)buffer, len );
    }

    return space != NULL;
}

///////////////////////////////////////////////////////////////////////////////////

PingCommand::PingCommand()
{
    head.cmd = eSSCommand_Ping;
}

PingCommand::~PingCommand()
{}

Slice PingCommand::encode()
{
    StreamBuf pack( 64, sizeof(SSHead) );

    // TODO: BODY

    // 计算长度
    space = pack.data();
    length = pack.length();
    head.size = pack.size();

    // 重置并且编码HEAD
    pack.reset();
    pack.encode( head.cmd );
    pack.encode( head.size );

    return pack.slice();
}

bool PingCommand::decode( const Slice & data )
{
    // 解析数据
    StreamBuf unpack(
            data.data(), data.size() );
    // TODO:

    return true;
}

///////////////////////////////////////////////////////////////////////////////////

SyncRequest::SyncRequest()
    : lastseq( 0ULL )
{
    head.cmd = eSSCommand_SyncRequest;
}

SyncRequest::~SyncRequest()
{}

Slice SyncRequest::encode()
{
    StreamBuf pack( 1024, sizeof(SSHead) );

    // BODY
    pack.encode( lastseq );
    pack.encode( lastkey );

    // 计算长度
    space = pack.data();
    length = pack.length();
    head.size = pack.size();

    // 重置并且编码HEAD
    pack.reset();
    pack.encode( head.cmd );
    pack.encode( head.size );

    return pack.slice();
}

bool SyncRequest::decode( const Slice & data )
{
    StreamBuf unpack(
            data.data(), data.size() );
    unpack.decode( lastseq );
    unpack.decode( lastkey );
    return true;
}

/////////////////////////////////////////////////////////////////////////////////////////

SyncResponse::SyncResponse()
{
    head.cmd = eSSCommand_SyncResponse;
}

SyncResponse::~SyncResponse()
{}

Slice SyncResponse::encode()
{
    StreamBuf pack( 1024, sizeof(SSHead) );

    // BODY
    pack.encode( method );
    pack.encode( binlog );
    pack.encode( value );

    // 计算长度
    space = pack.data();
    length = pack.length();
    head.size = pack.size();

    // 重置并且编码HEAD
    pack.reset();
    pack.encode( head.cmd );
    pack.encode( head.size );

    return pack.slice();
}

bool SyncResponse::decode( const Slice & data )
{
    StreamBuf unpack(
            data.data(), data.size() );
    unpack.decode( method );
    unpack.decode( binlog );
    unpack.decode( value );

    return true;
}
}
