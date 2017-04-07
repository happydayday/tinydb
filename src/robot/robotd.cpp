
#include <string>
#include <iostream>

#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

#include <linux/input.h>
#include <sys/types.h>
#include <fcntl.h>

#include "base.h"
#include "types.h"
#include "version.h"

#include "utils/file.h"
#include "utils/timeutils.h"

#include "robotclient.h"
//
RunStatus               g_RunStatus;
utils::LogFile *        g_Logger;

//
static void signal_handle( int signo );
static void help( const char * module );
static void parse_cmdline( int argc, char ** argv, std::string & module );
static void initialize( const char * module );
static void finitialize();

void signal_handle( int signo )
{
    switch( signo )
    {
        case SIGINT :
        case SIGTERM :
        case SIGQUIT :
            {
                g_RunStatus = eRunStatus_Stop;
            }
            break;

        case SIGHUP :
            g_RunStatus = eRunStatus_Reload;
            break;

        case SIGUSR1 :
            g_RunStatus = eRunStatus_FlushLog;
            break;
    }
}

void help( const char * module )
{
    printf("%s [-d] [-h|-v] \n", module);
    printf("\t%s --help\n", module);
    printf("\t%s --version\n", module);
    exit(0);
}

void parse_cmdline( int argc, char ** argv, std::string & module )
{
    bool isdaemon = false;

    // 解释命令
    char * result = strrchr( argv[0], '/' );
    module = result+1;

    // 解释参数
    if ( argc > 2 )
    {
        help( module.c_str() );
    }
    else if ( argc == 2 )
    {
        if ( strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0 )
        {
            help( module.c_str() );
        }
        else if ( strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0 )
        {
            help( module.c_str() );
        }
        else if ( strcmp(argv[1], "-d") == 0 )
        {
            isdaemon = true;
        }
    }

    // 后台执行
    if ( isdaemon )
    {
        daemon( 1, 0 );
    }

    return;
}

void initialize( const char * module )
{
    bool rc = false;

    // 初始化信号
    signal( SIGPIPE, SIG_IGN );
    signal( SIGINT, signal_handle );
    signal( SIGHUP, signal_handle );
    signal( SIGQUIT, signal_handle );
    signal( SIGTERM, signal_handle );
    signal( SIGUSR1, signal_handle );

    // 打开全局日志文件
    g_Logger = new utils::LogFile( "log", module );
    assert( g_Logger != NULL && "new utils::LogFile failed" );
    rc = g_Logger->open();
    assert( rc && "utils::LogFile()::open failed" );

    return;
}

void finitialize()
{
    g_Logger->close();
    delete g_Logger;
}

int main(int argc, char ** argv)
{
    std::string module;

    // 解释命令行
    parse_cmdline( argc, argv, module );

    // 初始化
    initialize( module.c_str() );
    g_RunStatus = eRunStatus_Running;

    tinydb::CRobotClient * client = new tinydb::CRobotClient( 0, 0 );
    if ( client == NULL )
    {
        return 0;
    }

    if ( !client->connect( "127.0.0.1", 18000, 10, true ) )
    {
        return 0;
    }

    // 服务开启
    LOG_INFO( "%s-%s start ...\n", module.c_str(), __APPVERSION__ );

    // 发送指令
    std::string cmd;
    while ( g_RunStatus == eRunStatus_Running )
    {
        getline( std::cin, cmd );
        if ( !cmd.empty() )
        {
            cmd.append( "\r\n" );
            client->send( client->getSid(), cmd );
            cmd.clear();
        }
        utils::TimeUtils::sleep( 100 );
    }

    // 服务退出
    g_RunStatus = eRunStatus_Stop;

    // 关闭连接
    client->shutdown( client->getSid() );
    delete client;
    client = NULL;

    LOG_INFO( "%s-%s stop .\n", module.c_str(), __APPVERSION__ );
    // 销毁
    finitialize();

    return 0;
}
