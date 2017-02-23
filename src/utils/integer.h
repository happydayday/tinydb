
#ifndef __SRC_UTILS_INTEGER_H__
#define __SRC_UTILS_INTEGER_H__

#include <string>
#include <stdint.h>

namespace utils
{

//
// 整型转字符串
//
class Integer
{
public :
    // int8_t 转 字符串
    static std::string toString( int8_t a );
    static std::string toString( uint8_t a );

    // int16_t 转 字符串
    static std::string toString( int16_t a );
    static std::string toString( uint16_t a );

    // int32_t 转 字符串
    static std::string toString( int32_t a );
    static std::string toString( uint32_t a );

    // int64_t 转 字符串
    static std::string toString( int64_t a );
    static std::string toString( uint64_t a );
};

// 变长整型
class Varint
{
public :
    // 序列化32位变长整型
    static char * encode( char * dst, uint32_t v );
    // 序列化64位变长整型
    static char * encode( char * dst, uint64_t v );

public :
    // 反序列化32位变长整型
    static const char * decode(
            const char * src, const char * limit, uint32_t & v );
    // 反序列化64位变长整型
    static const char * decode(
            const char * src, const char * limit, uint64_t & v );

private :
    static const uint32_t B = 128;
};

}

#endif
