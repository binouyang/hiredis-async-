#include <stdarg.h>
#include "hiredis_async_wrapper.h"
#include "singleton.h"





RDAsyncResult::RDAsyncResult()
{
    _result = NULL;
    _child_num = 0;
}

RDAsyncResult::~RDAsyncResult()
{
}

int32_t RDAsyncResult::fetch_query_result(redisReply *res)
{
    if (res->type == REDIS_REPLY_STRING)
    {
        _res_map[const_cast<char*>("str")] = res->str;
        return 1;
    }

    _child_num = res->elements;
    _result = res;
    uint32_t i = 0;
    int32_t cnt = 0;


    while (i < _child_num)
    {
        _res_map[res->element[i]->str] = res->element[i+1]->str;
        i += 2;
        cnt++;
    }
    return cnt;
}

void RDAsyncResult::free_result()
{
    if (_result)
    {
        freeReplyObject(_result);
    }
    _result = NULL;
    _child_num = 0;
    _res_map.clear();
}

#define TEST_GET    if (_res_map.find(k) == _res_map.end())\
                    {\
                        LOG_DEBUG("get %s failed.\n", k);\
                        return 0;\
                    }\

char RDAsyncResult::get_int8(const char * key)
{
    char * k = const_cast<char*>(key);
    TEST_GET

    return (char)(*(_res_map[k]));
}

int16_t RDAsyncResult::get_int16(const char * key)
{
    char * k = const_cast<char*>(key);
    TEST_GET

    return (int16_t)::atoi(_res_map[k]);
}

int32_t RDAsyncResult::get_int32(const char * key)
{
    char * k = const_cast<char*>(key);
    TEST_GET

    return (int32_t)::atoi(_res_map[k]);
}

int64_t RDAsyncResult::get_int64(const char * key)
{
    char * k = const_cast<char*>(key);
    TEST_GET

    return (int64_t)::atoll(_res_map[k]);
}

char * RDAsyncResult::get_string(const char * key)
{
    //TEST_GET
    char * k = const_cast<char*>(key);
    if (_res_map.find(k) == _res_map.end())
    {
        LOG_DEBUG("get %s failed.\n", k);
        return const_cast<char*>("");
    }

    return _res_map[k];
}

double RDAsyncResult::get_double(const char * key)
{
    char * k = const_cast<char*>(key);
    TEST_GET

    return 0;
}

float RDAsyncResult::get_float(const char * key)
{
    char * k = const_cast<char*>(key);
    TEST_GET

    return 0;
}

