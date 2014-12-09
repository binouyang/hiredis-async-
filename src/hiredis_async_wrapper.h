#ifndef __HIREDIS_ASYNC_WRAPPER_H__
#define __HIREDIS_ASYNC_WRAPPER_H__

#include <sys/socket.h>
#include <stdint.h>
#include <map>
#include <string>
#include <string.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libev.h>
#include <ev.h>
#include "singleton.h"

using std::map;

namespace redis_async
{
struct ptr_cmp
{
    bool operator()(const char * a, const char * b) const
    {
        return strcmp(a, b) < 0;
    }
};

typedef map<char *, char *, ptr_cmp> RES_MAP;
}

class RDAsyncConnectString;
class RDAsyncConnection;
//class RDAsyncQuery<class K, void (K::*method())(struct redisAsyncContext*, void *reply)>;
class RDAsyncResult;

class RDAsyncConnectString
{
public:
    std::string host;
    int16_t port;

public:
    RDAsyncConnectString()
    {
        host = "";
        port = 0;
    }

    ~RDAsyncConnectString()
    {
    }

private:

};

class RDAsyncConnection
{
public:

    RDAsyncConnection()
    {
        _redis_h = NULL;
        _init_status = 0;
        ev = ev_default_loop(0);
    }
    
    ~RDAsyncConnection()
    {
    }
    
    template<class T, void (T::*call_back)(const redisAsyncContext *ac, int status, int cmd)>
    int32_t open_connection(T *data)
    {
        if (("" == _con_str.host)
            || (0 == _con_str.port))
        {
            LOG_ERROR("redis string error, host:%s, port:%d.\n",
                            _con_str.host.c_str(), _con_str.port);
            return -1;
        }
    

        if (_redis_h != NULL)
        {
            //close_connection();
        }
        
        _redis_h = redisAsyncConnect(_con_str.host.c_str(), _con_str.port);
        LOG_DEBUG("host %s, port %d\n", _con_str.host.c_str(), _con_str.port);
        //_redis_h = redisConnectNonBlock(str->host, str->port);
        //_redis_h = redisConnect(str->host, str->port);
        if (_redis_h == NULL)
        {
            LOG_ERROR("redis connect err\n");
            return -1;
        }

        if (_redis_h->err)
        {
            LOG_ERROR("redis connect err[%s].\n", _redis_h->errstr);
            redisAsyncFree(_redis_h);
            _redis_h = NULL;
            return -2;
        }

        redisLibevAttach(ev, _redis_h);
        redisAsyncSetConnectCallback(_redis_h, con_call_back<T, call_back>);
        redisAsyncSetDisconnectCallback(_redis_h, discon_call_back<T, call_back>);
        _redis_h->data = data;
        _init_status = 1;

        return 0;
    }

    bool is_redis_connected()
    {
        return _redis_h->c.flags & REDIS_CONNECTED;
    }
    
    int32_t close_connection()
    {
        redisAsyncFree(_redis_h);
        _redis_h = NULL;
        _init_status = 0;
        return 0;
    }
    
    bool test_connection()
    {
 #if 0
        if (_init_status == 0)
        {
            if (open_connection(_con_str) < 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    
        redisReply* reply = (redisReply*)redisCommand(_redis_h, "ping");
        if (reply == NULL)
        {
            close_connection();
            if (open_connection() < 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    
        freeReplyObject(reply);
#endif
        return true;
    }

    inline redisAsyncContext * get_redis() const
    {
        return _redis_h;
    }

    inline bool is_connected() const
    {
        return _init_status;
    }

    inline void set_connect_string(std::string host, int32_t port)
    {
        _con_str.host = host;
        _con_str.port = port;
    }

    inline void set_ev_loop(struct ev_loop *ev)
    {
        this->ev = ev;
    }

    template<class T>
    void set(T *obj)
    {
        this->obj = (void *)obj;
    }

    template<class T, void (T::*call_back)(const redisAsyncContext *c, int status, int cmd)>
    static void con_call_back(const redisAsyncContext *c, int status)
    {
        LOG_DEBUG("connect ok\n");
        (static_cast<T *>(c->data)->*call_back)(c, status, 1);
    }

    template<class T, void (T::*call_back)(const redisAsyncContext *c, int status, int cmd)>
    static void discon_call_back(const redisAsyncContext *c, int status)
    {
        LOG_DEBUG("discon_call_back\n");
        (static_cast<T *>(c->data)->*call_back)(c, status, 2);
    }

private:
    redisAsyncContext * _redis_h;
    struct ev_loop *ev;
    bool _init_status;
    RDAsyncConnectString _con_str;

    void *obj;
    
    

};


class RDAsyncQuery
{
public:
    RDAsyncQuery()
    {
        _redis_con = NULL;
        _pipe_cmd_cnt = 0;
        _connect_stat = 0;
    }
    ~RDAsyncQuery()
    {
        _pipe_cmd_cnt = 0;
    }
    

    /*
        get 数据如果是异步的，目前没有较好的办法实现，除非用一个模板
        既然目前没有异步读取方面的需求，暂时就不理会吧
        */
/*    int32_t execute_get(RDResult * res,
                        const char * fmt,
                        ...);
                        */

    void set_connect_stat(int stat)
    {
        _connect_stat = stat;
    }

    int32_t get_connect_stat()
    {
        return _connect_stat;
    }

    bool is_connect_ok()
    {
        return (_connect_stat > 0);
    }
    
    void set_connection(RDAsyncConnection* con)
    {
        _redis_con = con;
        set_connect_stat(1);
    }
    
    
    void clear()
    {
        _redis_con = NULL;
    }
    
    int32_t execute_set(const char *fmt, ...)
    {
#if 0
        if (!_redis_con->test_connection())
        {
            return -1;
        }
#endif
        if (!is_connect_ok())
        {
            LOG_WARN("connect not ok\n");
            return -1;
        }

        va_list ap;
        va_start(ap, fmt);
        int32_t ret = redisvAsyncCommand(_redis_con->get_redis(), NULL, NULL, fmt, ap);
        if (ret != REDIS_OK)
        {
            //LOG_ERROR("redis disconnected\n");
            _redis_con->close_connection();
            va_end(ap);
       
            return -1;        
        }
        va_end(ap);
    
        return 0;
    }
    
    

private:
    RDAsyncConnection *_redis_con;
    int32_t _pipe_cmd_cnt;
    int32_t    _connect_stat;

};


class RDAsyncResult
{
public:
    RDAsyncResult();
    ~RDAsyncResult();

    inline redisReply * get_result() const
    {
        return _result;
    }

    int32_t fetch_query_result(redisReply * res);
    void free_result();

    char get_int8(const char * key);
    int16_t get_int16(const char * key);
    int32_t get_int32(const char * key);
    int64_t get_int64(const char * key);
    char * get_string(const char * key);
    double get_double(const char * key);
    float get_float(const char * key);

private:
    redisReply * _result;
    uint32_t _child_num;
    redis_async::RES_MAP _res_map;
};

#endif

