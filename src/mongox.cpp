/*
   ----------------------------------------------------------------------------
   | mongox: Synchronous and Asynchronous access to MongoDB                   |
   | Author: Chris Munt cmunt@mgateway.com                                    |
   |                    chris.e.munt@gmail.com                                |
   | Copyright (c) 2019-2023 MGateway Ltd                                     |
   | Surrey UK.                                                               |
   | All rights reserved.                                                     |
   |                                                                          |
   | http://www.mgateway.com                                                  |
   |                                                                          |
   | Licensed under the Apache License, Version 2.0 (the "License"); you may  |
   | not use this file except in compliance with the License.                 |
   | You may obtain a copy of the License at                                  |
   |                                                                          |
   | http://www.apache.org/licenses/LICENSE-2.0                               |
   |                                                                          |
   | Unless required by applicable law or agreed to in writing, software      |
   | distributed under the License is distributed on an "AS IS" BASIS,        |
   | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. |
   | See the License for the specific language governing permissions and      |
   | limitations under the License.                                           |
   ----------------------------------------------------------------------------
*/

/*

Change Log:

Version 1.0.1 17 September 2013:
   Experimental.

Version 1.0.2 15 November 2013:
   First release.

Version 1.0.3 17 November 2013:
   Support nested arrays and correct for numeric data types.

Version 1.0.4 25 November 2013:
   Add new methods and fix issues with long integers.  Also expose _id after calling insert().
      - command()
      - object_id()
      - object_id_date()

Version 1.0.5 29 November 2013:
   Introduce proper error handlers.
   Tighten up garbage collection - particularly of BSON objects.
   Introduce insert_batch() method

Version 1.0.6 30 November 2013:
   Fix segfault/accvio in the m_command method.

Version 1.0.7 2 December 2013:
   introduce create_index() method.
   - Expose all the arguments on API call mongo_find() through mongo_retrieve(): fields JSON object, limit (no of rows), skip (no of rows), options (flags)

Version 1.1.8 20 April 2015:
   Introduce support for Node v0.12.x

Version 1.2.9 20 December 2016:
   Rename as 'mongox' and publish through NPM

Version 1.2.10 21 December 2016:
   Rename as 'mongo-dbx' and publish through NPM

Version 1.3.11 16 August 2019:
   Support for Node.js v8, v10 and v12.

Version 1.3.12 12 September 2019:
   Replace functionality that was deprecated in Node.js/V8 v12.

Version 1.4.13 6 May 2020:
   Verify that the code base works with Node.js v14.x.x.
   Introduce support for Node.js/V8 worker threads (for Node.js v12.x.x. and later).
   Suppress a number of benign 'cast-function-type' compiler warnings when building on the Raspberry Pi.

Version 1.4.14 28 April 2021:
   Verify that the code base works with Node.js v16.x.x.
   Fix A number of faults related to the use of mongo-dbx functionality in Node.js/v8 worker threads.
   - Notably, callback functions were not being fired correctly for some asynchronous invocations of mongo-dbx methods.

Version 1.4.14a 25 April 2022:
   Verify that the code base works with Node.js v18.x.x.

Version 1.4.14b 3 May 2023:
   Verify that the code base works with Node.js v20.x.x.

Version 1.4.14c 22 June 2023:
   Documentation update.

*/


/*
   Includes:         MONGO_STATIC_BUILD;MONGO_HAVE_STDINT
   Windows library:  WS2_32.lib
*/


#if defined(_WIN32)

#pragma comment(lib, "Ws2_32.lib")

#define BUILDING_NODE_EXTENSION     1
#if defined(_MSC_VER)
/* Check for MS compiler later than VC6 */
#if (_MSC_VER >= 1400)
#define _CRT_SECURE_NO_DEPRECATE    1
#define _CRT_NONSTDC_NO_DEPRECATE   1
#endif
#endif
#elif defined(__linux__) || defined(__linux) || defined(linux)
#define LINUX                       1
#elif defined(__APPLE__)
#define MACOSX                      1
#else
#error "Unknown Compiler"
#endif

#if !defined(_WIN32)
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#endif

#if defined(__GNUC__) && __GNUC__ >= 8
#define DISABLE_WCAST_FUNCTION_TYPE _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wcast-function-type\"")
#define DISABLE_WCAST_FUNCTION_TYPE_END _Pragma("GCC diagnostic pop")
#else
#define DISABLE_WCAST_FUNCTION_TYPE
#define DISABLE_WCAST_FUNCTION_TYPE_END
#endif

DISABLE_WCAST_FUNCTION_TYPE

#include <v8.h>
#include <node.h>
#include <node_version.h>

#define MGX_VERSION_MAJOR        1
#define MGX_VERSION_MINOR        4
#define MGX_VERSION_BUILD        14
#define MGX_VERSION              MGX_VERSION_MAJOR "." MGX_VERSION_MINOR "." MGX_VERSION_BUILD

#define MGX_NODE_VERSION         (NODE_MAJOR_VERSION * 10000) + (NODE_MINOR_VERSION * 100) + NODE_PATCH_VERSION

#include <uv.h>
#include <node_object_wrap.h>

#if !defined(_WIN32)
#include <pthread.h>
#include <dlfcn.h>
#endif

#include "mongo.h"

#define MGX_ERROR_SIZE              512

#if MGX_NODE_VERSION >= 120000
#define MGX_GET(a,b)                a->Get(icontext,b).ToLocalChecked()
#define MGX_SET(a,b,c)              a->Set(icontext,b,c).FromJust()
#define MGX_TONUMBER(a)             a->NumberValue(icontext).ToChecked()
#define MGX_TOINT32(a)              a->Int32Value(icontext).FromJust()
#define MGX_TOUINT32(a)             a->Uint32Value(icontext).FromJust()
#define MGX_TOOBJECT(a)             a->ToObject(icontext).ToLocalChecked()
#define MGX_TOSTRING(a)             a->ToString(icontext).ToLocalChecked()
#elif MGX_NODE_VERSION >= 100000
#define MGX_GET(a,b)                a->Get(icontext,b).ToLocalChecked()
#define MGX_SET(a,b,c)              a->Set(icontext,b,c).FromJust()
#define MGX_TONUMBER(a)             a->NumberValue(icontext).ToChecked()
#define MGX_TOINT32(a)              a->Int32Value(icontext).FromJust()
#define MGX_TOUINT32(a)             a->Uint32Value(icontext).FromJust()
#define MGX_TOOBJECT(a)             a->ToObject(icontext).ToLocalChecked()
#define MGX_TOSTRING(a)             a->ToString(icontext).ToLocalChecked()
#elif MGX_NODE_VERSION >= 70000
#define MGX_GET(a,b)                a->Get(b)
#define MGX_SET(a,b,c)              a->Set(b,c)
#define MGX_TOINT32(a)              a->Int32Value()
#define MGX_TOUINT32(a)             a->Uint32Value()
#define MGX_TONUMBER(a)             a->NumberValue()
#define MGX_TOOBJECT(a)             a->ToObject()
#define MGX_TOSTRING(a)             a->ToString()
#else
#define MGX_GET(a,b)                a->Get(b)
#define MGX_SET(a,b,c)              a->Set(b,c)
#define MGX_TOINT32(a)              a->ToInt32()->Value()
#define MGX_TOUINT32(a)             a->ToUint32()->Value()
#define MGX_TONUMBER(a)             a->ToNumber()->Value()
#define MGX_TOOBJECT(a)             a->ToObject()
#define MGX_TOSTRING(a)             a->ToString()
#endif

#define MGX_INTEGER_NEW(a)          Integer::New(isolate, a)
#define MGX_OBJECT_NEW()            Object::New(isolate)
#define MGX_ARRAY_NEW(a)            Array::New(isolate, a)
#define MGX_NUMBER_NEW(a)           Number::New(isolate, a)
#define MGX_BOOLEAN_NEW(a)          Boolean::New(isolate, a)
#define MGX_NULL()                  Null(isolate)

#if MGX_NODE_VERSION >= 120000
#define MGX_DATE(a)                 Date::New(icontext, a).ToLocalChecked()
#else
#define MGX_DATE(a)                 Date::New(isolate, a)
#endif

#define MGX_NODE_SET_PROTOTYPE_METHOD(a, b)    NODE_SET_PROTOTYPE_METHOD(t, a, b);
#define MGX_NODE_SET_PROTOTYPE_METHODC(a, b)   NODE_SET_PROTOTYPE_METHOD(t, a, b);

#define MGX_THROW_EXCEPTION(a) \
   isolate->ThrowException(Exception::Error(mongox_new_string8(isolate, a, 1))); \
   return; \

#define MGX_THROW_EXCEPTIONV(a) \
   isolate->ThrowException(Exception::Error(a)); \
   return; \

#define MGX_MONGOAPI_START() \
   if (!s->open) { \
         isolate->ThrowException(Exception::Error(mongox_new_string8(isolate, (char *) "Connection not established to Mongo Database", 1))); \
         return; \
   } \

#define MGX_MONGOAPI_ERROR() \
   if (!baton) { \
      isolate->ThrowException(Exception::Error(mongox_new_string8(isolate, (char *) "Unable to process arguments", 1))); \
      return; \
   } \
   if (baton->p_mgxapi->error[0]) { \
      isolate->ThrowException(Exception::Error(mongox_new_string8(isolate, (char *) baton->p_mgxapi->error, 1))); \
      return; \
   } \

#define MGX_RETURN_VALUE(a) \
   args.GetReturnValue().Set(a); \
   return; \

#define MGX_CALLBACK_FUN(JSNARG, CB, ASYNC) \
   JSNARG = args.Length(); \
   if (JSNARG > 0 && args[JSNARG - 1]->IsFunction()) { \
      ASYNC = 1; \
      JSNARG --; \
   } \
   else { \
      ASYNC = 0; \
   } \


#define MGX_MONGOAPI_END()

#define MGX_DEFAULT_OID_NAME           "_id"

#define MGX_JSON_OBJECT                0
#define MGX_JSON_ARRAY                 1

#define MGX_METHOD_ABOUT               1
#define MGX_METHOD_VERSION             2
#define MGX_METHOD_OPEN                3
#define MGX_METHOD_CLOSE               4
#define MGX_METHOD_RETRIEVE            5
#define MGX_METHOD_INSERT              6
#define MGX_METHOD_INSERT_BATCH        7
#define MGX_METHOD_UPDATE              8
#define MGX_METHOD_REMOVE              9
#define MGX_METHOD_COMMAND             10
#define MGX_METHOD_CREATE_INDEX        11
#define MGX_METHOD_OBJECT_ID           12
#define MGX_METHOD_OBJECT_ID_DATE      13

static const char * mgx_methods[] = {
      "unknown",
      "about",
      "version",
      "open",
      "close",
      "retrieve",
      "insert",
      "insert_batch",
      "update",
      "remove",
      "command",
      "create_index",
      "object_id",
      "object_id_date",
      NULL
   };


#if defined(_WIN32)
typedef HINSTANCE       MGXPLIB;
typedef FARPROC         MGXPROC;
#else
typedef void            * MGXPLIB;
typedef void            * MGXPROC;
#endif


typedef struct tagMGXJSON {
   short       id;
   bson_oid_t  oid;
   char        oid_name[64];
   char        oid_value[32];
   struct tagMGXBSON *p_next;
} MGXJSON, *PMGXJSON;


typedef struct tagMGXBSON {
   short    id;
   bson     *bobj;
   struct tagMGXBSON *p_next;
} MGXBSON, *PMGXBSON;


typedef struct tagMGXAPI {
   int            level;
   int            bobj_main_list_no;
   mongo_cursor   *cursor;
   bson_iterator  *iterator;
   bson           *bobj_main;
   bson           *bobj_ref;
   bson           *bobj_fields;
   bson           **bobj_main_list;
   MGXJSON        *jobj_main_list;
   char           file_name[128];
   char           index_name[128];
   int            context;
   int            output_integer;
   int            options;
   int            limit;
   int            skip;
   unsigned long  margin;
   unsigned long  size;
   unsigned long  curr_size;
   unsigned long  output_size;
   unsigned long  output_curr_size;
   char           *output;
   char           method[32];
   int            error_code;
   char           error[MGX_ERROR_SIZE];
   MGXBSON        *p_mgxbson_head;
   MGXBSON        *p_mgxbson_tail;
} MGXAPI, *PMGXAPI;


#if !defined(_WIN32)
extern int errno;
#endif

#if defined(_WIN32)
CRITICAL_SECTION  mgx_async_mutex;
#else
pthread_mutex_t   mgx_async_mutex        = PTHREAD_MUTEX_INITIALIZER;
#endif


void *                  mgx_malloc                    (int size, short id);
int                     mgx_free                      (void *p, short id);
bson *                  mgx_bson_alloc                (MGXAPI * p_mgxapi, int init, short id);
int                     mgx_bson_free                 (MGXAPI * p_mgxapi);
int                     mgx_ucase                     (char *string);
int                     mgx_lcase                     (char *string);
int                     mgx_buffer_dump               (char *buffer, unsigned int len, short mode);
MGXPLIB                 mgx_dso_load                  (char * library);
MGXPROC                 mgx_dso_sym                   (MGXPLIB p_library, char * symbol);
int                     mgx_dso_unload                (MGXPLIB p_library);


using namespace node;
using namespace v8;


#if defined(_WIN32)
BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved)
{
   switch (fdwReason)
   { 
      case DLL_PROCESS_ATTACH:
         InitializeCriticalSection(&mgx_async_mutex);
         break;
      case DLL_THREAD_ATTACH:
         break;
      case DLL_THREAD_DETACH:
         break;
      case DLL_PROCESS_DETACH:
         DeleteCriticalSection(&mgx_async_mutex);
         break;
   }
   return TRUE;
}
#endif


class server : public node::ObjectWrap
{

private:

   short open;
   int   m_count;
   int   mongo_port;
   char  mongo_address[64];
   mongo mongo_connection;
#if defined(_WIN32)
   WORD              wVersionRequested;
   WSADATA           wsaData;
#endif

struct mongo_baton_t {
      server *                s;
      unsigned char           result_iserror;
      unsigned char           result_isarray;
      int                     increment_by;
      int                     sleep_for;
      Local<Object>           jobj_ref;
      Local<Object>           jobj_main;
      Local<Object>           jobj_fields;
      Local<Object>           json_result;
      Local<Array>            array_result;
      Persistent<Function>    cb;
      Isolate                 *isolate;
      MGXAPI * p_mgxapi;
   };


   int mongox_mgx_version(server *s, mongo_baton_t * baton)
   {
      sprintf(baton->p_mgxapi->output, "%d.%d.%d", MGX_VERSION_MAJOR, MGX_VERSION_MINOR, MGX_VERSION_BUILD);
/*
      sprintf(baton->p_mgxapi->output, "MongoX.JS: Version: %s (CM)", MGX_VERSION);
*/
      return 0;
   }

   int mongox_open(server *s, mongo_baton_t * baton)
   {
      int ret;

#if defined(_WIN32)
      int error_code;

      s->wVersionRequested = MAKEWORD(2, 2);

      error_code = WSAStartup(s->wVersionRequested, &(s->wsaData));
      if (error_code != 0) {
         strcpy(baton->p_mgxapi->error, "Windows WSAStartup() call failed");
         baton->p_mgxapi->error_code = error_code;
         return MONGO_ERROR;
      }
#endif

      ret = mongo_client(&(s->mongo_connection), s->mongo_address, s->mongo_port);

      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }

      return ret;
   }


   int mongox_close(server *s, mongo_baton_t * baton)
   {

      mongo_destroy(&(s->mongo_connection));

      return 1;
   }


   int mongox_retrieve(server *s, mongo_baton_t * baton)
   {

      baton->p_mgxapi->cursor = mongo_find(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_ref, baton->p_mgxapi->bobj_fields, baton->p_mgxapi->limit, baton->p_mgxapi->skip, baton->p_mgxapi->options);

      return 0;
   }


   int mongox_insert(server *s, mongo_baton_t * baton) 
   {
      int ret;

      ret = mongo_insert(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_main, 0);

      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }

      return ret;
   }


   int mongox_insert_batch(server *s, mongo_baton_t * baton) 
   {
      int ret;

      ret = mongo_insert_batch(&(s->mongo_connection), baton->p_mgxapi->file_name, (const bson **) baton->p_mgxapi->bobj_main_list, baton->p_mgxapi->bobj_main_list_no, 0, 0);

      return ret;
   }


   int mongox_update(server *s, mongo_baton_t * baton)
   {
      int ret;

      ret = mongo_update(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_ref, baton->p_mgxapi->bobj_main, MONGO_UPDATE_BASIC, 0);

      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }

      return ret;
   }


   int mongox_remove(server *s, mongo_baton_t * baton)
   {
      int ret;

      ret = mongo_remove(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_ref, 0);

      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }

      return ret;
   }

   int mongox_command(server *s, mongo_baton_t * baton)
   {
      int ret;

      baton->p_mgxapi->bobj_main = mgx_bson_alloc(baton->p_mgxapi, 1, 0);

      ret = mongo_run_command(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_ref, baton->p_mgxapi->bobj_main);

      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }

      return ret;
   }


   int mongox_create_index(server *s, mongo_baton_t * baton)
   {
      int ret;

      baton->p_mgxapi->bobj_main = mgx_bson_alloc(baton->p_mgxapi, 1, 0);

      if (baton->p_mgxapi->index_name[0])
         ret = mongo_create_index(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_ref, baton->p_mgxapi->index_name, 0, 0, baton->p_mgxapi->bobj_main);
      else
         ret = mongo_create_index(&(s->mongo_connection), baton->p_mgxapi->file_name, baton->p_mgxapi->bobj_ref, NULL, 0, 0, baton->p_mgxapi->bobj_main);

      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }
      return ret;
   }


   int mongox_object_id(server *s, mongo_baton_t * baton)
   {
      int ret;
      bson_oid_t oid;

      ret = 0;

      bson_oid_gen(&oid);
      bson_oid_to_string(&oid, baton->p_mgxapi->output);

      return ret;
   }


   int mongox_object_id_date(server *s, mongo_baton_t * baton)
   {
      int n, ret, len;
      char *p;
      time_t t;
      bson_oid_t oid;

      ret = mongox_is_object_id(s, baton, baton->p_mgxapi->file_name, &oid);
      if (ret != MONGO_OK) {
         mongox_error_message(s, baton);
      }
      else {
         t = bson_oid_generated_time(&oid);

         p = ctime(&t);
         if (p) {
            strcpy(baton->p_mgxapi->output, p);
            len = (int) strlen(baton->p_mgxapi->output);
            for (n = len - 1; n > 1; n --) {
               if (baton->p_mgxapi->output[n] != '\n' && baton->p_mgxapi->output[n] != '\r') {
                  baton->p_mgxapi->output[n + 1] = '\0';
                  break;
               }
            }
         }
      }

      return ret;
   }


   int mongox_is_object_id(server *s, mongo_baton_t * baton, char *oid_str, bson_oid_t *oid)
   {
      int len;
      char oid_str1[128];

      len = (int) strlen(oid_str);

      if (len != 24) {
         return -1;
      }
      bson_oid_from_string(oid, oid_str);
      bson_oid_to_string(oid, oid_str1);

      if (strcmp(oid_str, oid_str1)) {
         return -1;
      }

      return 0;
   }


   int mongox_error_message(server *s, mongo_baton_t * baton)
   {
      int size, error_code, len;

      size = MGX_ERROR_SIZE;
      error_code = s->mongo_connection.err;
      len = (int) strlen(s->mongo_connection.errstr);

      baton->p_mgxapi->error_code = error_code;
      if (len && len < size) {
         strcpy(baton->p_mgxapi->error, s->mongo_connection.errstr);
         return 0;
      }

      switch (error_code) {
         case MONGO_CONN_SUCCESS:
            strncat(baton->p_mgxapi->error, "Connection completed successfully.", size - 1);
            break;
         case MONGO_CONN_NO_SOCKET:
            strncat(baton->p_mgxapi->error, "Could not create a socket.", size - 1);
            break;
         case MONGO_CONN_FAIL:
            strncat(baton->p_mgxapi->error, "An error occured while calling connect()", size - 1);
            break;
         case MONGO_CONN_ADDR_FAIL:
            strncat(baton->p_mgxapi->error, "An error occured while calling getaddrinfo().", size - 1);
            break;
         case MONGO_CONN_NOT_MASTER:
            strncat(baton->p_mgxapi->error, "Warning: connected to a non-master node (read-only).", size - 1);
            break;
         case MONGO_CONN_BAD_SET_NAME:
            strncat(baton->p_mgxapi->error, "Given rs name doesn't match this replica set.", size - 1);
            break;
         case MONGO_CONN_NO_PRIMARY:
            strncat(baton->p_mgxapi->error, "Can't find primary in replica set. Connection closed.", size - 1);
            break;
         case MONGO_IO_ERROR:
            strncat(baton->p_mgxapi->error, "An error occurred while reading or writing on the socket.", size - 1);
            break;
         case MONGO_SOCKET_ERROR:
            strncat(baton->p_mgxapi->error, "Other socket error.", size - 1);
            break;
         case MONGO_READ_SIZE_ERROR:
            strncat(baton->p_mgxapi->error, "The response is not the expected length.", size - 1);
            break;
         case MONGO_COMMAND_FAILED:
            strncat(baton->p_mgxapi->error, "The command returned with 'ok' value of 0.", size - 1);
            break;
         case MONGO_WRITE_ERROR:
            strncat(baton->p_mgxapi->error, "Write with given write_concern returned an error.", size - 1);
            break;
         case MONGO_NS_INVALID:
            strncat(baton->p_mgxapi->error, "The name for the ns (database or collection) is invalid.", size - 1);
            break;
         case MONGO_BSON_INVALID:
            strncat(baton->p_mgxapi->error, "BSON not valid for the specified op.", size - 1);
            break;
         case MONGO_BSON_NOT_FINISHED:
            strncat(baton->p_mgxapi->error, "BSON object has not been finished.", size - 1);
            break;
         case MONGO_BSON_TOO_LARGE:
            strncat(baton->p_mgxapi->error, "BSON object exceeds max BSON size.", size - 1);
            break;
         case MONGO_WRITE_CONCERN_INVALID:
            strncat(baton->p_mgxapi->error, "Supplied write concern object is invalid.", size - 1);
            break;
         default:
            strncat(baton->p_mgxapi->error, "Unrecognized Mongo Error", size - 1);
            break;
      }
      baton->p_mgxapi->error[size - 1] = '\0';

      return 0;
   }


   int mongox_cleanup(server *s, mongo_baton_t * baton)
   {

      return 1;
   }


public:

   static Persistent<Function> s_ct;

#if MGX_NODE_VERSION >= 100000
   static void Init(Local<Object> exports)
#else
   static void Init(Handle<Object> exports)
#endif
   {
      Isolate* isolate = Isolate::GetCurrent();

      Local<FunctionTemplate> t = FunctionTemplate::New(isolate, New);
      t->InstanceTemplate()->SetInternalFieldCount(1);
      t->SetClassName(mongox_new_string8(isolate, (char *) "server",1));

      MGX_NODE_SET_PROTOTYPE_METHOD("about", About);
      MGX_NODE_SET_PROTOTYPE_METHOD("version", Version);
      MGX_NODE_SET_PROTOTYPE_METHOD("open", Open);
      MGX_NODE_SET_PROTOTYPE_METHOD("close", Close);
      MGX_NODE_SET_PROTOTYPE_METHOD("retrieve", Retrieve);
      MGX_NODE_SET_PROTOTYPE_METHOD("find", Retrieve);
      MGX_NODE_SET_PROTOTYPE_METHOD("insert", Insert);
      MGX_NODE_SET_PROTOTYPE_METHOD("insert_batch", Insert_Batch);
      MGX_NODE_SET_PROTOTYPE_METHOD("update", Update);
      MGX_NODE_SET_PROTOTYPE_METHOD("remove", Remove);
      MGX_NODE_SET_PROTOTYPE_METHOD("command", Command);
      MGX_NODE_SET_PROTOTYPE_METHOD("create_index", Create_Index);
      MGX_NODE_SET_PROTOTYPE_METHOD("object_id", Object_ID);
      MGX_NODE_SET_PROTOTYPE_METHOD("object_id_date", Object_ID_Date);

#if MGX_NODE_VERSION >= 120000
      Local<Context> icontext = isolate->GetCurrentContext();
      s_ct.Reset(isolate, t->GetFunction(icontext).ToLocalChecked());
      exports->Set(icontext, mongox_new_string8(isolate, (char *) "server", 1), t->GetFunction(icontext).ToLocalChecked()).FromJust();
#else
      s_ct.Reset(isolate, t->GetFunction());
      exports->Set(mongox_new_string8(isolate, (char *) "server", 1), t->GetFunction());
#endif

      return;
   }


   server() :
      m_count(0)
   {
   }


   ~server()
   {
   }


   static void New(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      server *s = new server();
      s->Wrap(args.This());

      s->open = 0;

      s->mongo_port = 0;
      strcpy(s->mongo_address, "");

      args.GetReturnValue().Set(args.This());
      return;
   }


   static mongo_baton_t * mongox_make_baton(server *s, int js_narg, const FunctionCallbackInfo<Value>& args, int context)
   {
      Isolate* isolate = args.GetIsolate();
#if MGX_NODE_VERSION >= 100000
      Local<Context> icontext = isolate->GetCurrentContext();
#endif
      HandleScope scope(isolate);
      int ret, obj_argn, n;
      char oid_name[64];
      char buffer[256];
      Local<Object> obj;
      Local<Value> key;
      Local<String> file;
      Local<String> value;
      Local<Array> jobj_array;
      bson *bobj;

      mongo_baton_t *baton = new mongo_baton_t();

      if (!baton)
         return NULL;

      *oid_name = '\0';

      baton->increment_by = 2;
      baton->sleep_for = 1;

      baton->p_mgxapi = (MGXAPI *) mgx_malloc(sizeof(MGXAPI), 101);
      if (!baton->p_mgxapi)
         return NULL;

      baton->p_mgxapi->context = context;
      baton->p_mgxapi->output_integer = 0;
      baton->p_mgxapi->error_code = 0;
      baton->p_mgxapi->error[0] = '\0';
      baton->p_mgxapi->output = NULL;

      baton->p_mgxapi->options = 0;
      baton->p_mgxapi->limit = 0;
      baton->p_mgxapi->skip = 0;

      baton->p_mgxapi->bobj_main_list_no = 0;
      baton->p_mgxapi->bobj_main_list = NULL;
      baton->p_mgxapi->jobj_main_list = NULL;
      baton->p_mgxapi->bobj_main = NULL;
      baton->p_mgxapi->bobj_ref = NULL;
      baton->p_mgxapi->bobj_fields = NULL;

      strncpy(baton->p_mgxapi->method, mgx_methods[context], 31);
      baton->p_mgxapi->method[31] = '\0';

      baton->p_mgxapi->margin = 1024;
      baton->p_mgxapi->size = baton->p_mgxapi->margin;
      baton->p_mgxapi->curr_size = 0;
      baton->p_mgxapi->level = 0;

      baton->p_mgxapi->p_mgxbson_head = NULL;
      baton->p_mgxapi->p_mgxbson_tail = NULL;

      baton->p_mgxapi->output_size = 1024;
      baton->p_mgxapi->output_curr_size = 0;
      baton->p_mgxapi->output = (char *) mgx_malloc(sizeof(char) * baton->p_mgxapi->output_size, 103);
      if (!baton->p_mgxapi->output) {
         mongox_destroy_baton(baton);
         return NULL;
      }
      baton->p_mgxapi->output[0] = '\0';

      baton->p_mgxapi->file_name[0] = '\0';
      baton->p_mgxapi->index_name[0] = '\0';

      if (context == MGX_METHOD_ABOUT || context == MGX_METHOD_VERSION || context == MGX_METHOD_CLOSE) {
         return baton;
      }
      else if (context == MGX_METHOD_OPEN) {
         baton->jobj_main = Local<Object>::Cast(args[0]);
         key = mongox_new_string8(isolate, (char *) "address", 1);
         if (MGX_GET(baton->jobj_main, key)->IsUndefined()) {
            strcpy(baton->p_mgxapi->error, "No IP address specified for Mongo Server");
            goto mongox_make_baton_exit;
         }
         else {
            value = MGX_TOSTRING(MGX_GET(baton->jobj_main, key));
            mongox_write_char8(isolate, value, s->mongo_address, 1);
         }

         key = mongox_new_string8(isolate, (char *) "port", 1);
         if (MGX_GET(baton->jobj_main, key)->IsUndefined()) {
            strcpy(baton->p_mgxapi->error, "No TCP Port specified for Mongo Server");
            goto mongox_make_baton_exit;
         }
         else {
            value = MGX_TOSTRING(MGX_GET(baton->jobj_main, key));
            mongox_write_char8(isolate, value, buffer, 1);
            s->mongo_port = (int) strtol(buffer, NULL, 10);
         }
      }
      else if (context == MGX_METHOD_INSERT) {
         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Namespace not specified for Insert Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            baton->jobj_main = Local<Object>::Cast(args[obj_argn]);

            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc(sizeof(MGXJSON), 10);
            strcpy(baton->p_mgxapi->jobj_main_list[0].oid_name, oid_name);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_main = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_main, NULL, bobj, 0, MGX_JSON_OBJECT, 0);
            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Object not specified for Insert Method");
            goto mongox_make_baton_exit;
         }
      }
      else if (context == MGX_METHOD_INSERT_BATCH) {
         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Namespace not specified for Insert Batch Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            jobj_array = Local<Array>::Cast(args[obj_argn]);
            baton->p_mgxapi->bobj_main_list_no = (int) jobj_array->Length();
            if (baton->p_mgxapi->bobj_main_list_no == 0) {
               strcpy(baton->p_mgxapi->error, "Mongo Object Array supplied for Insert Batch Method is empty");
               goto mongox_make_baton_exit;
            }

            baton->p_mgxapi->bobj_main_list = (bson **) mgx_malloc((sizeof(bson *) * baton->p_mgxapi->bobj_main_list_no), 10);
            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc((sizeof(MGXJSON) * baton->p_mgxapi->bobj_main_list_no), 10);

            for (n = 0; n < baton->p_mgxapi->bobj_main_list_no; n ++) {
               if (!MGX_GET(jobj_array, n)->IsObject()) {
                  sprintf(baton->p_mgxapi->error, "Mongo Object Array supplied for Insert Batch Method has a bad record at postion %d", n);
                  break;
               }
               baton->jobj_main = Local<Object>::Cast(MGX_TOOBJECT(MGX_GET(jobj_array, n)));
               strcpy(baton->p_mgxapi->jobj_main_list[n].oid_name, oid_name);
               baton->p_mgxapi->level = 0;
               bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
               baton->p_mgxapi->bobj_main_list[n] = bobj;
               ret = mongox_parse_json_object(s, baton, baton->jobj_main, NULL, bobj, n, MGX_JSON_OBJECT, 0);
               ret = bson_finish(bobj);
            }
            if (baton->p_mgxapi->error[0]) {
               goto mongox_make_baton_exit;
            }
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Object Array not specified for Insert Batch Method");
            goto mongox_make_baton_exit;
         }
      }
      else if (context == MGX_METHOD_UPDATE) {
         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Namespace not specified for Update Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            baton->jobj_ref = Local<Object>::Cast(args[obj_argn]);

            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc(sizeof(MGXJSON), 10);
            strcpy(baton->p_mgxapi->jobj_main_list[0].oid_name, oid_name);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_ref = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_ref, NULL, bobj, 0, MGX_JSON_OBJECT, 0);
            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Reference Object not specified for Update Method");
            goto mongox_make_baton_exit;
         }
         if (js_narg > (obj_argn + 1)) {
            baton->jobj_main = Local<Object>::Cast(args[obj_argn + 1]);
            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_main = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_main, NULL, bobj, 0, MGX_JSON_OBJECT, 0);
            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Object not specified for Update Method");
            goto mongox_make_baton_exit;
         }
      }
      else if (context == MGX_METHOD_RETRIEVE) {
         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Namespace not specified for Retrieve Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            baton->jobj_ref = Local<Object>::Cast(args[obj_argn]);

            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc(sizeof(MGXJSON), 10);
            strcpy(baton->p_mgxapi->jobj_main_list[0].oid_name, oid_name);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_ref = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_ref, NULL, bobj, 0, MGX_JSON_OBJECT, 0);
            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Reference Object not specified for Retrieve Method");
            goto mongox_make_baton_exit;
         }
         if (js_narg > (obj_argn + 1) && args[obj_argn + 1]->IsObject()) {
            baton->jobj_fields = Local<Object>::Cast(args[obj_argn + 1]);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_fields = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_fields, NULL, bobj, 0, MGX_JSON_OBJECT, 0);
            ret = bson_finish(bobj);
         }
         if (js_narg > (obj_argn + 2) && args[obj_argn + 2]->IsNumber()) {
            baton->p_mgxapi->limit = (int) MGX_TOINT32(args[obj_argn + 2]);
         }
         if (js_narg > (obj_argn + 3) && args[obj_argn + 3]->IsNumber()) {
            baton->p_mgxapi->skip = (int) MGX_TOINT32(args[obj_argn + 3]);
         }
         if (js_narg > (obj_argn + 4) && args[obj_argn + 4]->IsString()) {
            char buffer[256];
            value = MGX_TOSTRING(args[obj_argn + 4]);
            mongox_write_char8(isolate, value, buffer, 1);
            ret = mongox_parse_options(s, baton, buffer, 0);
            if (ret) {
               goto mongox_make_baton_exit;
            }
         }
      }
      else if (context == MGX_METHOD_REMOVE) {
         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Namespace not specified for Remove Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            baton->jobj_ref = Local<Object>::Cast(args[obj_argn]);

            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc(sizeof(MGXJSON), 10);
            strcpy(baton->p_mgxapi->jobj_main_list[0].oid_name, oid_name);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_ref = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_ref, NULL, bobj, 0, MGX_JSON_OBJECT, 0);
            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Reference Object not specified for Remove Method");
            goto mongox_make_baton_exit;
         }
      }
      else if (context == MGX_METHOD_COMMAND) {

         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Database not specified for Command Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            baton->jobj_ref = Local<Object>::Cast(args[obj_argn]);

            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc(sizeof(MGXJSON), 10);
            strcpy(baton->p_mgxapi->jobj_main_list[0].oid_name, oid_name);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_ref = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_ref, NULL, bobj, 0, MGX_JSON_OBJECT, 0);

            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Command Object not specified for Command Method");
            goto mongox_make_baton_exit;
         }
      }
      else if (context == MGX_METHOD_CREATE_INDEX) {

         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Database not specified for Create_Index Method");
            goto mongox_make_baton_exit;
         }
         obj_argn = 1;
         if (js_narg > 1 && args[1]->IsString()) {
            value = MGX_TOSTRING(args[1]);
            mongox_write_char8(isolate, value, oid_name, 1);
            obj_argn = 2;
         }
         if (!oid_name[0]) {
            strcpy(oid_name, MGX_DEFAULT_OID_NAME);
         }
         if (js_narg > obj_argn) {
            baton->jobj_ref = Local<Object>::Cast(args[obj_argn]);

            baton->p_mgxapi->jobj_main_list = (MGXJSON *) mgx_malloc(sizeof(MGXJSON), 10);
            strcpy(baton->p_mgxapi->jobj_main_list[0].oid_name, oid_name);

            baton->p_mgxapi->level = 0;
            bobj = mgx_bson_alloc(baton->p_mgxapi, 1, 0);
            baton->p_mgxapi->bobj_ref = bobj;
            ret = mongox_parse_json_object(s, baton, baton->jobj_ref, NULL, bobj, 0, MGX_JSON_OBJECT, 0);

            ret = bson_finish(bobj);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo Index Object not specified for Create_Index Method");
            goto mongox_make_baton_exit;
         }
         if (js_narg > (obj_argn + 1) && args[obj_argn + 1]->IsString()) {
            value = MGX_TOSTRING(args[obj_argn + 1]);
            mongox_write_char8(isolate, value, baton->p_mgxapi->index_name, 1);
         }
         if (js_narg > (obj_argn + 2) && args[obj_argn + 2]->IsString()) {
            char buffer[256];
            value = MGX_TOSTRING(args[obj_argn + 2]);
            mongox_write_char8(isolate, value, buffer, 1);
            ret = mongox_parse_options(s, baton, buffer, 0);
            if (ret) {
               goto mongox_make_baton_exit;
            }
         }
      }
      else if (context == MGX_METHOD_OBJECT_ID_DATE) {
         if (js_narg > 0) {
            file = Local<String>::Cast(args[0]);
            mongox_write_char8(isolate, file, baton->p_mgxapi->file_name, 1);
         }
         else {
            strcpy(baton->p_mgxapi->error, "Mongo ObjectID not specified");
            goto mongox_make_baton_exit;
         }
      }

mongox_make_baton_exit:

      return baton;
   }


   /* v1.4.14 */
   static int mongox_queue_task(void *work_cb, void *after_work_cb, mongo_baton_t *baton, short context)
   {
      uv_work_t *_req = new uv_work_t;
      _req->data = baton;

      /* v1.4.14 */
#if MGX_NODE_VERSION >= 120000
      uv_queue_work(GetCurrentEventLoop(baton->isolate), _req, (uv_work_cb) work_cb, (uv_after_work_cb) after_work_cb);
#else
      uv_queue_work(uv_default_loop(), _req, (uv_work_cb) work_cb, (uv_after_work_cb) after_work_cb);
#endif

      return 0;
   }


   static int mongox_parse_options(server *s, mongo_baton_t * baton, char *options, int context)
   {
      int ret, eol, eot, len;
      char *p, *pz;

      /* printf("\r\nALL Options: len=%d; =%s=\r\n", (int) strlen(options), options); */

      baton->p_mgxapi->options = 0;
      ret = 0;
      eol = 0;
      eot = 0;
      p = options;
      pz = p;
      while (!eol) {

         if (!(*pz))
            eol = 1;
         else if (*pz != '_' && !isalpha((int) *pz) && !isdigit((int) *pz))
            eot = 1;

         if (eol || eot) {
            *pz = '\0';
            len = (int) strlen(p);
            if (len) {
               /* printf("\r\nOption: len=%d; =%s=", (int) strlen(p), p); */

               if (baton->p_mgxapi->context == MGX_METHOD_INSERT) {
                  if (!strcmp(p, "MONGO_CONTINUE_ON_ERROR")) {
                     baton->p_mgxapi->options |= MONGO_CONTINUE_ON_ERROR;
                  }
                  else {
                     sprintf(baton->p_mgxapi->error, "Invalid Option (%s) supplied to Update method", p);
                     ret = -1;
                     break;
                  }
               }
               else if (baton->p_mgxapi->context == MGX_METHOD_UPDATE) {
                  if (!strcmp(p, "MONGO_UPDATE_UPSERT")) {
                     baton->p_mgxapi->options |= MONGO_UPDATE_UPSERT;
                  }
                  else if (!strcmp(p, "MONGO_UPDATE_MULTI")) {
                     baton->p_mgxapi->options |= MONGO_UPDATE_MULTI;
                  }
                  else if (!strcmp(p, "MONGO_UPDATE_BASIC")) {
                     baton->p_mgxapi->options |= MONGO_UPDATE_BASIC;
                  }
                  else {
                     sprintf(baton->p_mgxapi->error, "Invalid Option (%s) supplied to Update method", p);
                     ret = -1;
                     break;
                  }
               }
               else if (baton->p_mgxapi->context == MGX_METHOD_RETRIEVE) {

                  if (!strcmp(p, "MONGO_TAILABLE")) {
                     baton->p_mgxapi->options |= MONGO_TAILABLE;
                  }
                  else if (!strcmp(p, "MONGO_SLAVE_OK")) {
                     baton->p_mgxapi->options |= MONGO_SLAVE_OK;
                  }
                  else if (!strcmp(p, "MONGO_NO_CURSOR_TIMEOUT")) {
                     baton->p_mgxapi->options |= MONGO_NO_CURSOR_TIMEOUT;
                  }
                  else if (!strcmp(p, "MONGO_AWAIT_DATA")) {
                     baton->p_mgxapi->options |= MONGO_AWAIT_DATA;
                  }
                  else if (!strcmp(p, "MONGO_EXHAUST")) {
                     baton->p_mgxapi->options |= MONGO_EXHAUST;
                  }
                  else if (!strcmp(p, "MONGO_PARTIAL")) {
                     baton->p_mgxapi->options |= MONGO_PARTIAL;
                  }
                  else {
                     sprintf(baton->p_mgxapi->error, "Invalid Option (%s) supplied to Create_Index method", p);
                     ret = -1;
                     break;
                  }
               }
               else if (baton->p_mgxapi->context == MGX_METHOD_CREATE_INDEX) {

                  if (!strcmp(p, "MONGO_INDEX_UNIQUE")) {
                     baton->p_mgxapi->options |= MONGO_INDEX_UNIQUE;
                  }
                  else if (!strcmp(p, "MONGO_INDEX_DROP_DUPS")) {
                     baton->p_mgxapi->options |= MONGO_INDEX_DROP_DUPS;
                  }
                  else if (!strcmp(p, "MONGO_INDEX_BACKGROUND")) {
                     baton->p_mgxapi->options |= MONGO_INDEX_BACKGROUND;
                  }
                  else if (!strcmp(p, "MONGO_INDEX_SPARSE")) {
                     baton->p_mgxapi->options |= MONGO_INDEX_SPARSE;
                  }
                  else {
                     sprintf(baton->p_mgxapi->error, "Invalid Option (%s) supplied to Create_Index method", p);
                     ret = -1;
                     break;
                  }
               }
            }
            p = pz;
            while (*(++ p)) {
               if (isalpha((int) *p) || isdigit((int) *p)) {
                  break;
               }
            }
            pz = p;
            eot = 0;
         }
         else {
            pz ++;
         }
         if (eol) {
            break;
         }
      }

      return ret;
   }


   static int mongox_parse_json_object(server *s, mongo_baton_t * baton, Local<Object> jobj, char *jobj_name, bson *bobj, int jobj_no, int type, int context)
   {
      Isolate* isolate = Isolate::GetCurrent();
#if MGX_NODE_VERSION >= 100000
      Local<Context> icontext = isolate->GetCurrentContext();
#endif
      HandleScope scope(isolate);
      int ret;
      unsigned int n, name_len, value_len;
      char *name, *value;
      Local<Array> a;
      Local<String> name_str;
      Local<String> value_str;
      Local<Object> jobj_next;
      bson *bobj_next;

#if MGX_NODE_VERSION >= 120000
      a = jobj->GetPropertyNames(isolate->GetCurrentContext()).ToLocalChecked();
#else
      a = jobj->GetPropertyNames();
#endif
      for (n = 0; n < a->Length(); n ++) {

         name_str = MGX_TOSTRING(MGX_GET(a, n));
         name_len = mongox_string8_length(isolate, name_str, 1);

         name = (char *) mgx_malloc(sizeof(char) * (name_len + 2), 2001);
         *name = '\0';

         mongox_write_char8(isolate, name_str, name, 1);

         if (n == 0 && (baton->p_mgxapi->context == MGX_METHOD_INSERT || baton->p_mgxapi->context == MGX_METHOD_INSERT_BATCH) && baton->p_mgxapi->level == 0) {
            if (strcmp(name, baton->p_mgxapi->jobj_main_list[jobj_no].oid_name)) { /* no _id */
               /* bson_append_new_oid(baton->p_mgxapi->bobj_main, baton->p_mgxapi->oid_name); */
               bson_oid_gen(&(baton->p_mgxapi->jobj_main_list[jobj_no].oid));
               bson_oid_to_string(&(baton->p_mgxapi->jobj_main_list[jobj_no].oid), baton->p_mgxapi->jobj_main_list[jobj_no].oid_value);
               bson_append_oid(bobj, baton->p_mgxapi->jobj_main_list[jobj_no].oid_name, &(baton->p_mgxapi->jobj_main_list[jobj_no].oid));
            }
            if (!strcmp(name, baton->p_mgxapi->jobj_main_list[jobj_no].oid_name) && !MGX_GET(jobj, name_str)->IsString()) { /* _id but wrong type */
               /* bson_append_new_oid(baton->p_mgxapi->bobj_main, baton->p_mgxapi->oid_name); */
               bson_oid_gen(&(baton->p_mgxapi->jobj_main_list[jobj_no].oid));
               bson_oid_to_string(&(baton->p_mgxapi->jobj_main_list[jobj_no].oid), baton->p_mgxapi->jobj_main_list[jobj_no].oid_value);
               bson_append_oid(bobj, baton->p_mgxapi->jobj_main_list[jobj_no].oid_name, &(baton->p_mgxapi->jobj_main_list[jobj_no].oid));

               mgx_free((void *) name, 21);
               name = NULL;

               continue;
            }
         }

         if (MGX_GET(jobj, name_str)->IsArray()) {
            ret = bson_append_start_array(bobj, name);
            Local<Array> a = Local<Array>::Cast(MGX_GET(jobj, name_str));
            baton->p_mgxapi->level ++;
            mongox_parse_json_array(s, (mongo_baton_t *) baton, a, name, bobj, jobj_no, MGX_JSON_ARRAY, context);
            baton->p_mgxapi->level --;

            bson_append_finish_array(bobj);

         }
         else if (MGX_GET(jobj, name_str)->IsObject()) {

            jobj_next = MGX_TOOBJECT(MGX_GET(jobj, name_str));

            bobj_next = mgx_bson_alloc(baton->p_mgxapi, 1, 0);

            baton->p_mgxapi->level ++;
            mongox_parse_json_object(s, (mongo_baton_t *) baton, jobj_next, name, bobj_next, jobj_no, MGX_JSON_OBJECT, context);
            baton->p_mgxapi->level --;
            ret = bson_finish(bobj_next);

            ret = bson_append_bson(bobj, name, bobj_next);

         }
         else if (MGX_GET(jobj, name_str)->IsUint32()) {
            uint32_t uint32 = MGX_TOUINT32(MGX_GET(jobj, name_str));
            ret = bson_append_int(bobj, name, uint32);
         }
         else if (MGX_GET(jobj, name_str)->IsInt32()) {
            int32_t int32 = MGX_TOINT32(MGX_GET(jobj, name_str));
            ret = bson_append_int(bobj, name, int32);
         }
         else if (MGX_GET(jobj, name_str)->IsNumber()) {

            double num = MGX_TONUMBER(MGX_GET(jobj, name_str));

            ret = bson_append_double(bobj, name, num);
         }
         else {

            value_str = MGX_TOSTRING(MGX_GET(jobj, name_str));

            value_len = mongox_string8_length(isolate, value_str, 1);

            value = (char *) mgx_malloc(sizeof(char) * (value_len + 25), 2002);
            *value = '\0';

            mongox_write_char8(isolate, value_str, value, 1);

            if (!strcmp(name, baton->p_mgxapi->jobj_main_list[jobj_no].oid_name)) {

               ret = s->mongox_is_object_id(s, baton, value, &(baton->p_mgxapi->jobj_main_list[jobj_no].oid));
               if (ret) {
                  if (n == 0 && (baton->p_mgxapi->context == MGX_METHOD_INSERT || baton->p_mgxapi->context == MGX_METHOD_INSERT_BATCH) && baton->p_mgxapi->level == 0) { /* bad _id passed to insert */
                     /* bson_append_new_oid(baton->p_mgxapi->bobj_main, baton->p_mgxapi->oid_name); */
                     bson_oid_gen(&(baton->p_mgxapi->jobj_main_list[jobj_no].oid));
                     bson_oid_to_string(&(baton->p_mgxapi->jobj_main_list[jobj_no].oid), value);
                  }
               }
               strcpy(baton->p_mgxapi->jobj_main_list[jobj_no].oid_value, value);
               /* bson_oid_from_string(&(baton->p_mgxapi->oid), value); */
               ret = bson_append_oid(bobj, name, &(baton->p_mgxapi->jobj_main_list[jobj_no].oid));
            }
            else {
               ret = bson_append_string(bobj, name, value);
            }

            mgx_free((void *) value, 21);
            value = NULL;
         }

         mgx_free((void *) name, 21);
         name = NULL;

      }
      return 0;
   }


 static int mongox_parse_json_array(server *s, mongo_baton_t * baton, Local<Array> jarray, char *jobj_name, bson *bobj, int jobj_no, int type, int context)
   {
      Isolate* isolate = Isolate::GetCurrent();
#if MGX_NODE_VERSION >= 100000
      Local<Context> icontext = isolate->GetCurrentContext();
#endif
      HandleScope scope(isolate);
      unsigned int n, value_len, an;
      char *name, *value;
      char subs[32];
      Local<Array> a;
      Local<String> name_str;
      Local<String> value_str;
      Local<Object> jobj_next;
      bson *bobj_next;

      an = 0;

      a = Local<Array>::Cast(jarray);

      for (n = 0, an = 0; n < a->Length(); n ++, an ++) {

         name = subs;
         sprintf(name, "%d", an);

         if (MGX_GET(jarray, n)->IsArray()) {
            bson_append_start_array(bobj, name);

            Local<Array> a = Local<Array>::Cast(MGX_GET(jarray, n));
            baton->p_mgxapi->level ++;
            mongox_parse_json_object(s, (mongo_baton_t *) baton, a, name, bobj, jobj_no, MGX_JSON_ARRAY, context);
            baton->p_mgxapi->level --;

            bson_append_finish_array(bobj);

         }
         else if (MGX_GET(jarray, n)->IsObject()) {

            jobj_next = MGX_TOOBJECT(MGX_GET(jarray, n));

            bobj_next = mgx_bson_alloc(baton->p_mgxapi, 1, 0);

            baton->p_mgxapi->level ++;
            mongox_parse_json_object(s, (mongo_baton_t *) baton, jobj_next, name, bobj_next, jobj_no, MGX_JSON_OBJECT, context);
            baton->p_mgxapi->level --;
            bson_finish(bobj_next);

            bson_append_bson(bobj, name, bobj_next);

         }
         else if (MGX_GET(jarray, n)->IsUint32()) {
            uint32_t uint32 = MGX_TOUINT32(MGX_GET(jarray, n));

            bson_append_int(bobj, name, uint32);
         }
         else if (MGX_GET(jarray, n)->IsInt32()) {
            int32_t int32 = MGX_TOINT32(MGX_GET(jarray, n));

            bson_append_int(bobj, name, int32);
         }
         else if (MGX_GET(jarray, n)->IsNumber()) {
            double num = MGX_TONUMBER(MGX_GET(jarray, n));

            bson_append_double(bobj, name, num);
         }
         else {

            value_str = MGX_TOSTRING(MGX_GET(jarray, n));

            value_len = mongox_string8_length(isolate, value_str, 1);

            value = (char *) mgx_malloc(sizeof(char) * (value_len + 1), 2002);
            *value = '\0';

            mongox_write_char8(isolate, value_str, value, 1);

            bson_append_string(bobj, name, value);
         }
      }
      return 0;
   }



   static int mongox_parse_bson_object(server *s, mongo_baton_t * baton, Local<Object> jobj, bson *bobj, bson_iterator *iterator, int context)
   {
      Isolate* isolate = Isolate::GetCurrent();
#if MGX_NODE_VERSION >= 100000
      Local<Context> icontext = isolate->GetCurrentContext();
#endif
      EscapableHandleScope handle_scope(isolate);
      int int32;
      int64_t  int64;
      char *value, *key;
      char buffer[256];
      Local<String> key_str;
      Local<String> value_str;
      Local<Object> jobj_next;
      Local<Array> ja;
      bson_iterator iterator_a;
      bson_iterator iterator_o;
      bson_type type;

      if (bobj)
         bson_iterator_init(iterator, bobj);

      while ((type = bson_iterator_next(iterator))) {
         key = (char *) bson_iterator_key(iterator);
         if (type == BSON_OID) {

            bson_oid_to_string(bson_iterator_oid(iterator), buffer);

            key_str = mongox_new_string8(isolate, key, 1);
            value_str = mongox_new_string8(isolate, buffer, 1);
            MGX_SET(jobj, key_str, value_str);
         }
         else if (type == BSON_STRING) {
            value = (char *) bson_iterator_string(iterator);

            key_str = mongox_new_string8(isolate, key, 1);
            value_str = mongox_new_string8(isolate, value, 1);
            MGX_SET(jobj, key_str, value_str);
         }
         else if (type == BSON_INT) {
            int32 = (int) bson_iterator_int(iterator);

            key_str = mongox_new_string8(isolate, key, 1);

            MGX_SET(jobj, key_str, MGX_INTEGER_NEW(int32));
         }
         else if (type == BSON_LONG) {
            int64 = (int64_t) bson_iterator_long(iterator);

            key_str = mongox_new_string8(isolate, key, 1);

            MGX_SET(jobj, key_str, MGX_NUMBER_NEW((double ) int64));

         }
         else if (type == BSON_DOUBLE) {
            double num = (double) bson_iterator_double(iterator);

            key_str = mongox_new_string8(isolate, key, 1);

            MGX_SET(jobj, key_str, MGX_NUMBER_NEW(num));
         }
         else if (type == BSON_BOOL) {
            bson_bool_t num = (bson_bool_t) bson_iterator_bool(iterator);

            key_str = mongox_new_string8(isolate, key, 1);

            MGX_SET(jobj, key_str, MGX_BOOLEAN_NEW(num ? true : false));
         }
         else if (type == BSON_NULL) {
/*
            bson_bool_t num = (bson_bool_t) bson_iterator_bool(iterator);
*/
            key_str = mongox_new_string8(isolate, key, 1);

            MGX_SET(jobj, key_str, MGX_NULL());
         }
         else if (type == BSON_DATE) {

            bson_date_t num = (bson_date_t) bson_iterator_date(iterator);

            key_str = mongox_new_string8(isolate, key, 1);

            MGX_SET(jobj, key_str, MGX_DATE((double) num));
         }
         else if (type == BSON_ARRAY) {
            ja = MGX_ARRAY_NEW(0);

            key_str = mongox_new_string8(isolate, key, 1);
            MGX_SET(jobj, key_str, ja);

            bson_iterator_subiterator(iterator, &iterator_a);

            mongox_parse_bson_array(s, baton, ja, key, NULL, &iterator_a, context);
         }
         else if (type == BSON_OBJECT) {
            bson_iterator_subiterator(iterator, &iterator_o);
            jobj_next = MGX_OBJECT_NEW();

            key_str = mongox_new_string8(isolate, key, 1);
            MGX_SET(jobj, key_str, jobj_next);

            mongox_parse_bson_object(s, baton, jobj_next, (bson *) NULL, &iterator_o, 1);
         }
         else {
            sprintf(buffer, "BSON Type: %d", type);
            key_str = mongox_new_string8(isolate, key, 1);
            value_str = mongox_new_string8(isolate, buffer, 1);
            MGX_SET(jobj, key_str, value_str);

         }
      }

      return 1;

   }


   static int mongox_parse_bson_array(server *s, mongo_baton_t * baton, Local<Array> jarray, char *jobj_name, bson *bobj, bson_iterator *iterator, int context)
   {
      Isolate* isolate = Isolate::GetCurrent();
#if MGX_NODE_VERSION >= 100000
      Local<Context> icontext = isolate->GetCurrentContext();
#endif
      EscapableHandleScope handle_scope(isolate);
      int int32;
      int64_t  int64;
      unsigned int an;
      char *value;
      char buffer[256];
      Local<String> key_str;
      Local<String> value_str;
      Local<Object> jobj_next;
      Local<Array> ja;
      bson_iterator iterator_a;
      bson_iterator iterator_o;
      bson_type type;

      an = 0;
      while ((type = bson_iterator_next(iterator))) {

         if (type == BSON_OID) {
            bson_oid_to_string(bson_iterator_oid(iterator), buffer);
            value_str = mongox_new_string8(isolate, buffer, 1);
            MGX_SET(jarray, an, value_str);
         }
         else if (type == BSON_STRING) {
            value = (char *) bson_iterator_string(iterator);

            value_str = mongox_new_string8(isolate, value, 1);
            MGX_SET(jarray, an, value_str);
         }
         else if (type == BSON_INT) {
            int32 = (int) bson_iterator_int(iterator);

            MGX_SET(jarray, an, MGX_INTEGER_NEW(int32));
         }
         else if (type == BSON_LONG) {
            int64 = (int64_t) bson_iterator_long(iterator);

            MGX_SET(jarray, an, MGX_NUMBER_NEW((double) int64));
         }
         else if (type == BSON_DOUBLE) {
            double num = (double) bson_iterator_double(iterator);

            MGX_SET(jarray, an, MGX_NUMBER_NEW(num));
         }
         else if (type == BSON_BOOL) {
            bson_bool_t num = (bson_bool_t) bson_iterator_bool(iterator);

            MGX_SET(jarray, an, MGX_BOOLEAN_NEW(num ? true : false));
         }
         else if (type == BSON_NULL) {
/*
            bson_bool_t num = (bson_bool_t) bson_iterator_bool(iterator);
*/
            MGX_SET(jarray, an, MGX_NULL());
         }
         else if (type == BSON_DATE) {
            bson_date_t num = (bson_date_t) bson_iterator_date(iterator);

            MGX_SET(jarray, an, MGX_DATE((double) num));
         }
         else if (type == BSON_ARRAY) {
            ja = MGX_ARRAY_NEW(0);
            MGX_SET(jarray, an, ja);

            bson_iterator_subiterator(iterator, &iterator_a);

            mongox_parse_bson_array(s, baton, ja, jobj_name, NULL, &iterator_a, context);

         }
         else if (type == BSON_OBJECT) {
            bson_iterator_subiterator(iterator, &iterator_o);
            jobj_next = MGX_OBJECT_NEW();

            MGX_SET(jarray, an, jobj_next);

            mongox_parse_bson_object(s, baton, jobj_next, (bson *) NULL, &iterator_o, 1);
         }
         else {
            sprintf(buffer, "BSON Type: %d", type);
            value_str = mongox_new_string8(isolate, buffer, 1);
            MGX_SET(jarray, an, value_str);
         }
         an ++;
      }
      return 1;
   }


   static int mongox_destroy_baton(mongo_baton_t *baton)
   {

      mgx_bson_free(baton->p_mgxapi);

      if (baton->p_mgxapi) {

         if (baton->p_mgxapi->output) {
            mgx_free((void *) baton->p_mgxapi->output, 706);
         }
         if (baton->p_mgxapi->bobj_main_list) {
            mgx_free((void *) baton->p_mgxapi->bobj_main_list, 706);
         }
         if (baton->p_mgxapi->jobj_main_list) {
            mgx_free((void *) baton->p_mgxapi->jobj_main_list, 706);
         }

         mgx_free((void *) baton->p_mgxapi, 706);
      }

      delete baton;

      return 0;
   }


   static int mongox_string8_length(Isolate * isolate, Local<String> str, int utf8)
   {
      if (utf8) {
#if MGX_NODE_VERSION >= 120000
         return str->Utf8Length(isolate);
#else
         return str->Utf8Length();
#endif
      }
      else {
         return str->Length();
      }
   }


   static Local<String> mongox_new_string8(Isolate * isolate, char * buffer, int utf8)
   {
      if (utf8) {
#if MGX_NODE_VERSION >= 120000
         return String::NewFromUtf8(isolate, buffer, NewStringType::kNormal).ToLocalChecked();
#elif MGX_NODE_VERSION >= 1200
         return String::NewFromUtf8(isolate, buffer);
#else
         return String::NewFromUtf8(buffer);
#endif
      }
      else {
#if MGX_NODE_VERSION >= 100000
         return String::NewFromOneByte(isolate, (uint8_t *) buffer, NewStringType::kInternalized).ToLocalChecked();
#elif MGX_NODE_VERSION >= 1200
         return String::NewFromOneByte(isolate, (uint8_t *) buffer);
#else
         return String::New(buffer);
#endif
      }
   }


   static Local<String> mongox_new_string8n(Isolate * isolate, char * buffer, unsigned long len, int utf8)
   {
      if (utf8) {
#if MGX_NODE_VERSION >= 120000
         return String::NewFromUtf8(isolate, buffer, NewStringType::kNormal, len).ToLocalChecked();
#elif MGX_NODE_VERSION >= 1200
         return String::NewFromUtf8(isolate, buffer, String::kNormalString, len);
#else
         return String::NewFromUtf8(buffer, len);
#endif
      }
      else {
#if MGX_NODE_VERSION >= 100000
         return String::NewFromOneByte(isolate, (uint8_t *) buffer, NewStringType::kInternalized, len).ToLocalChecked();
#elif MGX_NODE_VERSION >= 1200
         return String::NewFromOneByte(isolate, (uint8_t *) buffer, String::kNormalString, len);
#else
         return String::New(buffer);
#endif
      }
   }


   static int mongox_write_char8(v8::Isolate * isolate, Local<String> str, char * buffer, int utf8)
   {
      if (utf8) {
#if MGX_NODE_VERSION >= 120000
         return str->WriteUtf8(isolate, buffer);
#else
         return str->WriteUtf8(buffer);
#endif
      }
      else {
#if MGX_NODE_VERSION >= 120000
         return str->WriteOneByte(isolate, (uint8_t *) buffer);
#elif MGX_NODE_VERSION >= 1200
         return str->WriteOneByte((uint8_t *) buffer);
#else
         return str->WriteAscii((char *) buffer);
#endif
      }
   }


   static void mongox_invoke_callback(uv_work_t *req, int status)
   {
      Isolate* isolate = Isolate::GetCurrent();
      HandleScope scope(isolate);
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);
      /* ev_unref(EV_DEFAULT_UC); */
      baton->s->Unref();

      Local<Value> argv[2];

      baton->json_result = mongox_result_object(baton, 1);

      if (baton->result_iserror)
         argv[0] = MGX_INTEGER_NEW(true);
      else
         argv[0] = MGX_INTEGER_NEW(false);

      if (baton->result_isarray)
         argv[1] = baton->array_result;
      else
         argv[1] = baton->json_result;

#if MGX_NODE_VERSION >= 40000
      TryCatch try_catch(isolate);
#else
      TryCatch try_catch;
#endif

      Local<Function> cb = Local<Function>::New(isolate, baton->cb);

#if MGX_NODE_VERSION >= 120000
      /* cb->Call(isolate->GetCurrentContext(), isolate->GetCurrentContext()->Global(), 2, argv); */
      cb->Call(isolate->GetCurrentContext(), Null(isolate), 2, argv).ToLocalChecked();
#else
      cb->Call(isolate->GetCurrentContext()->Global(), 2, argv);
#endif


#if MGX_NODE_VERSION >= 40000
      if (try_catch.HasCaught()) {
         FatalException(isolate, try_catch);
      }
#else
      if (try_catch.HasCaught()) {
         FatalException(isolate, try_catch);
      }
#endif

      baton->cb.Reset();

	   MGX_MONGOAPI_END();

      mongox_destroy_baton(baton);

      delete req;
      return;
   }

 
   static Local<Object> mongox_result_object(mongo_baton_t * baton, int context)
   {
      Isolate* isolate = Isolate::GetCurrent();
#if MGX_NODE_VERSION >= 100000
      Local<Context> icontext = isolate->GetCurrentContext();
#endif
      EscapableHandleScope handle_scope(isolate);
      int ret, n;
      Local<String> key;
      Local<String> value;
      Local<String> error;
      Local<Object> jobj;
      Local<Array> jobj_array;

      baton->result_iserror = 0;
      baton->result_isarray = 0;

      baton->json_result = MGX_OBJECT_NEW();

      if (baton->p_mgxapi->error[0]) {

         baton->result_iserror = 1;

         error = mongox_new_string8(isolate, baton->p_mgxapi->error, 1);
         key = mongox_new_string8(isolate, (char *) "ErrorMessage", 1);
         MGX_SET(baton->json_result, key, error);

         key = mongox_new_string8(isolate, (char *) "ErrorCode", 1);
         MGX_SET(baton->json_result, key, MGX_INTEGER_NEW(baton->p_mgxapi->error_code));

         key = mongox_new_string8(isolate, (char *) "ok", 1);
         MGX_SET(baton->json_result, key, MGX_INTEGER_NEW(false));
      }
      else {
         if (baton->p_mgxapi->context == MGX_METHOD_RETRIEVE) {
            int an;
            bson *bobj;
            bson_iterator iterator;
            Local<Object> jobj;
            Local<Array> a_subs = MGX_ARRAY_NEW(0);

            key = mongox_new_string8(isolate, (char *) "ok", 1);
            MGX_SET(baton->json_result, key, MGX_INTEGER_NEW(true));

            key = mongox_new_string8(isolate, (char *) "data", 1);
            MGX_SET(baton->json_result, key, a_subs);

            an = 0;

            while ((ret = mongo_cursor_next(baton->p_mgxapi->cursor)) == MONGO_OK ) {
               /* bson_print(&(cursor->current)); */

               bobj = (bson *) mongo_cursor_bson(baton->p_mgxapi->cursor);

               jobj = MGX_OBJECT_NEW();

               ret = mongox_parse_bson_object(baton->s, baton, jobj, bobj, &iterator, 0);

               MGX_SET(a_subs, an, jobj);

               an ++;

            }
            mongo_cursor_destroy(baton->p_mgxapi->cursor);
         }
         else if (baton->p_mgxapi->context == MGX_METHOD_COMMAND || baton->p_mgxapi->context == MGX_METHOD_CREATE_INDEX) {
            bson_iterator iterator;
            Local<Object> jobj;

            jobj = MGX_OBJECT_NEW();

            key = mongox_new_string8(isolate, (char *) "ok", 1);
            MGX_SET(baton->json_result, key, MGX_INTEGER_NEW(true));

            key = mongox_new_string8(isolate, (char *) "data", 1);

            ret = mongox_parse_bson_object(baton->s, baton, jobj, baton->p_mgxapi->bobj_main, &iterator, 0);
            MGX_SET(baton->json_result, key, jobj);
         }
         else {
            key = mongox_new_string8(isolate, (char *) "ok", 1);
            MGX_SET(baton->json_result, key, MGX_INTEGER_NEW(true));

            if (baton->p_mgxapi->context == MGX_METHOD_VERSION || baton->p_mgxapi->context == MGX_METHOD_ABOUT || baton->p_mgxapi->context == MGX_METHOD_OBJECT_ID) {
               key = mongox_new_string8(isolate, (char *) "result", 1);
               if (baton->p_mgxapi->output)
                  value = mongox_new_string8(isolate, baton->p_mgxapi->output, 1);
               else
                  value = mongox_new_string8(isolate, (char *) "", 1);
               MGX_SET(baton->json_result, key, value);
            }
            else {
               key = mongox_new_string8(isolate, (char *) "result", 1);
               MGX_SET(baton->json_result, key, MGX_INTEGER_NEW(baton->p_mgxapi->output_integer));
               if (baton->p_mgxapi->context == MGX_METHOD_INSERT) {
                  key = mongox_new_string8(isolate, baton->p_mgxapi->jobj_main_list[0].oid_name, 1);
                  value = mongox_new_string8(isolate, baton->p_mgxapi->jobj_main_list[0].oid_value, 1);
                  MGX_SET(baton->json_result, key, value);
               }
               else if (baton->p_mgxapi->context == MGX_METHOD_INSERT_BATCH) {
                  jobj_array = MGX_ARRAY_NEW(0);
                  key = mongox_new_string8(isolate, (char *) "data", 1);
                  MGX_SET(baton->json_result, key, jobj_array);
                  for (n = 0; n < baton->p_mgxapi->bobj_main_list_no; n ++) {
                     jobj = MGX_OBJECT_NEW();
                     MGX_SET(jobj_array, n, jobj);
                     key = mongox_new_string8(isolate, baton->p_mgxapi->jobj_main_list[n].oid_name, 1);
                     value = mongox_new_string8(isolate, baton->p_mgxapi->jobj_main_list[n].oid_value, 1);
                     MGX_SET(jobj, key, value);
                  }
               }
               else if (baton->p_mgxapi->context == MGX_METHOD_OBJECT_ID_DATE) {
                  key = mongox_new_string8(isolate, (char *) "DateText", 1);
                  value = mongox_new_string8(isolate, baton->p_mgxapi->output, 1);
                  MGX_SET(baton->json_result, key, value);
               }
            }
         }
      }

      if (baton->result_isarray) {
         return handle_scope.Escape(baton->array_result);
      }
      else {
         return handle_scope.Escape(baton->json_result);
      }
   }


   static void About(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_ABOUT);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_About, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_mgx_version(s, baton);

      Local<String> result = mongox_new_string8(isolate, baton->p_mgxapi->output, 1);
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(result);
   }


   static void EIO_About(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_mgx_version(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Version(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      server *s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_VERSION);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Version, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_mgx_version(s, baton);

      Local<String> result = mongox_new_string8(isolate, baton->p_mgxapi->output, 1);
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(result);
   }


   static void EIO_Version(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_mgx_version(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Open(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int ret, js_narg;
      Local<Object> json_result;
      server *s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_OPEN);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Open, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      ret = s->mongox_open(s, baton);

      if (ret == MONGO_OK) {
         s->open = 1; 
      }
      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Open(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_open(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Close(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server *s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_MONGOAPI_START();

      s->open = 0;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_CLOSE);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Close, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_close(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Close(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_close(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Retrieve(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server *s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_RETRIEVE);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Retrieve, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_retrieve(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Retrieve(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_retrieve(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Insert(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_INSERT);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Insert, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_insert(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Insert(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_insert(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Insert_Batch(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_INSERT_BATCH);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Insert_Batch, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_insert_batch(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }

   static void EIO_Insert_Batch(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_insert_batch(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Update(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_UPDATE);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Update, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_update(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Update(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_update(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Remove(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_REMOVE);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Remove, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_remove(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Remove(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_remove(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Command(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_COMMAND);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Command, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_command(s, baton);

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Command(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_command(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Create_Index(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_CREATE_INDEX);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Create_Index, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_create_index(s, baton);

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Create_Index(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_create_index(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Object_ID(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_OBJECT_ID);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Object_ID, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_object_id(s, baton);

      MGX_MONGOAPI_END();

      Local<String> result = mongox_new_string8(isolate, baton->p_mgxapi->output, 1);
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(result);
/*
      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
*/
   }


   static void EIO_Object_ID(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_object_id(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }


   static void Object_ID_Date(const FunctionCallbackInfo<Value>& args)
   {
      Isolate* isolate = args.GetIsolate();
      HandleScope scope(isolate);
      short async;
      int js_narg;
      Local<Object> json_result;
      server * s = ObjectWrap::Unwrap<server>(args.This());
      s->m_count ++;

      MGX_CALLBACK_FUN(js_narg, cb, async);

      mongo_baton_t *baton = mongox_make_baton(s, js_narg, args, MGX_METHOD_OBJECT_ID_DATE);
      MGX_MONGOAPI_ERROR();

      baton->s = s;

      MGX_MONGOAPI_START();

      if (async) {

         Local<Function> cb = Local<Function>::Cast(args[js_narg]);
         baton->isolate = isolate;
         baton->cb.Reset(isolate, cb);

         s->Ref();

         mongox_queue_task((void *) EIO_Object_ID_Date, (void *) mongox_invoke_callback, baton, 0); /* v1.4.14 */

         return;
      }

      s->mongox_object_id_date(s, baton);

      MGX_MONGOAPI_END();

      baton->json_result = mongox_result_object(baton, 0);
      json_result = baton->json_result;
      mongox_destroy_baton(baton);

      MGX_RETURN_VALUE(json_result);
   }


   static void EIO_Object_ID_Date(uv_work_t *req)
   {
      mongo_baton_t *baton = static_cast<mongo_baton_t *>(req->data);

      baton->s->mongox_object_id_date(baton->s, baton);

      baton->s->m_count += baton->increment_by;

      return;
   }

};


/* v1.4.13 */
#if MGX_NODE_VERSION >= 120000
class mgx_addon_data
{

public:

   mgx_addon_data(Isolate* isolate, Local<Object> exports):
      call_count(0) {
         /* Link the existence of this object instance to the existence of exports. */
         exports_.Reset(isolate, exports);
         exports_.SetWeak(this, DeleteMe, WeakCallbackType::kParameter);
      }

   ~mgx_addon_data() {
      if (!exports_.IsEmpty()) {
         /* Reset the reference to avoid leaking data. */
         exports_.ClearWeak();
         exports_.Reset();
      }
   }

   /* Per-addon data. */
   int call_count;

private:

   /* Method to call when "exports" is about to be garbage-collected. */
   static void DeleteMe(const WeakCallbackInfo<mgx_addon_data>& info) {
      delete info.GetParameter();
   }

   /*
   Weak handle to the "exports" object. An instance of this class will be
   destroyed along with the exports object to which it is weakly bound.
   */
   v8::Persistent<v8::Object> exports_;
};
#endif


Persistent<Function> server::s_ct;


extern "C" {
#if defined(_WIN32)
#if MGX_NODE_VERSION >= 100000
void __declspec(dllexport) init (Local<Object> exports)
#else
void __declspec(dllexport) init (Handle<Object> exports)
#endif
#else
#if MGX_NODE_VERSION >= 100000
static void init (Local<Object> exports)
#else
static void init (Handle<Object> exports)
#endif
#endif
{
   server::Init(exports);
}

#if MGX_NODE_VERSION >= 120000

/* exports, module, context */
extern "C" NODE_MODULE_EXPORT void
NODE_MODULE_INITIALIZER(Local<Object> exports,
                        Local<Value> module,
                        Local<Context> context) {
   Isolate* isolate = context->GetIsolate();

   /* Create a new instance of mgx_addon_data for this instance of the addon. */
   mgx_addon_data * data = new mgx_addon_data(isolate, exports);
   /* Wrap the data in a v8::External so we can pass it to the method we expose. */
   /* Local<External> external = External::New(isolate, data); */
   External::New(isolate, data);

   init(exports);

   /*
   Expose the method "Method" to JavaScript, and make sure it receives the
   per-addon-instance data we created above by passing `external` as the
   third parameter to the FunctionTemplate constructor.
   exports->Set(context, String::NewFromUtf8(isolate, "method", NewStringType::kNormal).ToLocalChecked(), FunctionTemplate::New(isolate, Method, external)->GetFunction(context).ToLocalChecked()).FromJust();
   */

}

#else

   NODE_MODULE(server, init);

#endif
}


void * mgx_malloc(int size, short id)
{
   void *p;

   p = (void *) malloc(size);

   /* printf("\r\nmgx_malloc: size=%d; id=%d; p=%p;", size, id, p); */

   return p;
}


int mgx_free(void *p, short id)
{
   /* printf("\r\nmgx_free: id=%d; p=%p;", id, p); */

   free((void *) p);

   return 0;
}


bson * mgx_bson_alloc(MGXAPI * p_mgxapi, int init, short id)
{
   bson *bson;
   MGXBSON *p_mgxbson;

   bson = NULL;
   p_mgxbson = (MGXBSON *) mgx_malloc(sizeof(MGXBSON), id);

   if (p_mgxbson) {
      p_mgxbson->bobj = bson_alloc();

      /* printf("\r\n +++++++ mgx_bson_alloc: p_mgxbson=%p; p_mgxbson->bson=%p;", p_mgxbson, p_mgxbson->bson); */

      if (p_mgxbson->bobj) {
         if (init) {
            bson_init(p_mgxbson->bobj);
         }
         p_mgxbson->id = id;
         p_mgxbson->p_next = NULL;
         if (p_mgxapi->p_mgxbson_tail) {
            p_mgxapi->p_mgxbson_tail->p_next = p_mgxbson;
            p_mgxapi->p_mgxbson_tail = p_mgxbson;
         }
         else {
            p_mgxapi->p_mgxbson_head = p_mgxbson;
            p_mgxapi->p_mgxbson_tail = p_mgxbson;
         }
         bson = p_mgxbson->bobj;
      }
      else {
         mgx_free((void *) p_mgxbson, id);
      }
   }
   return bson;
}


int mgx_bson_free(MGXAPI * p_mgxapi)
{
   MGXBSON *p_mgxbson, *p_mgxbson_next;

   p_mgxbson = p_mgxapi->p_mgxbson_head;
   while (p_mgxbson) {

      /* printf("\r\n ------- mgx_bson_free : p_mgxbson=%p; p_mgxbson->bson=%p;", p_mgxbson, p_mgxbson->bson); */

      p_mgxbson_next = p_mgxbson->p_next;
      bson_destroy(p_mgxbson->bobj);
      bson_free((void *) p_mgxbson->bobj);
      mgx_free((void *) p_mgxbson, p_mgxbson->id);
      p_mgxbson = p_mgxbson_next;
   }

   p_mgxapi->p_mgxbson_head = NULL;
   p_mgxapi->p_mgxbson_tail = NULL;

   return 0;
}


int mgx_ucase(char *string)
{
#ifdef _UNICODE

   CharUpperA(string);
   return 1;

#else

   int n, chr;

   n = 0;
   while (string[n] != '\0') {
      chr = (int) string[n];
      if (chr >= 97 && chr <= 122)
         string[n] = (char) (chr - 32);
      n ++;
   }
   return 1;

#endif
}


int mgx_lcase(char *string)
{
#ifdef _UNICODE

   CharLowerA(string);
   return 1;

#else

   int n, chr;

   n = 0;
   while (string[n] != '\0') {
      chr = (int) string[n];
      if (chr >= 65 && chr <= 90)
         string[n] = (char) (chr + 32);
      n ++;
   }
   return 1;

#endif
}


int mgx_buffer_dump(char *buffer, unsigned int len, short mode)
{
   unsigned int n;
   char c;

   printf("\nbuffer dump (size=%d)...\n", len);
   for (n = 0; n < len; n ++) {
      c = buffer[n];
      if (((int) c) < 32 || ((int) c) > 126)
         printf("\\x%02x", (int) c);
      else
         printf("%c", (int) c);

   }

   return 0;
}


MGXPLIB mgx_dso_load(char * library)
{
   MGXPLIB p_library;

#if defined(_WIN32)
   p_library = LoadLibraryA(library);
#else
   p_library = dlopen(library, RTLD_NOW);
#endif

   return p_library;
}


MGXPROC mgx_dso_sym(MGXPLIB p_library, char * symbol)
{
   MGXPROC p_proc;

#if defined(_WIN32)
   p_proc = GetProcAddress(p_library, symbol);
#else
   p_proc  = (void *) dlsym(p_library, symbol);
#endif

   return p_proc;
}


int mgx_dso_unload(MGXPLIB p_library)
{

#if defined(_WIN32)
   FreeLibrary(p_library);
#else
   dlclose(p_library); 
#endif

   return 1;
}

