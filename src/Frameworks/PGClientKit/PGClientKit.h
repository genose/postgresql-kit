
// Copyright 2009-2015 David Thorpe
// https://github.com/djthorpe/postgresql-kit
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

#import <Foundation/Foundation.h>

typedef struct __CFRuntimeBase {
    uintptr_t _cfisa;
    uint8_t _cfinfo[4];
#if __LP64__
    uint32_t _rc;
#endif
} CFRuntimeBase;


struct __shared_blob {
    __unsafe_unretained dispatch_source_t _rdsrc;
    __unsafe_unretained dispatch_source_t _wrsrc;
    __unsafe_unretained CFRunLoopSourceRef _source;
    __unsafe_unretained CFSocketNativeHandle _socket;
    uint8_t _closeFD;
    uint8_t _refCnt;
};

struct __CFSocket {
    __unsafe_unretained CFRuntimeBase _base;
    __unsafe_unretained struct __shared_blob *_shared; // non-NULL when valid, NULL when invalid
    
    uint8_t _state:2;         // mutable, not written safely
    uint8_t _isSaneFD:1;      // immutable
    uint8_t _connOriented:1;  // immutable
    uint8_t _wantConnect:1;   // immutable
    uint8_t _wantWrite:1;     // immutable
    uint8_t _wantReadType:2;  // immutable
    
    uint8_t _error;
    
    uint8_t _rsuspended:1;
    uint8_t _wsuspended:1;
    uint8_t _readable:1;
    uint8_t _writeable:1;
    uint8_t _unused:4;
    
    uint8_t _reenableRead:1;
    uint8_t _readDisabled:1;
    uint8_t _reenableWrite:1;
    uint8_t _writeDisabled:1;
    uint8_t _connectDisabled:1;
    uint8_t _connected:1;
    uint8_t _leaveErrors:1;
    uint8_t _closeOnInvalidate:1;
    
    int32_t _runLoopCounter;
    
    CFDataRef _address;         // immutable, once created
    CFDataRef _peerAddress;     // immutable, once created
    CFSocketCallBack _callout;  // immutable
    CFSocketContext _context;   // immutable
};


////////////////////////////////////////////////////////////////////////////////

// typedefs
typedef enum {
	PGConnectionStatusDisconnected = 0, // not connected
	PGConnectionStatusConnected = 1,    // connected and idle
	PGConnectionStatusRejected = 2,     // not connected, rejected connection
	PGConnectionStatusConnecting = 3,   // busy connecting
	PGConnectionStatusBusy = 4          // connected and busy
} PGConnectionStatus;

typedef enum {
	PGClientErrorNone = 0,                // no error occured
	PGClientErrorState = 100,             // state is wrong for this call
	PGClientErrorParameters = 101,        // invalid parameters
	PGClientErrorNeedsPassword = 102,     // password required
	PGClientErrorInvalidPassword = 103,   // password failure
	PGClientErrorRejected = 104,          // rejected from operation
	PGClientErrorExecute = 105,           // execution error
	PGClientErrorQuery = 106,             // invalid query
	PGClientErrorUnknown = 107            // unknown error
} PGClientErrorDomainCode;

////////////////////////////////////////////////////////////////////////////////

// forward class declarations
@class PGConnection;
@class PGConnectionPool;
@class PGPasswordStore;

@class PGResult;

@class PGQueryObject;
	@class PGQuery;
		@class PGQuerySelect;
		@class PGQueryInsert;
		@class PGQueryUpdate;
		@class PGQueryDelete;
		@class PGQueryDatabase;
		@class PGQueryRole;
		@class PGQuerySchema;
		@class PGQueryTableView;
	@class PGQuerySource;
	@class PGQueryPredicate;

@class PGTransaction;

// connections
#import "PGConnection.h"
#import "PGConnectionPool.h"

// queries callback pool
#import "PGConnectionOperation.h"

// queries
#import "PGQueryObject.h"
#import "PGQuery.h"
#import "PGQuerySelect.h"
#import "PGQueryInsert.h"
#import "PGQueryDelete.h"
#import "PGQueryUpdate.h"
#import "PGQueryDatabase.h"
#import "PGQueryRole.h"
#import "PGQuerySchema.h"
#import "PGQueryTableView.h"
#import "PGQuerySource.h"
#import "PGQueryPredicate.h"

// transactions
#import "PGTransaction.h"

// results
#import "PGResult.h"

// helpers
#import "NSURL+PGAdditions.h"
#import "NSError+PGAdditions.h"
#import "NSString+PGNetworkValidationAdditions.h"
#import "PGPasswordStore.h"

#if TARGET_OS_IPHONE
// Do not import additional header files
#else
// Import Mac OS X specific header files
#import "PGResult+TextTable.h"
#endif
