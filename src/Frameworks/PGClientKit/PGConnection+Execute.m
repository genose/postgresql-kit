
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

// *************************************
//
// Copyright 2017 - ?? Sebastien Cotillard - Genose.org
// 07/2017 Sebastien Cotillard
// https://github.com/genose
//
// *************************************
// ADDING Pool concurrent operation
// *************************************

#import <PGClientKit/PGClientKit.h>
#import <PGClientKit/PGClientKit+Private.h>

#import <objc/runtime.h>
#import <dlfcn.h>
#import <sys/ioctl.h>



#define INVALID_SOCKET (CFSocketNativeHandle)(-1)

#define closesocket(a) close((a))
#define ioctlsocket(a,b,c) ioctl((a),(b),(c))

CF_INLINE Boolean __CFSocketIsValid(CFSocketRef s);

static CFStringRef __CFFSocketCopyDescription(CFTypeRef cf) {
    CFSocketRef sock = (CFSocketRef)cf;
    CFStringRef contextDesc = NULL;
    if (NULL != sock->_context.info && NULL != sock->_context.copyDescription) {
        contextDesc = sock->_context.copyDescription(sock->_context.info);
    }
    if (NULL == contextDesc) {
        contextDesc = CFStringCreateWithFormat(kCFAllocatorSystemDefault, NULL, CFSTR("<CFSocket context %p>"), sock->_context.info);
    }
    Dl_info info;
    void *addr = sock->_callout;
    const char *name = (dladdr(addr, &info) && info.dli_saddr == addr && info.dli_sname) ? info.dli_sname : "???";
    int avail = -1;
    ioctlsocket(sock->_shared ? sock->_shared->_socket : -1, FIONREAD, &avail);
    CFStringRef result = CFStringCreateWithFormat(kCFAllocatorSystemDefault, NULL, CFSTR(
                                                                                         "<CFSocket %p [%p]>{valid = %s, socket = %d, "
                                                                                         "want connect = %s, connect disabled = %s, "
                                                                                         "want write = %s, reenable write = %s, write disabled = %s, "
                                                                                         "want read = %s, reenable read = %s, read disabled = %s, "
                                                                                         "leave errors = %s, close on invalidate = %s, connected = %s, "
                                                                                         "last error code = %d, bytes available for read = %d, "
                                                                                         "source = %p, callout = %s (%p), context = %@}"),
                                                  cf, CFGetAllocator(sock), (sock) ? "Yes" : "No", sock->_shared ? sock->_shared->_socket : -1,
                                                  sock->_wantConnect ? "Yes" : "No", sock->_connectDisabled ? "Yes" : "No",
                                                  sock->_wantWrite ? "Yes" : "No", sock->_reenableWrite ? "Yes" : "No", sock->_writeDisabled ? "Yes" : "No",
                                                  sock->_wantReadType ? "Yes" : "No", sock->_reenableRead ? "Yes" : "No", sock->_readDisabled? "Yes" : "No",
                                                  sock->_leaveErrors ? "Yes" : "No", sock->_closeOnInvalidate ? "Yes" : "No", sock->_connected ? "Yes" : "No",
                                                  sock->_error, avail,
                                                  sock->_shared ? sock->_shared->_source : NULL, name, addr, contextDesc);
    if (NULL != contextDesc) {
        CFRelease(contextDesc);
    }
    return result;
}
@implementation PGConnection (Execute)

////////////////////////////////////////////////////////////////////////////////
#pragma mark private methods - statement execution
////////////////////////////////////////////////////////////////////////////////



-(void)_execute:(NSString* )query values:(NSArray* )values whenDone:(void(^)(PGResult* result,NSError* error)) callback {
    
    
    NSParameterAssert(query && [query isKindOfClass:[NSString class]]);
    if(_connection==nil || [self state] != PGConnectionStateNone) {
        callback(nil,[self raiseError:nil code:PGClientErrorState]);
        return;
    }
    
    // create parameters object
    PGClientParams* params = _paramAllocForValues(values);
    if(params==nil) {
        callback(nil,[self raiseError:nil code:PGClientErrorParameters]);
        return;
    }
    // convert parameters
    for(NSUInteger i = 0; i < [values count]; i++) {
        id obj = [values objectAtIndex:i];
        if([obj isKindOfClass:[NSNull class]]) {
            _paramSetNull(params,i);
            continue;
        }
        if([obj isKindOfClass:[NSString class]]) {
            NSData* data = [(NSString* )obj dataUsingEncoding:NSUTF8StringEncoding];
            _paramSetBinary(params,i,data,(Oid)25);
            continue;
        }
        // TODO - other kinds of parameters
        NSLog(@"TODO: Turn %@ into arg",[obj class]);
        _paramSetNull(params,i);
    }
    
    
    // check number of parameters
    if(params->size > INT_MAX) {
        _paramFree(params);
        callback(nil,[self raiseError:nil code:PGClientErrorParameters]);
        return;
    }
    
    // call the delegate, determine the class to use for the resultset
    NSString* className = nil;
    if([[self delegate] respondsToSelector:@selector(connection:willExecute:)]) {
        className = [[self delegate] connection:self willExecute:query];
    }
    
    
    // set state, update status
    [self setState:PGConnectionStateQuery];
    
    [self _updateStatus];
    
    // execute the command, free parameters
    int resultFormat = ([self tupleFormat]==PGClientTupleFormatBinary) ? 1 : 0;
    int returnCode = PQsendQueryParams(_connection,[query UTF8String],(int)params->size,params->types,(const char** )params->values,params->lengths,params->formats,resultFormat);
    _paramFree(params);
    if(!returnCode) {
#if defined DEBUG && defined DEBUG2
        NSLog(@"%@ :: %@ :: - execute - ERROR :: callback %p :: While EXECUTE (%@)", NSStringFromSelector(_cmd),NSStringFromClass([self class]), callback, query);
#endif
        callback(nil,[self raiseError:nil code:PGClientErrorExecute reason:[NSString stringWithUTF8String:PQerrorMessage(_connection)]]);
        return;
    }else{
#if defined DEBUG && defined DEBUG2
        NSLog(@"%@ :: %@ :: - execute - PASSED :: callback %p :: While EXECUTE (%@)", NSStringFromSelector(_cmd),NSStringFromClass([self class]), callback, query);
#endif
    }
    
    //	// set state, update status
    //	[self setState:PGConnectionStateQuery];
    //
    //	[self _updateStatus];
    
    
#if defined DEBUG && defined DEBUG2
    NSLog(@"%@ :: %@ :: - execute - RETURN :: callback %p :: %p ", NSStringFromSelector(_cmd),NSStringFromClass([self class]), callback, _callbackOperation);
#endif
    NSParameterAssert(callback!=nil);
    
    _callbackOperation = (__bridge_retained void* )[callback copy];
    
    //   if([self currentPoolOperation] != nil) [[self currentPoolOperation] invalidate];
    
    [self addOperation:query withCallBackWhenDone: (__bridge_retained void* )callback withCallBackWhenError: (__bridge_retained void* )callback ];
    
    NSParameterAssert(_callbackOperation!=nil);
    
    [NSThread sleepForTimeInterval: .2];
}

////////////////////////////////////////////////////////////////////////////////
#pragma mark public methods - execution
////////////////////////////////////////////////////////////////////////////////

-(void)execute:(id)query whenDone:(void(^)(PGResult* result,NSError* error)) callback {
    
    
    //    if([((PGConnectionOperation*)[self currentPoolOperation]) valid])
    //        if([((PGConnectionOperation*)[self currentPoolOperation]) poolIdentifier] !=0 ){
    
    //        [NSThread detachNewThreadSelector:_cmd toTarget:self withObject:nil ];
    //    if([((PGConnectionOperation*)[self currentPoolOperation]) poolIdentifier] !=0 )
    
    _stateOperation = PGOperationStatePrepare;
    
    
    //        }
#if ( defined(__IPHONE_10_3) &&  __IPHONE_OS_VERSION_MAX_ALLOWED  > __IPHONE_10_3 ) || ( defined(MAC_OS_X_VERSION_10_12) && MAC_OS_X_VERSION_MAX_ALLOWED > MAC_OS_X_VERSION_10_12 )
    [NSThread detachNewThreadWithBlock:^
#else
     
//     dispatch_queue_t queue_inRun = ( ( dispatch_get_current_queue() == dispatch_get_main_queue() )? dispatch_get_main_queue() : dispatch_get_current_queue() );
     mach_port_t machTID = pthread_mach_thread_np(pthread_self());
     
     const char * queued_name = [[NSString stringWithFormat:@"%s_%x", "query_operation_dispacthed_threads", machTID ] cString];
     
     NSLog(@" //// %s ", queued_name);
     
     dispatch_queue_t queue_inRun = dispatch_queue_create(queued_name, DISPATCH_QUEUE_CONCURRENT);
     
               dispatch_barrier_sync(queue_inRun,^
#endif
     
     {
         //        [self performSelectorOnMainThread:@selector(_waitingPoolOperationForResult) withObject:self waitUntilDone:YES ];
         
         
         NSParameterAssert([query isKindOfClass:[NSString class]] || [query isKindOfClass:[PGQuery class]]);
         NSParameterAssert(callback);
         NSString* query2 = nil;
         NSError* error = nil;
         if([query isKindOfClass:[NSString class]]) {
             query2 = query;
         } else {
             query2 = [(PGQuery* )query quoteForConnection:self error:&error];
         }
         if(error) {
#if defined DEBUG && defined DEBUG2
             NSLog(@"%@ :: %@ - query ERROR - callback %p :: \n query :: %@ :::::",NSStringFromClass([self class]), NSStringFromSelector(_cmd), callback, query2);
#endif
             callback(nil,error);
         } else if(query2==nil) {
             callback(nil,[self raiseError:nil code:PGClientErrorExecute reason:@"Query is nil"]);
         } else {
#if defined DEBUG && defined DEBUG2
             NSLog(@"%@ :: %@ - query - callback %p :: \n query :: %@ :::::",NSStringFromClass([self class]), NSStringFromSelector(_cmd), callback, query2);
#endif
             void (^callback_recall)(PGResult* result,NSError* error) =   ^(PGResult* result_recall ,NSError* error_recall)
             {
                 NSLog(@" .... semaphore callback..... ");
                 callback(result_recall , error_recall);
                 NSLog(@" .... semaphore pass ..... ");
                 dispatch_semaphore_signal( [[self currentPoolOperation] semaphore] );
                 NSLog(@" .... semaphore signal end ..... ");
                 
             };
             //            dispatch_semaphore_signal(semaphore_query_send);
             [self _execute:query2 values:nil whenDone: callback_recall];
             
         }
     }
     
#if ( defined(__IPHONE_10_3) &&  __IPHONE_OS_VERSION_MAX_ALLOWED  > __IPHONE_10_3 ) || ( defined(MAC_OS_X_VERSION_10_12) && MAC_OS_X_VERSION_MAX_ALLOWED > MAC_OS_X_VERSION_10_12 )
     ]
#else
    
         )
#endif
    ;
    
    //    [self performSelector:@selector(_waitingPoolOperationForResult) withObject:self ];
    //    [self performSelector:@selector(_waitingPoolOperationForResultMaster) withObject:self ];
    
    _stateOperation = PGOperationStateBusy;
    [NSThread sleepForTimeInterval:.2];
    NSLog(@"Is main queue? : %d", dispatch_get_current_queue() == dispatch_get_main_queue());
    dispatch_semaphore_t semaphore_query_send = [[self currentPoolOperation] semaphore];
    [self wait_semaphore_read: semaphore_query_send ];
    NSLog(@"I will see this, after dispatch_semaphore_signal is called");
}
-(void)wait_semaphore_read:(dispatch_semaphore_t) sem
{
     mach_port_t machTID = pthread_mach_thread_np(pthread_self());
    const char * queued_name = [[NSString stringWithFormat:@"%@_%x  :: %s  :: %s :: %@ ", NSStringFromSelector(_cmd), machTID, dispatch_queue_get_label(dispatch_get_current_queue()), dispatch_queue_get_label(dispatch_get_main_queue()), sem ] cString];
    
    NSLog(@" //// %s ", queued_name);
    
    long diispacthed = YES;
    
    NSTimeInterval resolutionTimeOut = 0.05;
    NSDate* theNextDate = [NSDate dateWithTimeIntervalSinceNow:resolutionTimeOut];
    bool isRunningThreadMain = YES;
    bool isRunningThread = YES;
    while( diispacthed  && _runloopsource )
    {
        
        
        diispacthed = dispatch_semaphore_wait(sem,DISPATCH_TIME_NOW);
        dispatch_queue_t qq_qq = dispatch_get_current_queue();
        
        id shared_info = (__bridge id)(_socket);
        CFSocketCallBack shared_info_shared = (_socket->_callout);
        const char * class_named = object_getClassName(shared_info);
        
        CFSocketNativeHandle sock = CFSocketGetNative(_socket);
        
        struct __shared_blob *shared_dd = malloc(sizeof(struct __shared_blob));
        
        NSLog(@"%s", __CFFSocketCopyDescription(_socket));
        
//        shared_dd->_rdsrc = (_socket->_shared)->_rdsrc;
//        shared_dd->_wrsrc = (_socket->_shared)->_wrsrc;
//        shared_dd->_source = (_socket->_shared)->_source;
//        shared_dd->_socket = (_socket->_shared)->_socket;
//        shared_dd->_closeFD = (_socket->_shared)->_closeFD ;
//        shared_dd->_refCnt = (_socket->_shared)->_refCnt;
        
//        objc_mem(&shared_dd, *(_socket->_shared) , sizeof(struct __shared_blob));
        
        unsigned int outCount, i;
        objc_property_t *objcProperties = class_copyPropertyList([shared_info class], &outCount);
        for (i = 0; i < outCount; i++) {
            objc_property_t property = objcProperties[i];
            const char *propName = property_getName(property);
            if(propName) {
//                const char * propType = getPropertyType(property);
                NSString * propertyName = [NSString stringWithUTF8String:propName];
                
            }
        }
        
//        [((NSObject*)shared_info) shared];
        id sockk = (__bridge id)((_socket)->_shared);
        
//        typedef struct __CFSocket sockt;
//        
//        dispatch_object_t disp_obj = (dispatch_object_t) ((shared_info*)->_shared->_rdsrc);
//        dispatch_resume( ((dispatch_object_t) disp_obj)  );;
        if( diispacthed && !isRunningThreadMain && !isRunningThreadMain
           && [[self currentPoolOperation] valid]
           )
            [NSThread sleepForTimeInterval:.01];
        
        CFRunLoopSourceSignal(_runloopsource);
        CFRunLoopWakeUp(CFRunLoopGetMain());
        
       
        isRunningThreadMain = [[NSRunLoop mainRunLoop] runMode:NSRunLoopCommonModes beforeDate:theNextDate];
        if(!isRunningThreadMain)
        {
             [NSThread sleepForTimeInterval:.01];;
        }
        
        isRunningThreadMain = [[NSRunLoop mainRunLoop] runMode:NSDefaultRunLoopMode beforeDate:theNextDate];
        if(!isRunningThreadMain)
        {
            [NSThread sleepForTimeInterval:.01];;
        }
        
        CFRunLoopSourceSignal(_runloopsource);
        CFRunLoopWakeUp(CFRunLoopGetCurrent());
        
       
        isRunningThread = [[NSRunLoop currentRunLoop] runMode:NSRunLoopCommonModes beforeDate:theNextDate];
        if(!isRunningThread)
        {
             [NSThread sleepForTimeInterval:.01];;
        }
        
        isRunningThread = [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode beforeDate:theNextDate];
        if(!isRunningThread)
        {
            [NSThread sleepForTimeInterval:.01];;
        }
        
        if( diispacthed && !isRunningThreadMain && !isRunningThreadMain
           && [[self currentPoolOperation] valid] && [self connectionPoolOperationCount] > 1
           && !PQisBusy(_connection) )
            break;
        
    }
    //         [[NSThread currentThread] cancel];
}

-(PGResult* )execute:(id)query error:(NSError** )error {
    dispatch_semaphore_t s = dispatch_semaphore_create(0);
    __block PGResult* result = nil;
    [self execute:query whenDone:^(PGResult* r, NSError* e) {
        if(error) {
            (*error) = e;
        }
        result = r;
        dispatch_semaphore_signal(s);
    }];
    dispatch_semaphore_wait(s,DISPATCH_TIME_FOREVER);
    return result;
}

-(void)_queue:(PGTransaction* )transaction index:(NSUInteger)i lastResult:(PGResult* )result lastError:(NSError* )error whenQueryDone:(void(^)(PGResult* result,BOOL isLastQuery,NSError* error)) callback {
    if(error) {
        // rollback
        if([transaction transactional]) {
            NSString* rollbackTransaction = [(PGTransaction* )transaction quoteRollbackTransactionForConnection:self];
            NSParameterAssert(rollbackTransaction);
            [self execute:rollbackTransaction whenDone:^(PGResult* result2,NSError* error2) {
                callback(nil,YES,error);
            }];
        } else {
            callback(nil,YES,error);
        }
    } else if(i==[transaction count]) {
        // commit
        if([transaction transactional]) {
            NSString* commitTransaction = [(PGTransaction* )transaction quoteCommitTransactionForConnection:self];
            NSParameterAssert(commitTransaction);
            [self execute:commitTransaction whenDone:^(PGResult* result2, NSError* error2) {
                callback(result,YES,error);
            }];
        } else {
            callback(result,YES,error);
        }
    } else {
        // execute a single query
        [self execute:[transaction queryAtIndex:i] whenDone:^(PGResult* result,NSError* error) {
            if(i < [transaction count]) {
                [self _queue:transaction index:(i+1) lastResult:result lastError:error whenQueryDone:callback];
            }
        }];
    }
}

-(void)queue:(PGTransaction* )transaction whenQueryDone:(void(^)(PGResult* result,BOOL isLastQuery,NSError* error)) callback {
    NSParameterAssert(transaction && [transaction isKindOfClass:[PGTransaction class]]);
    
    // where there are no transactions to execute, raise error immediately
    if([transaction count]==0) {
        callback(nil,YES,[self raiseError:nil code:PGClientErrorExecute reason:@"No transactions to execute"]);
        return;
    }
    
    // check for connection status
    if(_connection==nil || [self state] != PGConnectionStateNone) {
        callback(nil,YES,[self raiseError:nil code:PGClientErrorState]);
        return;
    }
    
    // check for transaction status
    PGTransactionStatusType tstatus = PQtransactionStatus(_connection);
    if([transaction transactional] && tstatus != PQTRANS_IDLE) {
        callback(nil,YES,[self raiseError:nil code:PGClientErrorState reason:@"Already in a transaction"]);
        return;
    }
    
    if([transaction transactional]==NO) {
        // queue zeroth query
        [self _queue:transaction index:0 lastResult:nil lastError:nil whenQueryDone:callback];
    } else {
        // queue up a start transaction, which triggers the first query
        NSString* beginTransaction = [(PGTransaction* )transaction quoteBeginTransactionForConnection:self];
        NSParameterAssert(beginTransaction);
        [self execute:beginTransaction whenDone:^(PGResult* result, NSError* error) {
            // if the BEGIN transaction didn't work, then callback
            if(error) {
                callback(nil,YES,error);
            } else {
                // else queue up zeroth query
                [self _queue:transaction index:0 lastResult:result lastError:error whenQueryDone:callback];
            }
        }];
    }
}

@end


