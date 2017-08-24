
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

#include <sys/fcntl.h>
////////////////////////////////////////////////////////////////////////////////
#pragma mark C callback functions
////////////////////////////////////////////////////////////////////////////////


//typedef struct __shared_blob soctted ;
/**
 *  This method is called from the run loop upon new data being available to read
 *  on the socket, or the socket being able to write more data to the socket
 */
static int socketUsed_in = 0;
void _socketCallback(CFSocketRef s, CFSocketCallBackType callBackType,CFDataRef address,const void* data,void* __self) {
    
    
    
//    [NSThread sleepForTimeInterval:0.01];

    PGConnection* connection = (PGConnection* ) ((__bridge PGConnection* )__self);
    if(! connection
       
       || ! [ (PGConnection*)connection masterPoolOperation]
       //       || socketUsed_in > 20
       )
        return ;
    socketUsed_in ++;
    //    dispatch_barrier_async(dispatch_get_main_queue(), ^{
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@"%@ :: %s :::: Socket CALL ... %lu ", NSStringFromClass([((__bridge NSObject *)__self) class]),  (__FUNCTION__), callBackType);
#endif
    [((PGConnection* )connection) _socketCallback:callBackType];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@"%@ :: %s :::: Socket CALL END .... (%lu) ", NSStringFromClass([((__bridge NSObject *)__self) class]),  (__FUNCTION__), callBackType);
#endif
    socketUsed_in = 0;
    //    });
    
    //    [NSThread sleepForTimeInterval:.1];;
    
}

/**
 *  Notice processor callback which is called when there is a NOTICE message from
 *  the libpq library
 */
void _noticeProcessor(void* arg,const char* cString) {
    NSString* notice = [NSString stringWithUTF8String:cString];
    PGConnection* connection = (__bridge PGConnection* )arg;
    NSCParameterAssert(connection && [connection isKindOfClass:[PGConnection class]]);
    if([[connection delegate] respondsToSelector:@selector(connection:notice:)]) {
        [[connection delegate] connection:connection notice:notice];
    }
}


@implementation PGConnection (Callbacks)

////////////////////////////////////////////////////////////////////////////////
#pragma mark private methods - socket callbacks
////////////////////////////////////////////////////////////////////////////////

-(void)_socketCallbackNotification {
    @try
    {
//        NSParameterAssert(_connection);
    if(!_connection){

        return;
    }
        // consume input
        PQconsumeInput(_connection);
        
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"PGConnectionStateQuery - Read - _socketCallbackNotification ::  loop ");
#endif
        
        // loop for notifications
        PGnotify* notify = nil;
        while((notify = PQnotifies(_connection)) != nil) {
            if([[self delegate] respondsToSelector:@selector(connection:notificationOnChannel:payload:)]) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@"PGConnectionStateQuery - Read - _socketCallbackNotification ::  call delegate (%@) (%@)", [self delegate], NSStringFromSelector(@selector(connection:notificationOnChannel:payload:)));
#endif
                
                NSString* channel = [NSString stringWithUTF8String:notify->relname];
                NSString* payload = [NSString stringWithUTF8String:notify->extra];
                [[self delegate] connection:self notificationOnChannel:channel payload:payload];
            }
            PQfreemem(notify);
        }
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"PGConnectionStateQuery - Read - _socketCallbackNotification ::  free ");
#endif
    }
    @catch (NSException *exception) {
        NSLog(@" %@ exeception .... %@",NSStringFromSelector(_cmd),exception);
    }
    @finally {
        
    }
}

-(void)_socketCallbackConnectEndedWithStatus:(PostgresPollingStatusType)pqstatus {
    
    //::	void (^callback)(BOOL usedPassword,NSError* error) = (__bridge void (^)(BOOL,NSError* ))(_callback);
    PGConnectionOperation* currentPool = ((PGConnectionOperation*)[self currentPoolOperation]);
    if(
       !_connection ||
       !currentPool ||
       ![currentPool valid] ||
       [currentPool poolIdentifier] != 0)
        return;
    
    pqstatus = PQconnectPoll(_connection);
    
    
    
    BOOL needsPassword = PQconnectionNeedsPassword(_connection) ? YES : NO;
    BOOL usedPassword = PQconnectionUsedPassword(_connection) ? YES : NO;
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
				NSLog(@"%@ (%p) :: BEGIN :: - Read::BEGIN - %@ ::  free (callback %p)", NSStringFromClass([self class]), self, NSStringFromSelector(_cmd), callback);
#endif
    // update the status
    [self setState:PGConnectionStateNone];
    [self _updateStatus]; // this also calls disconnect when rejected
    
    @try {
        // callback
        
        
        _callbackOperation =  [((PGConnectionOperation*)[self masterPoolOperation]) getCallback];
        void (^callback)(BOOL usedPassword,NSError* error ) = nil;
        if(_callbackOperation != nil)
        {
            callback = (__bridge void (^)( BOOL,NSError*  ))( _callbackOperation );
            
        }else{
            [NSException raise:NSInvalidArgumentException format:@" Warning :: Master Pool callback was sudently cleaned .... "];
        }
        
        
        
        if(pqstatus==PGRES_POLLING_OK) {
            // set up notice processor, set success condition
            PQsetNoticeProcessor(_connection,_noticeProcessor,(__bridge void *)(self));
            currentPool = [self currentPoolOperation];
            if(!currentPool) {

                return;
            }

            if([currentPool poolIdentifier] == 0){
                
                
                mach_port_t machTID = pthread_mach_thread_np(pthread_self());
                
                const char * queued_name = [[NSString stringWithFormat:@"%s_%x_%@", "operation_dispacthed_threads", machTID, NSStringFromSelector(_cmd) ] cString];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@" //// %s ", queued_name);
#endif
                dispatch_queue_t queue_inRun = dispatch_queue_create(queued_name, DISPATCH_QUEUE_CONCURRENT);
                queue_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
                
                
                
                
                [((PGConnectionOperation*)[self masterPoolOperation]) invalidate];
                
                dispatch_async(queue_inRun,^{
                    @try{
                        
                        dispatch_semaphore_t ss = [((PGConnectionOperation*)[self masterPoolOperation]) semaphore]; // dispatch_semaphore_create(0);
                        _callbackOperation =  [((PGConnectionOperation*)[self masterPoolOperation]) getCallback];
                        if(_callbackOperation != nil && _connection )
                        {
                            void (^callback_master)(BOOL usedPassword,NSError* error ) = (__bridge void (^)( BOOL,NSError*  ))( _callbackOperation );
                            
                            callback_master(usedPassword ? YES : NO,nil);
                            
                        }else if(!_connection){
                            [NSException raise:NSInvalidArgumentException format:@" Warning :: Master Pool callback (async) was sudently cleaned .... "];
                        }
                        else{
                            [NSException raise:NSInvalidArgumentException format:@" Warning :: Master Pool Warning (async) :: NO OPERATION :: was sudently cleaned .... "];
                        }
                        dispatch_semaphore_signal(ss);
//                        [((PGConnectionOperation*)[self masterPoolOperation]) finish];
                    } @catch (NSException *exception) {
                        NSLog(@"**************************** \n ERROR :: %@ :: \n **************************** \n [ %@ ] \n **************************** \n ", NSStringFromSelector(_cmd), exception);
                        dispatch_semaphore_t ss = [((PGConnectionOperation*)[self masterPoolOperation]) semaphore]; // dispatch_semaphore_create(0);
                        dispatch_semaphore_signal(ss);
                    } @finally {
                        
                    }
                    
                }
                               
                               
                               );
                
                //          dispatch_semaphore_wait(ss,DISPATCH_TIME_FOREVER);
                //         [self wait_semaphore_read:ss];
                [self wait_semaphore_read: [((PGConnectionOperation*)[self masterPoolOperation]) semaphore] withQueue:queue_inRun];
                // [((PGConnectionOperation*)[self masterPoolOperation]) validate];
            }
            //         dispatch_destroy(queue);
        } else if(needsPassword) {
            // error callback - connection not made, needs password
            callback(NO,[self raiseError:nil code:PGClientErrorNeedsPassword]);
        } else if(usedPassword) {
            // error callback - connection not made, password was invalid
            callback(YES,[self raiseError:nil code:PGClientErrorInvalidPassword]);
        } else {
            // error callback - connection not made, some other kind of rejection
            callback(YES,[self raiseError:nil code:PGClientErrorRejected]);
        }
        
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"%@ (%p) :: END :: - Read::END - %@ ::  free (callback %p)", NSStringFromClass([self class]), self, NSStringFromSelector(_cmd), callback);
#endif
        // :: TODO  ::
        //::    _callback = nil;
        //
        //    [((PGConnectionOperation*)[self currentPoolOperation]) invalidate];
        //    [((PGConnectionOperation*)[self masterPoolOperation]) invalidate];
        //    [self pushPoolOperation];
        
        
    } @catch (NSException *exception) {
        NSLog(@"**************************** \n ERROR :: %@ :: \n **************************** \n [ %@ ] \n **************************** \n ", NSStringFromSelector(_cmd), exception);
        dispatch_semaphore_t ss = [((PGConnectionOperation*)[self masterPoolOperation]) semaphore]; // dispatch_semaphore_create(0);
        dispatch_semaphore_signal(ss);
    } @finally {
        
    }
}


-(void)_socketCallbackResetEndedWithStatus:(PostgresPollingStatusType)pqstatus {
    NSParameterAssert([[self currentPoolOperation] valid]);
    //::	void (^callback)(NSError* error) = (__bridge void (^)(NSError* ))(_callback);
    void (^callback)(NSError* error) = (__bridge void (^)(NSError* ))( [((PGConnectionOperation*)[self currentPoolOperation]) getCallback] );
    if(pqstatus==PGRES_POLLING_OK) {
        callback(nil);
    } else {
        callback([self raiseError:nil code:PGClientErrorRejected]);
    }
    //::	_callback = nil;
    [((PGConnectionOperation*)[self currentPoolOperation]) invalidate];
    [self setState:PGConnectionStateNone];
    [self _updateStatus]; // this also calls disconnect when rejected
}

/**
 *  The connect callback will continue to poll the connection for new data. When
 *  the poll status is either OK or FAILED, the application's callback block is
 *  run.
 */
-(void)_socketCallbackConnect {
    // ignore this call if either connection or callback are nil
    
    PGConnectionOperation* currentpoolOpe = [self currentPoolOperation];
    if(_connection==nil
       || ![[self currentPoolOperation] valid]
       || ( [currentpoolOpe poolIdentifier]) !=0
       ) {
        // || (_callback==nil) // && _callbackOperation == nil)
        return;
    }
    
    PostgresPollingStatusType pqstatus = PQconnectPoll(_connection);
    switch(pqstatus) {
        case PGRES_POLLING_READING:
        case PGRES_POLLING_WRITING:
            // still connecting - ask to call poll again in runloop
            [self performSelector:@selector(_socketCallbackConnect) withObject:nil afterDelay:0.1];
            break;
        case PGRES_POLLING_OK:
        case PGRES_POLLING_FAILED:
            if(( [currentpoolOpe poolIdentifier]) ==0 && [currentpoolOpe valid] )
            {
                // finished connecting
                //             [self performSelectorInBackground:@selector(_socketCallbackConnectEndedWithStatus:)  withObject:nil];
                [self performSelector:@selector(_socketCallbackConnectEndedWithStatus:) withObject:nil afterDelay:0.1];
                
                //                             [self performSelector:@selector(_socketCallbackConnectEndedWithStatus:) withObject:nil];
            }
            //			[self _socketCallbackConnectEndedWithStatus:pqstatus];
            break;
        default:
            break;
    }
}

/**
 *  The reset callback is very similar to the connect callback, and could probably
 *  be merged with that one.
 */
-(void)_socketCallbackReset {
    NSParameterAssert(_connection);
    
    PostgresPollingStatusType pqstatus = PQresetPoll(_connection);
    switch(pqstatus) {
        case PGRES_POLLING_READING:
        case PGRES_POLLING_WRITING:
            // still connecting - call poll again
            PQresetPoll(_connection);
            break;
        case PGRES_POLLING_OK:
        case PGRES_POLLING_FAILED:
            // finished connecting
            [self _socketCallbackResetEndedWithStatus:pqstatus];
            break;
        default:
            break;
    }
}

/**
 *  In the case of a query being processed, this method will consume any input
 *  then any results from the server
 */
-(void)_socketCallbackQueryRead {
    NSParameterAssert(_connection);
    
    bool pgBusy = false;
    
    @synchronized (self) {
        
        
        PQconsumeInput(_connection);
        
        /* it seems that we don't really need to check for busy and it seems to
         * create some issues, so ignore for now */
        // check for busy, return if more to do
        
        pgBusy = PQisBusy(_connection);
    }
    
    if(pgBusy) {
        return;
    }
    
    //
    NSParameterAssert([[self currentPoolOperation] valid]);
    
    PGConnectionOperation* currentPoolOperation = [self currentPoolOperation];
    
    //::    void (^callback)(PGResult* result,NSError* error) = (__bridge void (^)(PGResult* ,NSError* ))(_callback);
    
    void (^callback)(PGResult* result,NSError* error) = (__bridge void (^)(PGResult* ,NSError* ))( [((PGConnectionOperation*)[self currentPoolOperation]) getCallback] );
    
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@":::: PGConnectionStateQuery(%@::%p) - Read BEGIN - Result - :: callback  \n_callback :: (%@)\n ********************************* ",NSStringFromClass([self class]), self  , callback  );
#endif
    
    
    // consume results
    PGresult* result = nil;
    while(1) {
        result = PQgetResult(_connection);
        if(result==nil) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery - Read - Result nil - End");
#endif
            break;
        }
        NSError* error = nil;
        PGResult* r = nil;
        
        
        // check for connection errors
        if(PQresultStatus(result)==PGRES_EMPTY_QUERY) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery - Read - Result - Empty Query");
#endif
            // callback empty query
            error = [self raiseError:nil code:PGClientErrorQuery reason:@"Empty query"];
            PQclear(result);
        } else if(PQresultStatus(result)==PGRES_BAD_RESPONSE || PQresultStatus(result)==PGRES_FATAL_ERROR) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery - Read - Result - Client Error (%s)", PQresultErrorMessage(result));
#endif
            error = [self raiseError:nil code:PGClientErrorExecute reason:[NSString stringWithUTF8String:PQresultErrorMessage(result)]];
            PQclear(result);
        } else {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery - Read - Result - READ Query RESULT (%@)",[currentPoolOperation queryString] );
#endif
            // TODO: allocate a different kind of class
            r = [[PGResult alloc] initWithResult:result format:[self tupleFormat]];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery - Read - Result - READ END (%s)", PQresultErrorMessage(result));
#endif
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery - Read - Result - Done :: (%@)", r );
#endif
        }
        if(r || error) {
            
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@"PGConnectionStateQuery(%@::%p) - Read - Result - Done :: callback \n ********************************* \nresult :: (%@)\n ********************************* \nerror :: (%@) \n_callback :: (%@)\n ********************************* ",NSStringFromClass([self class]), self, r, error, callback  );
#endif
            // queue up callback on nearest thread
            
            dispatch_queue_t qu_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
            
            dispatch_async( qu_inRun ,^{
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@"PGConnectionStateQuery - Read - callback %p ",callback);
#endif
                id curope = [self currentPoolOperation];
                id queryResults = [ curope setResults: r ];
                
                callback(r,error);
                [((PGConnectionOperation*)[self currentPoolOperation]) invalidate];
//                [((PGConnectionOperation*)[self currentPoolOperation]) finish];
                [((PGConnectionOperation*)[self masterPoolOperation]) validate];
            });
            
            break;
        }
    }
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@"PGConnectionStateQuery - Read - Result - Clear " );
#endif
    // all results consumed - update state
    [self setState:PGConnectionStateNone];
    
    //::	_callback = nil; // release the callback
    [((PGConnectionOperation*)[self masterPoolOperation]) invalidate];
    //    [((PGConnectionOperation*)[self currentPoolOperation]) invalidate];
    
    [self _updateStatus];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@"PGConnectionStateQuery - Read END - Result - Clear::End " );
#endif
    _stateOperation = PGOperationStateNone;
}

/**
 *  In the case of a query being processed, this method will consume any input
 *  flush the connection and consume any results which are being processed.
 */
-(void)_socketCallbackQueryWrite {
    NSParameterAssert(_connection);
    // flush
    int returnCode = PQflush(_connection);
    if(returnCode==-1) {
        // callback with error
        NSParameterAssert([[self currentPoolOperation] valid]);
        //::		void (^callback)(PGResult* result,NSError* error) = (__bridge void (^)(PGResult* ,NSError* ))(_callback);
        void (^callback)(PGResult* result,NSError* error) = (__bridge void (^)(PGResult* ,NSError* ))( [((PGConnectionOperation*)[self currentPoolOperation]) getCallback] );
        NSError* error = [self raiseError:nil code:PGClientErrorState reason:@"Data flush failed during query"];
        callback(nil,error);
    }
}

/**
 *  This method is called from _socketCallback and depending on the
 *  current state of the connection, it will call the connect, reset, query
 *  or notification socket callback
 */
-(void)_socketCallback:(CFSocketCallBackType)callBackType {
    @try{
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        switch(callBackType) {
            case kCFSocketReadCallBack:
                NSLog(@"kCFSocketReadCallBack");
                break;
            case kCFSocketAcceptCallBack:
                NSLog(@"kCFSocketAcceptCallBack");
                break;
            case kCFSocketDataCallBack:
                NSLog(@"kCFSocketDataCallBack");
                break;
            case kCFSocketConnectCallBack:
                NSLog(@"kCFSocketConnectCallBack");
                break;
            case kCFSocketWriteCallBack:
                NSLog(@"kCFSocketWriteCallBack");
                break;
            default:
                NSLog(@"CFSocketCallBackType OTHER");
                break;
        }
#endif
        PGConnectionState state_on = [self state];
        switch(state_on) {
            case PGConnectionStateConnect:
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@"PGConnectionStateConnect");
#endif
                [self _socketCallbackConnect];
                break;
            case PGConnectionStateReset:
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@"PGConnectionStateReset");
#endif
                [self _socketCallbackReset];
                break;
            case PGConnectionStateQuery:
                if(callBackType==kCFSocketReadCallBack) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                    NSLog(@"PGConnectionStateQuery - Read - _socketCallback :: _socketCallbackQueryRead :: callback (%p)",[[self currentPoolOperation] getCallback]);
#endif
                    [self _socketCallbackQueryRead];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                    NSLog(@"PGConnectionStateQuery - Read - _socketCallback :: _socketCallbackNotification :: callback (%p)", [[self currentPoolOperation] getCallback]);
#endif
                    [self _socketCallbackNotification];
                    
                } else if(callBackType==kCFSocketWriteCallBack) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                    NSLog(@"PGConnectionStateQuery - Write");
#endif
                    [self _socketCallbackQueryWrite];
                }
                break;
            default:
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@"PGConnectionStateOther");
#endif
                [self _socketCallbackNotification];
                break;
        }
        
        [self _updateStatus];
    }
    @catch (NSException *exception) {
        NSLog(@" %@ exeception .... %@",NSStringFromSelector(_cmd),exception);
    }
    @finally {
        
    }
}

@end



