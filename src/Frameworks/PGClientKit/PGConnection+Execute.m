
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

@implementation PGConnection (Execute)

////////////////////////////////////////////////////////////////////////////////
#pragma mark private methods - statement execution
////////////////////////////////////////////////////////////////////////////////



-(void)_execute:(NSString* )query values:(NSArray* )values whenDone:(void(^)(PGResult* result,NSError* error)) callback {
    
    
    NSParameterAssert(query && [query isKindOfClass:[NSString class]]);
    
    if(_connection==nil ) {
        callback(nil,[self raiseError:nil code:PGClientErrorState]);
        return;
    }
    
    PGConnectionState curstate = [self state];
    if( curstate != PGConnectionStateNone)
    {
        NSLog(@" :: Warning :: %@ :: %@ :: \n :: Wrong State  :: %u ", NSStringFromSelector(_cmd), self, curstate);
        ;;
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
    
    // execute the command, free parameters
    int resultFormat = 0;
    int returnCode = 0;
    @synchronized(self)
    {
        // set state, update status
        [self setState:PGConnectionStateQuery];
        
        [self _updateStatus];
        
        // execute the command, free parameters
        resultFormat = ([self tupleFormat]==PGClientTupleFormatBinary) ? 1 : 0;
        returnCode = PQsendQueryParams(_connection,[query UTF8String],(int)params->size,params->types,(const char** )params->values,params->lengths,params->formats,resultFormat);
       
    }
    
    _paramFree(params);
    
    if(!returnCode) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"%@ :: %@ :: - execute - ERROR :: callback %p :: While EXECUTE (%@)", NSStringFromSelector(_cmd),NSStringFromClass([self class]), callback, query);
#endif
        callback(nil,[self raiseError:nil code:PGClientErrorExecute reason:[NSString stringWithUTF8String:PQerrorMessage(_connection)]]);
        return;
    }else{
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"%@ :: %@ :: - execute - PASSED :: callback %p :: While EXECUTE (%@)", NSStringFromSelector(_cmd),NSStringFromClass([self class]), callback, query);
#endif
    }
    
    
    NSParameterAssert(callback!=nil);
    
    //    _callbackOperation = (__bridge_retained void* )[callback copy];
    
    //   if([self currentPoolOperation] != nil) [[self currentPoolOperation] invalidate];
    
    [self addOperation:query withCallBackWhenDone: (__bridge_retained void* )callback withCallBackWhenError: (__bridge_retained void* )callback ];
    
    
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@"%@ :: %@ :: - execute - RETURN :: callback %p :: %p ", NSStringFromSelector(_cmd),NSStringFromClass([self class]), callback, _callbackOperation);
#endif
    
    //    NSParameterAssert(_callbackOperation!=nil);
    
    //    [NSThread sleepForTimeInterval: .2];
}

////////////////////////////////////////////////////////////////////////////////
#pragma mark public methods - execution
////////////////////////////////////////////////////////////////////////////////

-(id)execute:(id)query whenDone:(void(^)(PGResult* result,NSError* error)) callback {
    
    NSParameterAssert([query isKindOfClass:[NSString class]] || [query isKindOfClass:[PGQuery class]]);
    NSParameterAssert(callback);
    
    if( ! _connection )
    {
        void (^ _Nullable callbackRecall)(void * pm,NSError* error) = ( void (^ _Nullable )(void* ,NSError* ))( callback );
        
        [self _reconnectWithHandler: callbackRecall];
        if( ! _connection || !_socket)
            callback(NO,[self raiseError:nil code:PGClientErrorState]);
        
//        [[self masterPoolOperation] invalidate];
//        [[self masterPoolOperation] finish];
        
//        return;
    }
    
    _stateOperation = PGOperationStatePrepare;
    
    //    mach_port_t machTID = pthread_mach_thread_np(pthread_self());
    NSString * queued_name_STR = [NSString stringWithFormat:@"%s_%ld :: %@", "query_operation_dispacthed_threads", (long)[self connectionPoolOperationCount], [NSThread currentThread] ];
    const char * queued_name = [queued_name_STR UTF8String];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@" //// -_-_-_-_ Query Dispatch START  -_-_-_-_- :: %@ ", queued_name_STR);
#endif
    dispatch_queue_t queue_inRun = dispatch_queue_create(queued_name, DISPATCH_QUEUE_CONCURRENT);
    
    NSString* query2 = nil;
    NSError* error = nil;
    __block id queryResults = nil;
    
    if([query isKindOfClass:[NSString class]]) {
        query2 = query;
    } else {
        query2 = [(PGQuery* )query quoteForConnection:self error:&error];
    }
    if(error) {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"%@ :: %@ - query ERROR - callback %p :: \n query :: %@ :::::",NSStringFromClass([self class]), NSStringFromSelector(_cmd), callback, query2);
#endif
        callback(nil,error);
    } else if(query2==nil) {
        callback(nil,[self raiseError:nil code:PGClientErrorExecute reason:@"Query is nil"]);
    } else {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
        NSLog(@"%@ :: %@ - query - callback %p :: \n query :: %@ :::::",NSStringFromClass([self class]), NSStringFromSelector(_cmd), callback, query2);
#endif
        void (^callback_recall)(PGResult* result,NSError* error) =   ^(PGResult* result_recall ,NSError* error_recall)
        {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@" .... semaphore callback..... %@", [[self currentPoolOperation] semaphore]);
#endif
//            id curope = [self currentPoolOperation];
//            queryResults = [ curope setResults: result_recall ];
//            [result_recall fetchRowAsDictionary]
            callback(result_recall , error_recall);
            
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@" .... semaphore pass ..... ");
            //                 dispatch_semaphore_signal( [[self currentPoolOperation] semaphore] );
            NSLog(@" .... semaphore signal end ..... %@", [[self currentPoolOperation] semaphore]);
#endif
            
        };
        //            dispatch_semaphore_signal(semaphore_query_send);
        [self _execute:query2 values:nil whenDone: callback_recall];
        
    }
    
    
    _stateOperation = PGOperationStateBusy;
    //    [NSThread sleepForTimeInterval:.2];
    dispatch_queue_t qu_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
    //    NSLog(@"+++ Is main queue? : %d", qu_inRun == dispatch_get_main_queue());
    
    id curope = [self currentPoolOperation];
    
    dispatch_semaphore_t semaphore_query_send = [curope semaphore];
    [self wait_semaphore_read: semaphore_query_send withQueue:queue_inRun];
    
//    NSLog(@" RESULTS ::\n Query : %@ \n:: Result : %@ ", query2, queryResults);
    
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@" -_-_-_-_ Query END  -_-_-_-_-  :: Query : %@ :: %@ ", query2, queued_name_STR);
#endif
    _stateOperation = PGOperationStateNone;
    return (queryResults)?[queryResults copy] : nil;
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
    PGTransactionStatusType transac_status = PQtransactionStatus(_connection);
    if([transaction transactional] && transac_status != PQTRANS_IDLE) {
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



#pragma mark -------- Sync Thread and Semaphores
-(void)wait_semaphore_read:(dispatch_semaphore_t) sem {
    dispatch_queue_t qu_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
    
    NSRunLoop *qq_loop = [NSRunLoop currentRunLoop];
    NSRunLoop *qq_loop_main = [NSRunLoop mainRunLoop];
    
    
    if( qq_loop != [NSRunLoop mainRunLoop]){
        
        dispatch_barrier_sync(qu_inRun, ^{
//            @synchronized (self) {
                    [self wait_semaphore_read:sem withQueue:nil];
//            }
            
        });
    }else{
//        @synchronized (self) {
        //        dispatch_barrier_async(qu_inRun, ^{
        [self wait_semaphore_read:sem withQueue:nil];
//        }
        //        });
    }
    
}
-(void)wait_semaphore_read:(dispatch_semaphore_t) sem withQueue:(dispatch_queue_t)qq_in {
    
    //    mach_port_t machTID = pthread_mach_thread_np(pthread_self());
    
    NSString *queued_name_STR =  [NSString stringWithFormat:@"%@_%ld  :: %s :: %@ ", NSStringFromSelector(_cmd), (long)[self connectionPoolOperationCount],
                                  dispatch_queue_get_label(dispatch_get_main_queue()),
                                  sem ];
    
    //    const char * queued_name = [queued_name_STR UTF8String];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@" //// Start :: %@ ", queued_name_STR);
#endif
    //         NSLog(@" //// Start **** :: %s ", queued_name);
    
    long diispacthed = YES;
    
    bool PG_busy = YES;
    
    long timeoutThread = 1200; // .05 * 600 = 60s
    
    @try {
        
    while(
          diispacthed
          && _runloopsource
          && _socket
          && _connection )
    {
        
        timeoutThread -= 0.05;
        
//        PG_busy = PQisBusy(_connection);
        diispacthed = dispatch_semaphore_wait(sem,DISPATCH_TIME_NOW);
        
        CFIndex indexInPool = CFArrayGetCount(_callbackOperationPool);
        if(!indexInPool)
        {
            [NSException raise:NSInvalidArgumentException format:@" Warning :: Pool was sudently cleaned .... "];
            
            break;
        }
        
        [NSThread sleepForTimeInterval:0.05];
        
        if(  timeoutThread <= 0)
        {
             NSLog(@" //// STEP :: %@ :: Thread Time OUT  %@ ",NSStringFromSelector(_cmd), queued_name_STR);
            break;
        }
        
        
        if( nil == [[self currentPoolOperation] getCallback] || !diispacthed){
//            [NSThread sleepForTimeInterval:0.1];
            break;
        }
        
        
        
        
        if([[self currentPoolOperation] valid] )
        {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@" //// STEP :: %@ ", queued_name_STR);
#endif
            [self performSelector:@selector(dispathCall) withObject:nil];
        }else{
            break;
        }
        indexInPool = CFArrayGetCount(_callbackOperationPool);
        
        if(!indexInPool)
        {
            [NSException raise:NSInvalidArgumentException format:@" Warning @2 :: Pool was sudently cleaned .... "];
            
            break;
        }
        
//        if( diispacthed && ![[self currentPoolOperation] valid] )
//            [NSThread sleepForTimeInterval:0.01];
        
        //             if( diispacthed
        ////                && !isRunningThreadMain && !isRunningThreadMain
        //                && [[self currentPoolOperation] valid]
        //                && [self connectionPoolOperationCount] > 1
        //                && ! PG_busy )
        //                 break;
        
        PGConnectionStatus con_status = [((PGConnection*)self) status];
        if(  con_status == PGConnectionStatusDisconnected || ! _connection)
            break;
    }
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@" //// Clean **** :: %@ ", queued_name_STR);
#endif
    
    } @catch (NSException *exception) {
        NSLog(@"**************************** \n ERROR :: %@ :: \n **************************** \n [ %@ ] \n **************************** \n ", NSStringFromSelector(_cmd), exception);
        dispatch_semaphore_signal(sem);
    } @finally {
        
    }
    [[NSThread currentThread] cancel];
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
    NSLog(@" //// END  :: %@ ", queued_name_STR);
#endif
    
}

-(void)dispathCall
{
    
    NSTimeInterval resolutionTimeOut = 0.05;
    NSDate* theNextDate = [NSDate dateWithTimeIntervalSinceNow:resolutionTimeOut];
    bool isRunningThreadMain = YES;
    bool isRunningThread = YES;
    
//    [NSThread sleepForTimeInterval:0.05];;
    PGConnectionStatus con_status = [((PGConnection*)self) status];
    if(
       (
        con_status != PGConnectionStatusBusy &&
        con_status != PGConnectionStatusConnected &&
        con_status != PGConnectionStatusConnecting
        
        )
       || ! _connection || !_socket)
        return;
    
    {
        if(_runloopsource){
            CFRunLoopSourceSignal(_runloopsource);
        }else{
            return;
        }
        
        CFRunLoopWakeUp(CFRunLoopGetMain());
        
        if(_runloopsource){
            CFRunLoopSourceSignal(_runloopsource);
        }
        
        CFRunLoopWakeUp(CFRunLoopGetCurrent());
        
        dispatch_queue_t qu_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
        dispatch_queue_t qu_inRun_main = dispatch_get_main_queue();
        
        NSRunLoop *qq_loop = [NSRunLoop currentRunLoop];
        NSRunLoop *qq_loop_main = [NSRunLoop mainRunLoop];
        
        isRunningThread = [qq_loop runMode:NSRunLoopCommonModes beforeDate:theNextDate];
        if(!isRunningThread)
        {
            [NSThread sleepForTimeInterval:0.1];;
        }
        
        if( qq_loop != [NSRunLoop mainRunLoop]){
            if([[self currentPoolOperation] poolIdentifier] == 0 ){
                [qq_loop runUntilDate:theNextDate];
                if(!isRunningThread){
                    [qq_loop_main  runUntilDate:theNextDate];
                    [NSThread sleepForTimeInterval:0.1];;
                }
                
                //                [qq_loop_main runUntilDate:theNextDate];
                //
                //                isRunningThread = [qq_loop_main runMode:NSRunLoopCommonModes beforeDate:theNextDate];
                //                if(!isRunningThread)
                //                {
                //                    [NSThread sleepForTimeInterval:.1];;
                //                     [self performSelectorOnMainThread:@selector(dispathCall)  withObject:self waitUntilDone:YES modes:@[NSRunLoopCommonModes, NSDefaultRunLoopMode] ];
                //                }
                //
            }else{
                
                {
                    [qq_loop_main  runUntilDate:theNextDate];
                    [qq_loop runUntilDate:theNextDate];
                    
                }
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
                NSLog(@"  END  performSelectorOnMainThread :: %@", NSStringFromSelector(_cmd));
#endif
                return; // don t care
            }
        }else
            if([[self currentPoolOperation] valid])
            {
                if(!isRunningThread){
                    [qq_loop runUntilDate:theNextDate];
                }else{
                    [qq_loop_main  runUntilDate:theNextDate];
                }
            }
        
    }
}

@end


