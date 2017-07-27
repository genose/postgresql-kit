
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
    
    //    [NSThread sleepForTimeInterval: .2];
}

////////////////////////////////////////////////////////////////////////////////
#pragma mark public methods - execution
////////////////////////////////////////////////////////////////////////////////

-(void)execute:(id)query whenDone:(void(^)(PGResult* result,NSError* error)) callback {
    
    
    
    //    if([((PGConnectionOperation*)[self currentPoolOperation]) valid])
    //        if([((PGConnectionOperation*)[self currentPoolOperation]) poolIdentifier] !=0 ){
    
    //        [NSThread detachNewThreadSelector:_cmd toTarget:self withObject:nil ];
//    if([((PGConnectionOperation*)[self currentPoolOperation]) poolIdentifier] !=0 )
    

    
    //        }
    [NSThread detachNewThreadWithBlock:^{
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
            
            
            
            [self _execute:query2 values:nil whenDone: callback];
            
        }
    }];

    [self performSelector:@selector(_waitingPoolOperationForResult) withObject:self ];
    [self performSelector:@selector(_waitingPoolOperationForResultMaster) withObject:self ];
    
    
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


