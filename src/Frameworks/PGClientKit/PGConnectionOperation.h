// *************************************
//
// Copyright 2017 - ?? Sebastien Cotillard - Genose.org
// https://github.com/genose
//
// *************************************
//
// Copyright 2009-2015 David Thorpe
// https://github.com/djthorpe/postgresql-kit
//
// Originaly Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

#import <Foundation/Foundation.h>


@class PGConnection;
@interface PGConnectionOperation : NSObject
{
    
    void *  _operationConnectionRef;
    id _operationConnectionClassRef;
    
    void *  _callbackWhenDone;
    void * _callbackWhenError;
    id      _operation;
    id      _operationInfo;
    id      _operationType;
    NSInteger _poolRefIdentifier;
    NSInteger _operationStatus;
    bool _invalidated;
    dispatch_semaphore_t semaphore;
    id resultsSet;
}
//(void(^)(id result,NSError* error)) 
-(instancetype)initWithParametersDelegate:(id)connectionDelegate withRefPoolIdentifier:(NSInteger)poolIdentifier refClassOperation:(id)operation callWhenDone:(void*) callBackBlockDone callWhenError:(void(^)(id result,NSError* error))  callBackBlockError;

-(PGConnection*)getConnectionDelegate;

-(dispatch_semaphore_t)semaphore;
-(NSInteger)poolIdentifier;

-(id)queryString;
-(id)UTF8String;

-(void)finish;
-(bool)valid;
-(void)validate;
-(void)invalidate;
-(void *)getCallback;

-(id)setResults:(id)results;
-(id)results;

@end
