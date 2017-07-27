// *************************************
//
// Copyright 2017 - ?? Sebastien Cotillard - Genose.org
// 07/2017 Sebastien Cotillard
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


#import "PGConnectionOperation.h"
#import <PGClientKit/PGClientKit.h>

@implementation PGConnectionOperation

-(instancetype)init
{
    self = [super init];
    if( self == nil) return nil;
    
    
    _operation = [NSObject new];
    _operationInfo = nil;
    _callbackWhenDone = NULL;
    _callbackWhenError = NULL;
    invalidated = YES;
    return self;
    
}
-(instancetype)initWithParametersDelegate:(id)connectionDelegate withRefPoolIdentifier:(NSInteger)poolIdentifier refClassOperation:(id)operation callWhenDone:(void*)  callBackBlockDone callWhenError:(void*)  callBackBlockError
{
    // void (^callback)(PGResult* result,NSError* error) = (__bridge void (^)(PGResult* ,NSError* ))(_callback);
    self = [[[self class] alloc ] init];
    if(self != nil)
    {
        _operationConnectionClassRef = connectionDelegate;
//        _operationConnectionRef = ((PGConnection*)_operationConnectionClassRef)->_connection;
        
        
        _operationType = [((NSObject*)operation) class];
        _operation = operation;
        
        
        poolRefIdentifier = poolIdentifier;
        
        _callbackWhenDone = (__bridge_retained void* )([(__bridge void (^)(void* ,void* ))(callBackBlockDone)  copy]);//(__bridge void (^)(PGResult* ,NSError* ))(callBackBlockDone);
        _callbackWhenError = (__bridge_retained void* )([(__bridge void (^)(void* ,void* ))(callBackBlockError)   copy]);//(__bridge void (^)(PGResult* ,NSError* ))(callBackBlockDone);
        invalidated = NO;
    }else{
        return nil;
    }
    return self;
}
-(bool)valid
{
    return !invalidated;
}
-(void)invalidate
{
    NSLog(@" %@::%@ :: INVALIDATE pool (%d :: %@ ) .... ", NSStringFromClass([self class]), NSStringFromSelector(_cmd), poolRefIdentifier, [self description]);
    if(poolRefIdentifier !=0)
        [_operationConnectionClassRef invalidateOperation: poolRefIdentifier];
    invalidated = TRUE;
}
-(PGConnection*)getConnectionDelegate
{
    return _operationConnectionClassRef;
    
}

-(void *)getCallback
{
//    if(invalidated){
//        _callbackWhenDone = nil;
//    };
    return _callbackWhenDone;
}
-(id)UTF8String
{
    if([_operation respondsToSelector:@selector(UTF8String)])
    {
        return [_operation UTF8String];
    }
    
    return nil;
}
-(id)queryString
{
    if([_operation respondsToSelector:@selector(queryString)])
    {
        return [_operation queryString];
    }
    
    return nil;
}
-(id)string
{
    id queryStringDescription = [self queryString];
    if(queryStringDescription != nil)
    {
        return queryStringDescription;
    }
    
    if([_operation isKindOfClass:[NSString class]] && [_operation respondsToSelector:@selector(UTF8String)])
    {
        return [NSString stringWithFormat:@"<%@> : %@ ",NSStringFromClass([_operation class]), _operation ];
    }
    
    if([_operation respondsToSelector:@selector(string)])
    {
        return [_operation string];
    }
    

    
    return @"<no description>";
}
-(NSString *)description
{
    return [NSString stringWithFormat:@"<%@:%p> \n OperationType : %@ \n Connection Delegate : <%@:%p> \n Operation (%@)",NSStringFromClass([self class]), self, _operationType, NSStringFromClass([_operationConnectionClassRef class ]), _operationConnectionClassRef, [self string] ];
}
@end
