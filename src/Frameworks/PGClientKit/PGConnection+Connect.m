
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

#import <PGClientKit/PGClientKit.h>
#import <PGClientKit/PGClientKit+Private.h>

////////////////////////////////////////////////////////////////////////////////
#pragma mark Key Value pair construction

void freeKVPairs(PGKVPairs* pairs) {
	if(pairs) {
		free(pairs->keywords);
		free(pairs->values);
		free(pairs);
	}
}

PGKVPairs* allocKVPairs(NSUInteger size) {
	PGKVPairs* pairs = malloc(sizeof(PGKVPairs));
	if(pairs==nil) {
		return nil;
	}
	pairs->keywords = malloc(sizeof(const char* ) * (size+1));
	pairs->values = malloc(sizeof(const char* ) * (size+1));
	if(pairs->keywords==nil || pairs->values==nil) {
		freeKVPairs(pairs);
		return nil;
	}
	return pairs;
}

PGKVPairs* makeKVPairs(NSDictionary* dict) {
	PGKVPairs* pairs = allocKVPairs([dict count]);	
	NSUInteger i = 0;
	for(NSString* theKey in dict) {
		pairs->keywords[i] = [theKey UTF8String];
		pairs->values[i] = [[[dict valueForKey:theKey] description] UTF8String];
		i++;
	}
	pairs->keywords[i] = '\0';
	pairs->values[i] = '\0';
	return pairs;
}

@implementation PGConnection (Connect)

////////////////////////////////////////////////////////////////////////////////
#pragma mark private methods - connections
////////////////////////////////////////////////////////////////////////////////

/**
 *  Returns a dictionary of connection parameters for a URL, or nil if the URL
 *  is somehow invalid. Will add on connect_timeout, client_encoding and
 *  application_name parameters if they are not already added into the URL
 */
-(NSDictionary* )_connectionParametersForURL:(NSURL* )theURL {
	// make parameters from the URL
	NSMutableDictionary* theParameters = [[theURL postgresqlParameters] mutableCopy];
	if(theParameters==nil) {
		return nil;
	}
	if([self timeout]) {
		[theParameters setValue:[NSNumber numberWithUnsignedInteger:[self timeout]] forKey:@"connect_timeout"];
	}
	// set client encoding and application name if not already set
	if([theParameters objectForKey:@"client_encoding"]==nil) {
		[theParameters setValue:PGConnectionDefaultEncoding forKey:@"client_encoding"];
	}
	if([theParameters objectForKey:@"application_name"]==nil) {
		[theParameters setValue:[[NSProcessInfo processInfo] processName] forKey:@"application_name"];
	}
	// Allow delegate to make changes to the parameters
	if([[self delegate] respondsToSelector:@selector(connection:willOpenWithParameters:)]) {
		[[self delegate] connection:self willOpenWithParameters:theParameters];
	}
	return theParameters;
}

////////////////////////////////////////////////////////////////////////////////
#pragma mark public methods - connections
////////////////////////////////////////////////////////////////////////////////
-(void) _reconnectWithHandler: ( void(^ _Nullable )(void * pm,NSError* error)) callback
{
//    if(_connection && _socket)
//    {
//        [self disconnect];
//    }
    
    if(_connection && callback == nil){
        PQfinish(_connection);
        _connection = nil;
    }
    if(_socket && callback == nil){
        [self _socketDisconnect];
        _socket = nil;
    }
    
    if(!_connection && !_socket)
    {
        NSLog(@" %@ ::  %@",NSStringFromSelector(_cmd), callback);
        // extract connection parameters
        NSDictionary* parameters = [self _connectionParametersForURL: _connectedUrl];
        if(parameters==nil) {
           if(callback != nil) callback(NO,[self raiseError:nil code:PGClientErrorParameters]);
            return;
        }
        
        // update the status as necessary
        [self _updateStatus];
        
        // create parameter pairs
        PGKVPairs* pairs = makeKVPairs(parameters);
        if(pairs==nil) {
           if(callback != nil) callback(NO,[self raiseError:nil code:PGClientErrorParameters]);
            return;
        }
        
        // create connection
        _connection = PQconnectStartParams(pairs->keywords,pairs->values,0);
        freeKVPairs(pairs);
        if(_connection==nil) {
            if(callback != nil) callback(NO,[self raiseError:nil code:PGClientErrorParameters]);
            return;
        }
        
        _connectionClosed = NO;
        
        // check for initial bad connection status
        if(PQstatus(_connection)==CONNECTION_BAD) {
            PQfinish(_connection);
            _connection = nil;
            [self _updateStatus];
            if(callback != nil) callback(NO,[self raiseError:nil code:PGClientErrorParameters]);
            return;
        }
        // not quite good for cascaded Operation
        //::	_callback = (__bridge_retained void* )[callback copy];
        // So we do good things to deal with cascaded Operation
        if(callback != nil){
            if(CFArrayGetCount(_callbackOperationPool))
                id mp = [[self masterPoolOperation] class];
        
            [self addOperation:self withCallBackWhenDone: (__bridge_retained void* )callback withCallBackWhenError: (__bridge_retained void* )callback ];
        }else{
             if( ! CFArrayGetCount(_callbackOperationPool))
                 [self addOperation:self withCallBackWhenDone: (__bridge_retained void* )callback withCallBackWhenError: (__bridge_retained void* )callback ];
            [self _socketConnect:PGConnectionStateConnect];
        }
        
        NSLog(@" %@ :: connection Initialized (%p :: connection : %p :: socket : %p) ... ",NSStringFromSelector(_cmd), self, _connection, _socket);

    }else{
        NSLog(@" %@ :: connection seems good ... ",NSStringFromSelector(_cmd));
    }
}

-(void)connectWithURL:(NSURL* )url whenDone:(void(^)(BOOL usedPassword,NSError* error)) callback {
	NSParameterAssert(url!=nil);
	NSParameterAssert(callback!=nil);
    @try {
        
	// check for bad initial state
	if(_connection != nil ) {
		callback(NO,[self raiseError:nil code:PGClientErrorState]);
		return;
	}
     PGConnectionState curstate = [self state];
     if( curstate != PGConnectionStateNone)
     {
         NSLog(@" :: Warning :: %@ :: %@ :: \n :: Connection previously initialized ::", NSStringFromSelector(_cmd), self);
         ;;
     }
    
	// check other internal variable consistency
	NSParameterAssert(_connection==nil);
	NSParameterAssert(_socket==nil);
	NSParameterAssert(_runloopsource==nil);
        
            _connectedUrl = url;
        
#if ( defined(__IPHONE_10_3) &&  __IPHONE_OS_VERSION_MAX_ALLOWED  > __IPHONE_10_3 ) || ( defined(MAC_OS_X_VERSION_10_12) && MAC_OS_X_VERSION_MAX_ALLOWED > MAC_OS_X_VERSION_10_12 )
    [NSThread detachNewThreadWithBlock:^{
#else

//     dispatch_async(dispatch_get_main_queue(),^{
#endif

        // set callback
        NSParameterAssert(callback!=nil);
        
//        void (^callbackRecall)(bool result,NSError* error) = (__bridge void (^)(bool ,NSError* ))( callback);
        void (^ _Nullable callbackRecall)(void * pm,NSError* error) = ( void (^ _Nullable )(void* ,NSError* ))( callback );
        
        [self _reconnectWithHandler: callbackRecall];
	 
    
        
   #if ( defined(__IPHONE_10_3) &&  __IPHONE_OS_VERSION_MAX_ALLOWED  > __IPHONE_10_3 ) || ( defined(MAC_OS_X_VERSION_10_12) && MAC_OS_X_VERSION_MAX_ALLOWED > MAC_OS_X_VERSION_10_12 )
    } ];
#else

//                   } );
#endif
	// add socket to run loop
#if ( defined(__IPHONE_10_3) &&  __IPHONE_OS_VERSION_MAX_ALLOWED  > __IPHONE_10_3 ) || ( defined(MAC_OS_X_VERSION_10_12) && MAC_OS_X_VERSION_MAX_ALLOWED > MAC_OS_X_VERSION_10_12 )
    [NSThread detachNewThreadWithBlock:^
#else
//        dispatch_get_current_queue
//             dispatch_async(dispatch_get_current_queue(),^
#endif
        {
#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@" ------- %@ :: %@ :::: Connection Started ....", NSStringFromClass([self class]), NSStringFromSelector(_cmd));
#endif
            
            // start queued request
            [self _socketConnect:PGConnectionStateConnect];
            
//            if( _socket == nil)
//            {
//                NSLog(@" ERROR :: Premature Closing of socket .... (%@) ", self);
//             
//                if(_connection)
//                    PQfinish(_connection);
//                
//                _connection = nil;
//                
//                callback(NO,[self raiseError:nil code:PGClientErrorUnknown]);
//                
//            }
//            CFRunLoopRun();
//    [self performSelector:@selector(_waitingPoolOperationForResult) withObject:self ];
//    [self performSelector:@selector(_waitingPoolOperationForResultMaster) withObject:self ];

#if defined(DEBUG)  && defined(DEBUG2) && DEBUG == 1 && DEBUG2 == 1
            NSLog(@" ------- %@ :: %@ :::: Connection STOPPed ....", NSStringFromClass([self class]), NSStringFromSelector(_cmd));
#endif
            
        }
#if ( defined(__IPHONE_10_3) &&  __IPHONE_OS_VERSION_MAX_ALLOWED  > __IPHONE_10_3 ) || ( defined(MAC_OS_X_VERSION_10_12) && MAC_OS_X_VERSION_MAX_ALLOWED > MAC_OS_X_VERSION_10_12 )
    ]
#else
    
//    )
#endif
            ;

    } @catch (NSException *exception) {
        NSLog(@"**************************** \n ERROR connection init :: %@ :: \n **************************** \n [ %@ ] \n **************************** \n ", NSStringFromSelector(_cmd), exception);
        return;
    } @finally {
        
    }

}

-(BOOL)connectWithURL:(NSURL* )url usedPassword:(BOOL* )usedPassword error:(NSError** )error {
	// not yet implemented!
	[self raiseError:error code:PGClientErrorUnknown reason:@"connectWithURL: not yet implemented"];
	return NO;
}
/*


	dispatch_semaphore_t s = dispatch_semaphore_create(0);
	__block BOOL returnValue = NO;
	[self connectWithURL:url whenDone:^(BOOL p, NSError* e) {
		if(usedPassword) {
			(*usedPassword) = p;
		}
		if(error) {
			(*error) = e;
		}
		if(e) {
			returnValue = NO;
		}
		dispatch_semaphore_signal(s);
	}];
	dispatch_semaphore_wait(s,DISPATCH_TIME_FOREVER);
	return returnValue;
}
*/

@end


