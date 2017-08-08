
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

NSDictionary* PGClientErrorDomainCodeDescription = nil;

@implementation PGConnection (Errors)

////////////////////////////////////////////////////////////////////////////////
#pragma mark private methods - error handling
////////////////////////////////////////////////////////////////////////////////

/**
 *  Method to generate a new NSError object
 */
-(NSError* )_errorWithCode:(int)errorCode reason:(NSString* )format,... {
	static dispatch_once_t onceToken;
    dispatch_once(&onceToken,^{
        // Do some work that happens once
		PGClientErrorDomainCodeDescription = @{
			[NSNumber numberWithInt:PGClientErrorNone]: @"No Error",
			[NSNumber numberWithInt:PGClientErrorState]: @"Connection State Mismatch",
			[NSNumber numberWithInt:PGClientErrorParameters]: @"Invalid connection parameters",
			[NSNumber numberWithInt:PGClientErrorNeedsPassword]: @"Connection requires authentication",
			[NSNumber numberWithInt:PGClientErrorInvalidPassword]: @"Password authentication failed",
			[NSNumber numberWithInt:PGClientErrorRejected]: @"Connection was rejected",
			[NSNumber numberWithInt:PGClientErrorExecute]: @"Execution error",
			[NSNumber numberWithInt:PGClientErrorQuery]: @"Query error",
			[NSNumber numberWithInt:PGClientErrorUnknown]: @"Unknown or internal error"
		};
    });
	NSString* reason = nil;
	if(format) {
		va_list args;
		va_start(args,format);
		reason = [[NSString alloc] initWithFormat:format arguments:args];
		va_end(args);
	}
	if(reason==nil) {
		reason = [PGClientErrorDomainCodeDescription objectForKey:[NSNumber numberWithInt:errorCode]];
	}
	if(reason==nil) {
		reason = [PGClientErrorDomainCodeDescription objectForKey:[NSNumber numberWithInt:PGClientErrorUnknown]];
	}
	NSParameterAssert(reason);
	return [NSError errorWithDomain:PGClientErrorDomain code:errorCode userInfo:@{
		NSLocalizedDescriptionKey: reason
	}];
}

-(NSError* )_errorWithCode:(int)errorCode {
	return [self _errorWithCode:errorCode reason:nil];
}

-(void)_raiseError:(NSError* )error {
	NSParameterAssert(error);
	// perform selector
	if([[self delegate] respondsToSelector:@selector(connection:error:)]) {
#if defined DEBUG && defined DEBUG2
        NSLog(@"PGConnectionStateQuery - _raiseError :: send to delegate connection:error:" );
#endif
//		[[self delegate] connection:self error:error];
// @{@"connection":self, @"error":error}

//         dispatch_queue_t qu_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
//         dispatch_barrier_sync(qu_inRun, ^ {
            	[[self delegate] connection:self error:error];
//            [NSThread detachNewThreadSelector:@selector(connection:error:) toTarget:[self delegate] withObject:@{@"connection":self, @"error":error} ];
    #if defined DEBUG && defined DEBUG2
     NSLog(@"PGConnectionStateQuery - _raiseError :: send to delegate connection:error: END" );
#endif
//} );
        
        }else{
#if defined DEBUG && defined DEBUG2
        NSLog(@"PGConnectionStateQuery - _raiseError :: no delegate respondsto connection:error:" );
#endif
    }
}

-(NSError* )raiseError:(NSError** )error code:(PGClientErrorDomainCode)code reason:(NSString* )format,...  {
	// format the reason
	NSString* reason = nil;
	if(format) {
		va_list args;
		va_start(args,format);
		reason = [[NSString alloc] initWithFormat:format arguments:args];
		va_end(args);
	}
	// create the error
	NSError* theError = [self _errorWithCode:code reason:reason];
	if(error) {
		(*error) = theError;
	}
	// raise the error with the delegate
	if([self delegate]) {
#if defined DEBUG && defined DEBUG2
        NSLog(@"PGConnectionStateQuery - _raiseError :: performSelectorOnMainThread :: (%@)", theError );
#endif
        
        
//         [self performSelectorOnMainThread:@selector(_raiseError:) withObject:theError waitUntilDone:YES];
        
        [self _raiseError:theError];
    }else{
#if defined DEBUG && defined DEBUG2
        NSLog(@"PGConnectionStateQuery - _raiseError:code: :: no delegate avail" );
#endif
    }
#if defined DEBUG && defined DEBUG2
    NSLog(@"PGConnectionStateQuery - _raiseError :: return " );
#endif
	return theError;
}

-(NSError* )raiseError:(NSError** )error code:(PGClientErrorDomainCode)code {
	return [self raiseError:error code:code reason:nil];
}

@end


