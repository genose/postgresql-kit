
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
#import <PGServerKit/PGServerKit.h>

@interface PGFoundationServer : NSObject  <PGServerDelegate> {
	PGServer* _server;
	BOOL _stop;
	BOOL _terminate;
}

// static methods
+(NSString* )defaultDataPath;

// constructor
-(id)init;
-(id)initWithServer:(PGServer* )server;

// properties
@property (readonly) BOOL isStarted;
@property (readonly) BOOL isStopped;
@property (readonly) BOOL isError;
@property (readonly) NSString* dataPath;
@property (readonly) int pid;
@property (readonly) NSUInteger port;
@property (readonly) PGServer* pgserver;

// methods
-(BOOL)start;
-(BOOL)startWithPort:(NSUInteger)port;
-(BOOL)stop;
-(BOOL)deleteData;

@end
