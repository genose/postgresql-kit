
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

@implementation PGConnection (Disconnect)

-(void)disconnect {
    @synchronized (self) {
        [self _cancelDestroy];
        [self _socketDisconnect];
        if(_connection != NULL) {
            
           
            PQfinish(_connection);
            _connection = nil;
            _parameters = nil;
            _connectionClosed = YES;
            
        }
         NSLog(@" %@ (%p) :: %@ :: Close connection pool (%ld) :: (%@) .... ", NSStringFromClass([self class]), self, NSStringFromSelector(_cmd), CFArrayGetCount(_callbackOperationPool), _connectedUrl);
    }
    [self _updateStatus];
}

@end



