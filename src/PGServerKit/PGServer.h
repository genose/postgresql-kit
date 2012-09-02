
#import <Foundation/Foundation.h>
#import "PGServerKit.h"

extern NSInteger PGServerDefaultPort;

@interface PGServer : NSObject {
	PGServerState _state;
}
@property id delegate;
@property PGServerState state;
@property (retain) NSString* hostname;
@property NSInteger port;
@property (readonly) NSString* version;
@property (retain) NSString* dataPath;
@property (assign) int pid;

// return shared server object
+(PGServer* )sharedServer;

// start, stop and reload server
-(BOOL)startWithDataPath:(NSString* )thePath;
-(BOOL)stop;
-(BOOL)reload;

// utility methods
+(NSString* )stateAsString:(PGServerState)theState;

@end
