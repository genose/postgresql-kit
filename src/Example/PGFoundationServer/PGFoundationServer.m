#import "PGFoundationServer.h"

@implementation PGFoundationServer

@synthesize signal;
@synthesize returnValue;
@dynamic dataPath;
@dynamic port;
@dynamic hostname;

////////////////////////////////////////////////////////////////////////////////
// properties

-(NSString* )dataPath {
	NSString* theIdent = @"PostgreSQL";
	NSArray* theApplicationSupportDirectory = NSSearchPathForDirectoriesInDomains(NSApplicationSupportDirectory,NSUserDomainMask, YES);
	NSParameterAssert([theApplicationSupportDirectory count]);
	return [[theApplicationSupportDirectory objectAtIndex:0] stringByAppendingPathComponent:theIdent];
}

-(NSUInteger)port {
	// retrieve port from NSUserDefaults
	if([[NSUserDefaults standardUserDefaults] objectForKey:@"port"]) {
		NSInteger port = [[NSUserDefaults standardUserDefaults] integerForKey:@"port"];
		if(port > 0) {
			return (NSUInteger)port;
		}
	}
	// retrieve port from configuration
	PGServerPreferences* configuration = [[self server] configuration];
	if(configuration==nil) {
		return 0;
	}
	return [configuration port];
}

-(NSString* )hostname {
	// retrieve hostname from NSUserDefaults
	NSString* hostname = [[NSUserDefaults standardUserDefaults] stringForKey:@"hostname"];
	if(hostname) {
		return hostname;
	}
	// retrieve port from configuration
	PGServerPreferences* configuration = [[self server] configuration];
	if(configuration==nil) {
		return nil;
	}
	return [configuration listenAddresses];
}

////////////////////////////////////////////////////////////////////////////////
// delegate methods

-(void)pgserver:(PGServer* )server message:(NSString* )message {
	printf("%s\n",[message UTF8String]);
}

-(void)pgserverStateChange:(PGServer* )sender {
	switch([sender state]) {
		case PGServerStateAlreadyRunning:
			// need to reload the server
			printf("Server is already running, restarting\n");
			[[self server] restart];
			break;
		case PGServerStateError:
			// error occured, so program should quit with -1 return value
			printf("Server error, quitting\n");
			[self setReturnValue:-1];
		case PGServerStateStopped:
			// quit the application
			printf("Server stopped, ending application\n");
			[self setSignal:-1];
			CFRunLoopStop([[NSRunLoop currentRunLoop] getCFRunLoop]);
			break;
		default:
			printf("Server state: %s\n",[[PGServer stateAsString:[sender state]] UTF8String]);
	}
}

////////////////////////////////////////////////////////////////////////////////
// start/stop methods

-(int)start {
	// create a server
	[self setServer:[PGServer serverWithDataPath:[self dataPath]]];
	// set server delegate
	[[self server] setDelegate:self];	
	// set success return value
	[self setReturnValue:0];
	
	// Report server version
	printf("Server version: %s",[[[self server] version] UTF8String]);
	
	// create a timer to fire once run loop is started
	[NSTimer scheduledTimerWithTimeInterval:0.1 target:self selector:@selector(timerFired:) userInfo:nil repeats:NO];
	
	// start the run loop
	double resolution = 300.0;
	BOOL isRunning;
	do {
		NSDate* theNextDate = [NSDate dateWithTimeIntervalSinceNow:resolution];
		isRunning = [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode beforeDate:theNextDate];
	} while(isRunning==YES && [self signal] >= 0);

	// return the code
	return [self returnValue];
}

-(void)stop {
	[[self server] stop];
}

////////////////////////////////////////////////////////////////////////////////
// we need to fire the timer once to actually start the server up

-(void)timerFired:(id)theTimer {
	PGServerState state = [[self server] state];
	if(state==PGServerStateUnknown) {
		[[self server] startWithNetworkBinding:[self hostname] port:[self port]];
	}
}

@end