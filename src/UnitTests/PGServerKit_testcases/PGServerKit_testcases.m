
#import <Foundation/Foundation.h>
#import <XCTest/XCTest.h>
#import <PGServerKit/PGServerKit.h>
#import "PGFoundationServer.h"

////////////////////////////////////////////////////////////////////////////////

PGFoundationServer* app = nil;
PGServer* server = nil;
NSString* dataPath = nil;

////////////////////////////////////////////////////////////////////////////////

@interface PGServerKit_testcases : XCTestCase

@end

////////////////////////////////////////////////////////////////////////////////

@implementation PGServerKit_testcases

-(void)setUp {
    [super setUp];
    // TODO
}

- (void)tearDown {
    // TODO
    [super tearDown];
}

////////////////////////////////////////////////////////////////////////////////

-(void)test_001 {
    XCTAssert(server==nil,@"Test 001A");
    XCTAssert(dataPath==nil,@"Test 001B");
}

-(void)test_002 {
	dataPath = [PGFoundationServer dataPath];
	server = [PGServer serverWithDataPath:dataPath];
    XCTAssert(server!=nil,@"Test 002");
}

-(void)test_003 {
    XCTAssert(PGServerSuperuser,@"Test 003");
}

-(void)test_004 {
    XCTAssert(PGServerDefaultPort,@"Test 004");
}

-(void)test_005 {
    XCTAssert([server state]==PGServerStateUnknown,@"Test 005");
}

-(void)test_006 {
    XCTAssert([server version],@"Test 006");
}

-(void)test_007 {
    XCTAssert([[server dataPath] isEqualToString:dataPath],@"Test 007");
	NSLog(@"dataPath=%@",[server dataPath]);
}

-(void)test_008 {
    XCTAssert([server socketPath]==nil,@"Test 008");
}

-(void)test_009 {
    XCTAssert([server hostname]==nil,@"Test 009");
}

-(void)test_010 {
    XCTAssert([server port]==0,@"Test 010");
}

-(void)test_011 {
	// pid should be -1 when class is initialized
    XCTAssert([server pid]==-1,@"Test 011");
}

-(void)test_012 {
    XCTAssert([server uptime]==0,@"Test 012");
}

-(void)test_013 {
    XCTAssert(app==nil,@"Test 013A");
	app = [[PGFoundationServer alloc] initWithServer:server];
    XCTAssert(app,@"Test 013B");
}

-(void)test_014 {
	if([app isStarted]) {
		[self test_999];
	}
	BOOL isSuccess = [app start];
	XCTAssert(isSuccess,@"Test 014A");
}

-(void)test_999 {
	if(app && [app isStarted]) {
		BOOL isSuccess = [app stop];
		XCTAssert(isSuccess,@"Test 999A");
	}
	NSUInteger counter = 0;
	while([app isStopped]==NO) {
		[NSThread sleepForTimeInterval:0.5];
		XCTAssert(counter < 10,@"Test 999B");
		counter++;
	}
}


@end
