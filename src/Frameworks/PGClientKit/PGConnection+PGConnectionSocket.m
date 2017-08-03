//
//  PGConnection+PGConnectionSocket.m
//  postgresql-kit

    // *************************************
    //
    // Copyright 2017 - ?? Sebastien Cotillard - Genose.org
    // 07/2017 Sebastien Cotillard
    // https://github.com/genose
    //
    // *************************************
    // ADDING Pool concurrent operation
    // *************************************
    // ADDING Fraking CFSocket non-blocking Main Thread respons and concurrent operation
    // *************************************

#import "PGConnection+PGConnectionSocket.h"
extern void _socketCallback;
@implementation PGConnection (PGConnectionSocket)
-(CFSocketRef)__CFSocket_instanciate
{

        // create socket object
    CFSocketContext context = {0, (__bridge void* )(self), NULL, NULL, NULL};

    _socket = CFSocketCreate(kCFAllocatorDefault, 0, 0, 0, kCFSocketReadCallBack | kCFSocketWriteCallBack, &_socketCallback,&context);

    PQsocket(_connection);

//    _socket = CFSocketCreateWithNative(kCFAllocatorDefault,PQsocket(_connection),kCFSocketReadCallBack | kCFSocketWriteCallBack,&_socketCallback,&context);


}


    ////////////////////////////////////////////////////////////////////////////////
#pragma mark private methods - socket connect/disconnect
    ////////////////////////////////////////////////////////////////////////////////

-(void)_socketConnect:(PGConnectionState)state {
    NSParameterAssert(_state==PGConnectionStateNone);
    NSParameterAssert(state==PGConnectionStateConnect || state==PGConnectionStateReset || state==PGConnectionStateNone);
    NSParameterAssert(_connection);
    NSParameterAssert(_socket==nil && _runloopsource==nil);


    [self __CFSocket_instanciate];

        //    _socket = CFSocketCreate(kCFAllocatorDefault, 0, 0, 0, kCFSocketReadCallBack | kCFSocketWriteCallBack,&_socketCallback,&context);

    NSParameterAssert(_socket && CFSocketIsValid(_socket));
        // let libpq do the socket closing
    CFSocketSetSocketFlags(_socket,~kCFSocketCloseOnInvalidate & CFSocketGetSocketFlags(_socket));
        //    CFSocketEnableCallBacks(_socket, kCFSocketDataCallBack | kCFSocketReadCallBack | kCFSocketWriteCallBack);


    dispatch_source_t dsrc = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0,  dispatch_get_current_queue() );

    dispatch_source_set_timer(dsrc, dispatch_time(DISPATCH_TIME_NOW, 0), NSEC_PER_SEC / 2, NSEC_PER_SEC);

        //	cf(_socket, F_SETFL,  O_NONBLOCK);
        // set state
    [self setState:state];
    [self _updateStatus];

        // add to run loop to begin polling
    _runloopsource = CFSocketCreateRunLoopSource(NULL,_socket,128);
    NSParameterAssert(_runloopsource && CFRunLoopSourceIsValid(_runloopsource));
    CFRunLoopAddSource(
                       //                       CFRunLoopGetMain(),
                       CFRunLoopGetCurrent(),
                       _runloopsource,(CFStringRef)kCFRunLoopCommonModes);
#if defined DEBUG && defined DEBUG2
    NSLog(@"%@ :: %@ :::: Socket created ....", NSStringFromClass([self class]), NSStringFromSelector(_cmd));
#endif

    dispatch_semaphore_t semaphore_query_send = [[self masterPoolOperation] semaphore];
    [self wait_semaphore_read: semaphore_query_send ];


        //    CFRunLoopRun();//(kCFRunLoopDefaultMode, 0.2 , NO);
#if defined DEBUG && defined DEBUG2
    NSLog(@" ------- %@ :: %@ :::: Socket Runloop Started ....", NSStringFromClass([self class]), NSStringFromSelector(_cmd));
#endif
}

-(void)_socketDisconnect {
    if(_runloopsource) {
        CFRunLoopSourceInvalidate(_runloopsource);
        CFRelease(_runloopsource);
        _runloopsource = nil;
    }
    if(_socket) {
        CFSocketInvalidate(_socket);
        CFRelease(_socket);
        _socket = nil;
    }
}

@end
