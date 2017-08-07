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


#include <CoreFoundation/CFSocket.h>

//#include "<CoreFoundation/CFInternal.h>"
#include <dispatch/dispatch.h>
#include <netinet/in.h>
#include <sys/sysctl.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <dlfcn.h>


extern void _CFRunLoopSourceWakeUpRunLoops(CFRunLoopSourceRef rls);
extern void  __CFRunLoopSourceWakeUpLoop(const void*,void*);

extern void _socketCallback;
static CFMutableArrayRef __CFCF_CFAllSockets = NULL;

#define INVALID_SOCKET (CFSocketNativeHandle)(-1)
#define MAX_SOCKADDR_LEN 256
#define __CFCF_CFSockQueue()  dispatch_get_current_queue()

@implementation PGConnection (PGConnectionSocket)
void _CFRunLoopSourceWakeUpRunLoops(CFRunLoopSourceRef rls) {
    return;
    //    CFBagRef loops = NULL;
    //    __CFRunLoopSourceLock(rls);
    //    if (__CFIsValid(rls) && NULL != rls->_runLoops) {
    //        loops = CFBagCreateCopy(kCFAllocatorSystemDefault, rls->_runLoops);
    //    }
    //    __CFRunLoopSourceUnlock(rls);
    //    if (loops) {
    //        CFBagApplyFunction(loops, __CFRunLoopSourceWakeUpLoop, NULL);
    //        CFRelease(loops);
    //    }
}
CFSocketRef _CFCF_CFSocketCreateWithNative(CFAllocatorRef allocator, CFSocketNativeHandle ufd, CFOptionFlags callBackTypes, CFSocketCallBack callout, const CFSocketContext *context) {
#if defined(CHECK_FOR_FORK_RET)
    CHECK_FOR_FORK_RET(NULL);
#endif
    if(__CFCF_CFAllSockets == nil)
        __CFCF_CFAllSockets = CFArrayCreateMutable(kCFAllocatorSystemDefault, 0, &kCFTypeArrayCallBacks);
    CFSocketGetTypeID(); // cause initialization if necessary
    
    struct stat statbuf;
    int ret = fstat(ufd, &statbuf);
    if (ret < 0) ufd = INVALID_SOCKET;
    
    Boolean sane = false;
    if (INVALID_SOCKET != ufd) {
        uint32_t type = (statbuf.st_mode & S_IFMT);
        sane = (S_IFSOCK == type) || (S_IFIFO == type) || (S_IFCHR == type);
        if (0 && !sane) {
            CFLog(kCFLogLevelWarning, CFSTR("*** _CFCF_CFSocketCreateWithNative(): creating CFSocket with silly fd type (%07o) -- may or may not work"), type);
        }
    }
    
    if (INVALID_SOCKET != ufd) {
        Boolean canHandle = false;
        int tmp_kq = kqueue();
        if (0 <= tmp_kq) {
            struct kevent ev[2];
            EV_SET(&ev[0], ufd, EVFILT_READ, EV_ADD, 0, 0, 0);
            EV_SET(&ev[1], ufd, EVFILT_WRITE, EV_ADD, 0, 0, 0);
            int ret = kevent(tmp_kq, ev, 2, NULL, 0, NULL);
            canHandle = (0 <= ret); // if kevent(ADD) succeeds, can handle
            close(tmp_kq);
        }
        if (0 && !canHandle) {
            CFLog(kCFLogLevelWarning, CFSTR("*** _CFCF_CFSocketCreateWithNative(): creating CFSocket with unsupported fd type -- may or may not work"));
        }
    }
    
    if (INVALID_SOCKET == ufd) {
        // Historically, bad ufd was allowed, but gave an uncached and already-invalid CFSocketRef
        SInt32 size = sizeof(struct __CFSocket) - sizeof(CFRuntimeBase);
        CFSocketRef memory = (CFSocketRef)_CFRuntimeCreateInstance(allocator, CFSocketGetTypeID(), size, NULL);
        if (NULL == memory) {
            return NULL;
        }
        memory->_callout = callout;
        memory->_state = kCFSocketStateInvalid;
        return memory;
    }
    
    __block CFSocketRef sock = NULL;
    dispatch_sync(__CFCF_CFSockQueue(), ^{
        @try {
            
            for (CFIndex idx = 0, cnt = CFArrayGetCount(__CFCF_CFAllSockets); idx < cnt; idx++) {
                CFSocketRef s = (CFSocketRef)CFArrayGetValueAtIndex(__CFCF_CFAllSockets, idx);
                if (s->_shared->_socket == ufd) {
                    CFRetain(s);
                    sock = s;
                    return;
                }
            }
            
            SInt32 size = sizeof(struct __CFSocket) - sizeof(CFRuntimeBase);
            __CF_CFSocketRef memory = malloc(sizeof(__CF_CFSocketRef));
             CFSocketRef cfcf_memory  = (CFSocketRef)_CFRuntimeCreateInstance(allocator, CFSocketGetTypeID(), size, NULL);
            if (NULL == memory) {
                return;
            }
            
            int socketType = 0;
            if (INVALID_SOCKET != ufd) {
                socklen_t typeSize = sizeof(socketType);
                int ret = getsockopt(ufd, SOL_SOCKET, SO_TYPE, (void *)&socketType, (socklen_t *)&typeSize);
                if (ret < 0) socketType = 0;
            }
            
            ((CFSocketRef)cfcf_memory)->_error = 0;
            
            memory->_rsuspended = true;
            memory->_wsuspended = true;
            memory->_readable = false;
            memory->_writeable = false;
            
            memory->_isSaneFD = sane ? 1 : 0;
            memory->_wantReadType = (callBackTypes & 0x3);
            memory->_reenableRead = memory->_wantReadType ? true : false;
            memory->_readDisabled = false;
            memory->_wantWrite = (callBackTypes & kCFSocketWriteCallBack) ? true : false;
            memory->_reenableWrite = false;
            memory->_writeDisabled = false;
            memory->_wantConnect = (callBackTypes & kCFSocketConnectCallBack) ? true : false;
            memory->_connectDisabled = false;
            memory->_leaveErrors = false;
            memory->_closeOnInvalidate = true;
            memory->_connOriented = (SOCK_STREAM == socketType || SOCK_SEQPACKET == socketType);
            memory->_connected = (memory->_wantReadType == kCFSocketAcceptCallBack || !memory->_connOriented) ? true : false;
            
            memory->_error = 0;
            memory->_runLoopCounter = 0;
            memory->_address = NULL;
            memory->_peerAddress = NULL;
            memory->_context.info = NULL;
            memory->_context.retain = NULL;
            memory->_context.release = NULL;
            memory->_context.copyDescription = NULL;
            memory->_callout = callout;
            if (NULL != context) {
                objc_memmove_collectable(&memory->_context, context, sizeof(CFSocketContext));
                memory->_context.info = context->retain ? (void *)context->retain(context->info) : context->info;
            }
            
            struct __shared_blob *shared = malloc(sizeof(struct __shared_blob));
            shared->_rdsrc = NULL;
            shared->_wrsrc = NULL;
            shared->_source = NULL;
            shared->_socket = ufd;
            shared->_closeFD = true; // copy of _closeOnInvalidate
            shared->_refCnt = 1; // one for the CFSocket
            memory->_shared = shared;
            
            if (memory->_wantReadType) {
                dispatch_source_t dsrc = NULL;
                if (sane != nil) {
                    dsrc = dispatch_source_create(DISPATCH_SOURCE_TYPE_READ, ufd, 0, __CFCF_CFSockQueue());
                } else {
                    dsrc = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, __CFCF_CFSockQueue());
                    dispatch_source_set_timer(dsrc, dispatch_time(DISPATCH_TIME_NOW, 0), NSEC_PER_SEC / 2, NSEC_PER_SEC);
                }
                dispatch_block_t event_block = ^{
                    memory->_readable = true;
                    if (!memory->_rsuspended) {
                        dispatch_suspend(dsrc);
                        memory->_rsuspended = true;
                    }
                    if (shared->_source) {
                        CFRunLoopSourceSignal(shared->_source);
                        _CFRunLoopSourceWakeUpRunLoops(shared->_source);
                    }
                };
                dispatch_block_t cancel_block = ^{
                    shared->_rdsrc = NULL;
                    shared->_refCnt--;
                    if (0 == shared->_refCnt) {
                        if (shared->_closeFD) close(shared->_socket);
                        free(shared);
                    }
                    //                dispatch_release(dsrc);
                };
                dispatch_source_set_event_handler(dsrc, event_block);
                dispatch_source_set_cancel_handler(dsrc, cancel_block);
                shared->_rdsrc = dsrc;
            }
            if (memory->_wantWrite || memory->_wantConnect) {
                dispatch_source_t dsrc = NULL;
                if (sane != nil) {
                    dsrc = dispatch_source_create(DISPATCH_SOURCE_TYPE_WRITE, ufd, 0, __CFCF_CFSockQueue());
                } else {
                    dsrc = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, __CFCF_CFSockQueue());
                    dispatch_source_set_timer(dsrc, dispatch_time(DISPATCH_TIME_NOW, 0), NSEC_PER_SEC / 2, NSEC_PER_SEC);
                }
                dispatch_block_t event_block = ^{
                    memory->_writeable = true;
                    if (!memory->_wsuspended) {
                        dispatch_suspend(dsrc);
                        memory->_wsuspended = true;
                    }
                    if (shared->_source) {
                        CFRunLoopSourceSignal(shared->_source);
                        _CFRunLoopSourceWakeUpRunLoops(shared->_source);
                    }
                };
                dispatch_block_t cancel_block = ^{
                    shared->_wrsrc = NULL;
                    shared->_refCnt--;
                    if (0 == shared->_refCnt) {
                        if (shared->_closeFD) close(shared->_socket);
                        free(shared);
                    }
                    //                dispatch_release(dsrc);
                };
                dispatch_source_set_event_handler(dsrc, event_block);
                dispatch_source_set_cancel_handler(dsrc, cancel_block);
                shared->_wrsrc = dsrc;
            }
            
            if (shared->_rdsrc) {
                shared->_refCnt++;
            }
            if (shared->_wrsrc) {
                shared->_refCnt++;
            }
            
            memory->_state = kCFSocketStateReady;
            CFIndex indexInPool = CFArrayGetCount(__CFCF_CFAllSockets);
           
            CFArrayAppendValue(__CFCF_CFAllSockets, cfcf_memory); 
            sock =  memory;
            
        } @catch (NSException *exception) {
            NSLog(@" :::::: Error   :: %@ ", exception );
        } @finally {
            
        }
    });
    CFLog(5, CFSTR("_CFCF_CFSocketCreateWithNative(): created socket %p with callbacks 0x%x"), sock, callBackTypes);
    return sock;
}




-(CFSocketRef)__CFSocket_instanciate
{
    
    // create socket object
    CFSocketContext context = {0, (__bridge void* )(self), NULL, NULL, NULL};
    
    //    _socket = CFSocketCreate(kCFAllocatorDefault, 0, 0, 0, kCFSocketReadCallBack | kCFSocketWriteCallBack, &_socketCallback,&context);
    
    //    PQsocket(_connection);
    
    _socket = CFSocketCreateWithNative(kCFAllocatorDefault,PQsocket(_connection),kCFSocketReadCallBack | kCFSocketWriteCallBack,&_socketCallback,&context);
    
    
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
    
//    NSParameterAssert(_socket && CFSocketIsValid(_socket));
    // let libpq do the socket closing
    CFSocketSetSocketFlags(_socket,~kCFSocketCloseOnInvalidate & CFSocketGetSocketFlags(_socket));
    //    CFSocketEnableCallBacks(_socket, kCFSocketDataCallBack | kCFSocketReadCallBack | kCFSocketWriteCallBack);
    
    dispatch_queue_t qu_inRun = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_BACKGROUND, 0);
    
    dispatch_source_t dsrc = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0,  qu_inRun );
    
    dispatch_source_set_timer(dsrc, dispatch_time(DISPATCH_TIME_NOW, 0), NSEC_PER_SEC / 2, NSEC_PER_SEC);
    
    //	cf(_socket, F_SETFL,  O_NONBLOCK);
    // set state
    [self setState:state];
    [self _updateStatus];
    
    // add to run loop to begin polling
    _runloopsource = CFSocketCreateRunLoopSource(NULL,_socket,1);
    NSParameterAssert(_runloopsource && CFRunLoopSourceIsValid(_runloopsource));
    CFRunLoopAddSource(
                       //                       CFRunLoopGetMain(),
                       CFRunLoopGetCurrent(), // One connection PER Thread
                       _runloopsource,(CFStringRef)kCFRunLoopCommonModes);
    
    dispatch_semaphore_t semaphore_query_send = [[self masterPoolOperation] semaphore];
//    [self dispathCall];
#if defined DEBUG && defined DEBUG2
    NSLog(@"%@ :: %@ :::: Socket created ....", NSStringFromClass([self class]), NSStringFromSelector(_cmd));
#endif
    
   
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
