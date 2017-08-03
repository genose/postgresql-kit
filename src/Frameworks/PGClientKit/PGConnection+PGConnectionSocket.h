//
//  PGConnection+PGConnectionSocket.h
//  postgresql-kit
//
//  Created by Cotillard Sebastien on 02/08/2017.
//
//

#import <PGClientKit/PGClientKit.h>
#import <PGClientKit/PGClientKit+Private.h>

@interface PGConnection (PGConnectionSocket)

-(void)__CFSocket_instanciate;
@end
