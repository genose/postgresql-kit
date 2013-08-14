
#import <Foundation/Foundation.h>

@interface NSURL (PGAdditions)

/**
 *  NSURL helper function to generate PostgreSQL connection URL's. The URL
 *  which is generated will allow connection to a PostgeSQL instance on the
 *  default local socket.
 *
 *  @param database Name of the database to connect to, or nil
 *  @param username Username to authenticate against. When nil, uses current username
 *  @param params   Additional parameters for the connection
 *
 *  @return An NSURL object
 */
+(id)URLWithLocalDatabase:(NSString* )database username:(NSString* )username params:(NSDictionary* )params;

/**
 *  NSURL helper function to generate PostgreSQL connection URL's. The URL
 *  which is generated will allow connection to a PostgeSQL instance on a
 *  remote host on the default port, addressed by name, IP4 or IP6 address.
 *
 *  @param host     Hostname, IP4 or IP6 address. If nil, localhost is assumed.
 *  @param ssl      If YES, SSL communication will be required, or else it is preferred.
 *  @param database Name of the database to connect to, or nil
 *  @param username Username to authenticate against. When nil, uses current username
 *  @param params   Additional parameters for the connection
 *
 *  @return An NSURL object
 */
+(id)URLWithHost:(NSString* )host  ssl:(BOOL)ssl database:(NSString* )database username:(NSString* )username params:(NSDictionary* )params;

/**
 *  NSURL helper function to generate PostgreSQL connection URL's. The URL
 *  which is generated will allow connection to a PostgeSQL instance on a
 *  remote host on a non-standard port, addressed by name, IP4 or IP6 address.
 *
 *  @param host     Hostname, IP4 or IP6 address. If nil, localhost is assumed.
 *  @param port     The port number to connect to. Uses the default port if this is 0
 *  @param ssl      If YES, SSL communication will be required, or else it is preferred.
 *  @param database Name of the database to connect to, or nil
 *  @param username Username to authenticate against. When nil, uses current username
 *  @param params   Additional parameters for the connection
 *
 *  @return An NSURL object
 */
+(id)URLWithHost:(NSString* )host port:(NSUInteger)port ssl:(BOOL)ssl database:(NSString* )database username:(NSString* )username params:(NSDictionary* )params;


/**
 *  NSURL helper function to generate PostgreSQL connection URL's. The URL
 *  which is generated will allow connection to a PostgeSQL instance on the
 *  default local socket.
 *
 *  @param database Name of the database to connect to, or nil
 *  @param username Username to authenticate against. When nil, uses current username
 *  @param params   Additional parameters for the connection
 *
 *  @return An NSURL object
 */
-(id)initWithLocalDatabase:(NSString* )database username:(NSString* )username params:(NSDictionary* )params;

/**
 *  NSURL helper function to generate PostgreSQL connection URL's. The URL
 *  which is generated will allow connection to a PostgeSQL instance on a
 *  remote host on the default port, addressed by name, IP4 or IP6 address.
 *
 *  @param host     Hostname, IP4 or IP6 address. If nil, localhost is assumed.
 *  @param ssl      If YES, SSL communication will be required, or else it is preferred.
 *  @param database Name of the database to connect to, or nil
 *  @param username Username to authenticate against. When nil, uses current username
 *  @param params   Additional parameters for the connection
 *
 *  @return An NSURL object
 */
-(id)initWithHost:(NSString* )host ssl:(BOOL)ssl database:(NSString* )database username:(NSString* )username params:(NSDictionary* )params;

/**
 *  NSURL helper function to generate PostgreSQL connection URL's. The URL
 *  which is generated will allow connection to a PostgeSQL instance on a
 *  remote host on a non-standard port, addressed by name, IP4 or IP6 address.
 *
 *  @param host     Hostname, IP4 or IP6 address. If nil, localhost is assumed.
 *  @param port     The port number to connect to. Uses the default port if this is 0
 *  @param ssl      If YES, SSL communication will be required, or else it is preferred.
 *  @param database Name of the database to connect to, or nil
 *  @param username Username to authenticate against. When nil, uses current username
 *  @param params   Additional parameters for the connection
 *
 *  @return An NSURL object
 */
-(id)initWithHost:(NSString* )host port:(NSUInteger)port ssl:(BOOL)ssl database:(NSString* )database username:(NSString* )username ssl:(BOOL)ssl params:(NSDictionary* )params;

@end
