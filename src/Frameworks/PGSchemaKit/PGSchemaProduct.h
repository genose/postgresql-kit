
#import <Foundation/Foundation.h>

@interface PGSchemaProduct : NSObject {
	id _productnv; // returns PGSchemaProductNV*
	NSMutableArray* _requires; // array of PGSchemaProductNV*
	NSMutableArray* _create; // array of PGSchemaProductOp*
	NSMutableArray* _drop; // array of PGSchemaProductOp*
}

// constructors
-(id)initWithPath:(NSString* )path error:(NSError** )error;
+(PGSchemaProduct* )schemaWithPath:(NSString* )path error:(NSError** )error;

// properties
@property (readonly) NSString* name;
@property (readonly) NSUInteger version;

@end
