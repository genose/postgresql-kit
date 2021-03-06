
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

@implementation PGResult

@dynamic size, numberOfColumns, affectedRows, dataReturned, columnNames, rowNumber, format;

    ////////////////////////////////////////////////////////////////////////////////
    // initialization

-(id)init {
    return nil;
}

-(id)initWithResult:(PGresult* )theResult format:(PGClientTupleFormat)format encoding:(NSStringEncoding)encoding {
    self = [super init];
    if(self) {
        NSParameterAssert(theResult);
        _result = theResult;
        _format = format;
        _encoding = encoding;
        _cachedData = [NSMutableDictionary dictionaryWithCapacity:PQntuples(_result)];
    }
    return self;
}

-(id)initWithResult:(PGresult* )theResult format:(PGClientTupleFormat)format {
    return [self initWithResult:theResult format:format encoding:NSUTF8StringEncoding];
}

-(void)dealloc {
    @synchronized(self) {


        [NSThread sleepForTimeInterval:0.1];
        [_cachedData removeAllObjects];
//        _result = ((PGresult* )_result);
        if( _result != NULL ){
            PQclear((PGresult* )_result);
        }
        _result = NULL;
        [NSThread sleepForTimeInterval:0.1];
    }
}

    ////////////////////////////////////////////////////////////////////////////////

-(PGClientTupleFormat)format {
    return _format;
}

-(NSUInteger)size {
    NSParameterAssert(_result);
    NSUInteger _number = NSIntegerMax;
    if(_number==NSIntegerMax) {
        _number = PQntuples(_result);
    }
    return _number;
}

-(NSUInteger)numberOfColumns {
    NSParameterAssert(_result);
    NSUInteger _number = NSIntegerMax;
    if(_number==NSIntegerMax) {
        _number = PQnfields(_result);
    }
    return _number;
}

-(NSUInteger)affectedRows {
    NSParameterAssert(_result);
    NSUInteger _number = NSIntegerMax;
    if(_number==NSIntegerMax) {
        NSString* affectedRows = [NSString stringWithUTF8String:PQcmdTuples(_result)];
        _number = [affectedRows integerValue];
    }
    return _number;
}

-(BOOL)dataReturned {
    return PQresultStatus(_result)==PGRES_TUPLES_OK ? YES : NO;
}

-(NSArray* )columnNames {
    NSParameterAssert(_result);
    NSUInteger numberOfColumns = [self numberOfColumns];
    NSMutableArray* theColumns = [NSMutableArray arrayWithCapacity:numberOfColumns];
    for(NSUInteger i = 0; i < numberOfColumns; i++) {
        [theColumns addObject:[NSString stringWithUTF8String:PQfname(_result,(int)i)]];
    }
    return theColumns;
}

-(void)setRowNumber:(NSUInteger)rowNumber {
    NSParameterAssert(rowNumber < [self size]);
    _rowNumber = rowNumber;
}

-(NSUInteger)rowNumber {
    return _rowNumber;
}

    ////////////////////////////////////////////////////////////////////////////////
    // private methods

-(NSObject* )_tupleForRow:(NSUInteger)r column:(NSUInteger)c {
        // check for null
    if(PQgetisnull(_result,(int)r,(int)c)) {
        return [NSNull null];
    }
        // get bytes, length
    const void* bytes = PQgetvalue(_result,(int)r,(int)c);
    NSUInteger size = PQgetlength(_result,(int)r,(int)c);
    NSParameterAssert(bytes);
        //	NSParameterAssert(size);

    switch(_format) {
        case PGClientTupleFormatText:
            return pgdata_text2obj(PQftype(_result,(int)c),bytes,size,_encoding);
        case PGClientTupleFormatBinary:
            return pgdata_bin2obj(PQftype(_result,(int)c),bytes,size,_encoding);
        default:
            return nil;
    }
}

    ////////////////////////////////////////////////////////////////////////////////

    // return the current row as an array of NSObject values
-(NSArray* )fetchRowAsArray {
    if(_rowNumber >= [self size]) {
        return nil;
    }
    NSNumber* key = [NSNumber numberWithUnsignedInteger:_rowNumber];
    NSMutableArray* theArray = [_cachedData objectForKey:key];
    if(theArray==nil) {
            // create the array
        NSUInteger numberOfColumns = [self numberOfColumns];
        theArray = [NSMutableArray arrayWithCapacity:numberOfColumns];
            // fill in the columns
        for(NSUInteger i = 0; i < numberOfColumns; i++) {
            id obj = [self _tupleForRow:_rowNumber column:i];
            NSParameterAssert(obj);
            [theArray addObject:obj];
        }
            // cache the array
        [_cachedData setObject:theArray forKey:key];
    }
        // increment to next row, return
    _rowNumber++;
    return theArray;
}

-(NSDictionary* )fetchRowAsDictionary {
    return [NSDictionary dictionaryWithObjects:[self fetchRowAsArray] forKeys:[self columnNames]];
}


-(NSArray* )arrayForColumn:(NSString* )columnName {
    NSParameterAssert(columnName);
    NSInteger c = [[self columnNames] indexOfObject:columnName];
    if(c < 0 || c >= [self numberOfColumns]) {
        return nil;
    }
    NSUInteger size = [self size];
    if(size==0) {
        return @[ ];
    }
    NSMutableArray* array = [NSMutableArray arrayWithCapacity:size];
    for(NSUInteger i  = 0; i < size; i++) {
        [array addObject:[self _tupleForRow:i column:c]];
    }
    return array;
}


#pragma mark ********* KVC Protocol ********


-(void)setValue:(id)value forUndefinedKey:(NSString *)key
{
    NSLog(@" :::: \n :: %@ :: %@ \n :: %@ \n :::::",NSStringFromSelector(_cmd), key, self );
    NSMutableDictionary * dictTmp = [((NSMutableDictionary*)_cachedData) mutableCopy];
    [((NSMutableDictionary*)dictTmp) setValue:value forKey: key];
    [((NSMutableDictionary*)_cachedData) setDictionary: dictTmp];
    
}
- (void)setObject:(id)anObject forKey:(NSString * _Nonnull)key
{
    NSLog(@" :::: \n :: %@ :: %@ \n :: %@ \n :::::",NSStringFromSelector(_cmd), key, self );
    NSMutableDictionary * dictTmp = [((NSMutableDictionary*)_cachedData) mutableCopy];
    [((NSMutableDictionary*)dictTmp) setValue:anObject forKey: key];
    [((NSMutableDictionary*)_cachedData) setDictionary: dictTmp];
}

-(BOOL)keyExists:(NSString*)keyPath
{
    return ((BOOL)[((NSMutableDictionary*)_cachedData) valueForKeyPath: keyPath]);
}

-(id)valueForKeyPath:(NSString *)keyPath
{
    return [_cachedData valueForKeyPath:keyPath];
}

-(id)valueForKey:(NSString *)key
{
      NSLog(@" :::: \n :: %@ :: %@ \n :: %@ \n :::::",NSStringFromSelector(_cmd), key, self );
    id value = nil;
    if ( ! [key containsString: @"resultDetail."]) {
        value =  [_cachedData objectForKey:key];
        
        if( nil == value )
        {
            
            value = [_cachedData valueForKeyPath:[NSString stringWithFormat:@"resultDetail.%@",key]];
            
            
        }
    }
    
    return value;
}

-(id)valueForUndefinedKey:(NSString *)key
{
    NSLog(@" :::: \n :: %@ :: %@ \n :: %@ \n :::::",NSStringFromSelector(_cmd), key, self );
    id value = [((NSMutableDictionary*)_cachedData) valueForKey:key];
    return value;
}

-(id)objectForKey:(id)key
{
    // ::
    NSLog(@" :::: \n :: %@ :: %@ \n :: %@ \n :::::",NSStringFromSelector(_cmd), key, self );
    id value = nil;
    
        value =  [((NSMutableDictionary*)_cachedData) objectForKey:key];
        
        if( nil == value )
        {
//            if(![key respondsToSelector:@selector(UTF8String)])
//            {
//                key = [NSString stringWithFormat:@"%ld",key];
//            }
            
            value = [((NSMutableDictionary*)_cachedData) valueForKeyPath:[NSString stringWithFormat:@"%@",key]];
        }
    
    
    return value;
}

-(NSUInteger)count
{
    
//    bool cnt = [ _cachedData isKindOfClass:[NSDictionary class]] && [self dataReturned];
//    if(!cnt)
//    {
//        return 0;
//    }
//    
//    id keysC = [((NSMutableDictionary*)_cachedData) allKeys];
//    NSUInteger allkeysC = [keysC count];
    NSInteger cnt = ( _result ) ? (long) PQntuples(_result): 0;
    return cnt;
}

-(NSArray *)allKeys
{
    
    id keysC = [((NSMutableDictionary*)_cachedData) allKeys];
    return keysC ;
}

-(NSEnumerator *)keyEnumerator
{
    return  (([_cachedData respondsToSelector: @selector(keyEnumerator) ]) ? [_cachedData keyEnumerator] : nil);
}
-(instancetype)initWithObjects:(NSArray *)objects forKeys:(NSArray *)keys
{
    [NSException raise:NSInvocationOperationCancelledException format:@"\n ************ \n ********** \n ERROR :: Unimplemented :: %@  :: \n >>> args / keys :: %@ \n >>>> keys :: %@ \n ************ \n ********** \n ",NSStringFromSelector(_cmd), keys, objects];
    return self;
}

-(id)copyWithZone:(NSZone *)zone
{
    
    PGResult* newClass = [[[self class] allocWithZone:zone] initWithResult: _result  format: [self format] encoding: _encoding ];
    
    return newClass;
    
}
    ////////////////////////////////////////////////////////////////////////////////

-(NSString* )description {
    NSDictionary* theValues =
    [NSDictionary dictionaryWithObjectsAndKeys:
     [NSNumber numberWithUnsignedInteger:[self size]],@"size",
     [NSNumber numberWithUnsignedInteger:[self affectedRows]],@"affectedRows",
     [NSNumber numberWithBool:[self dataReturned]],@"dataReturned",
     [self columnNames],@"columnNames",nil];
    return [theValues description];
}

@end
