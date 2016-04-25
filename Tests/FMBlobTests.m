//
//  FMBlobTests.m
//  fmdb
//
//  Created by Kevin Wooten on 4/21/16.
//
//

#import "FMDBTempDBTests.h"
#import "FMBlob.h"
#import "FMDatabaseAdditions.h"


@interface FMBlobTests : FMDBTempDBTests

@end

@implementation FMBlobTests

static NSMutableData *randomData;

+ (void)populateDatabase:(FMDatabase *)db
{
    [db executeUpdate:@"create table blobs (data blob)"];
    
    [db beginTransaction];
    
    randomData = [NSMutableData dataWithLength:20];
    arc4random_buf(randomData.mutableBytes, randomData.length);
    
    [db executeUpdate:@"insert into blobs(data) values (?)", randomData];
    
    [db commit];
}

- (void)setUp {
    [super setUp];
}

- (void)tearDown {
    [super tearDown];
}

- (void)testGoodRead {
    
    long rowId = [self.db longForQuery:@"select rowid from blobs"];
    
    NSError *error = nil;
    FMBlob *blob = [[FMBlob alloc] initWithDatabase:self.db dbName:@"main" tableName:@"blobs" columnName:@"data" rowId:rowId mode:FMBlobOpenModeRead error:&error];
    XCTAssertNotNil(blob, @"Error (%ld): %@", error.code, error.localizedDescription);
    
    NSMutableData *readData = [NSMutableData dataWithLength:randomData.length];
    XCTAssertTrue([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    XCTAssertEqualObjects(randomData, readData);
}

- (void)testBadReads {
    
    long rowId = [self.db longForQuery:@"select rowid from blobs"];
    
    NSError *error = nil;
    FMBlob *blob = [[FMBlob alloc] initWithDatabase:self.db dbName:@"main" tableName:@"blobs" columnName:@"data" rowId:rowId mode:FMBlobOpenModeRead error:&error];
    XCTAssertNotNil(blob, @"Error (%ld): %@", error.code, error.localizedDescription);
    
    NSMutableData *readData = [NSMutableData dataWithLength:randomData.length];
    
    XCTAssertFalse([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:1 error:&error], @"Read should have failed becaue of bad offset");
    XCTAssertEqualObjects(readData, [NSMutableData dataWithLength:randomData.length], @"No data should have been read");

    XCTAssertFalse([blob readIntoBuffer:readData.mutableBytes length:readData.length+1 atOffset:0 error:&error], @"Read should have failed becaue of bad length");
    XCTAssertEqualObjects(readData, [NSMutableData dataWithLength:randomData.length], @"No data should have been read");
}

- (void)testGoodWrite {
    
    long rowId = [self.db longForQuery:@"select rowid from blobs"];
    
    NSError *error = nil;
    FMBlob *blob = [[FMBlob alloc] initWithDatabase:self.db dbName:@"main" tableName:@"blobs" columnName:@"data" rowId:rowId mode:FMBlobOpenModeReadWrite error:&error];
    XCTAssertNotNil(blob, @"Error (%ld): %@", error.code, error.localizedDescription);
    
    NSMutableData *readData = [NSMutableData dataWithLength:randomData.length];
    XCTAssertTrue([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    XCTAssertEqualObjects(randomData, readData);
    
    // Generate and write new random data
    NSMutableData *newData = [NSMutableData dataWithLength:randomData.length];
    arc4random_buf(newData.mutableBytes, newData.length);
    
    XCTAssertNotEqualObjects(newData, randomData);
    
    XCTAssertTrue([blob writeFromBuffer:newData.bytes length:newData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    // Readback new data
    readData = [NSMutableData dataWithLength:newData.length];
    XCTAssertTrue([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    XCTAssertEqualObjects(newData, readData);
}

- (void)testBadWrites {
    
    long rowId = [self.db longForQuery:@"select rowid from blobs"];
    
    NSError *error = nil;
    FMBlob *blob = [[FMBlob alloc] initWithDatabase:self.db dbName:@"main" tableName:@"blobs" columnName:@"data" rowId:rowId mode:FMBlobOpenModeRead error:&error];
    XCTAssertNotNil(blob, @"Error (%ld): %@", error.code, error.localizedDescription);
    
    // Generate and write new random data
    NSMutableData *newData = [NSMutableData dataWithLength:randomData.length];
    arc4random_buf(newData.mutableBytes, newData.length);
    
    XCTAssertNotEqualObjects(newData, randomData);
    
    XCTAssertFalse([blob writeFromBuffer:newData.bytes length:newData.length atOffset:1 error:&error], @"Write should have failed because of bad offset");
    
    // Readback new data & check
    NSMutableData *readData = [NSMutableData dataWithLength:newData.length];
    XCTAssertTrue([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    XCTAssertEqualObjects(randomData, readData, @"Data should have been unchanged");

    XCTAssertFalse([blob writeFromBuffer:newData.bytes length:newData.length + 1 atOffset:0 error:&error], @"Write should have failed because of bad length");
    
    // Readback new data & check
    readData = [NSMutableData dataWithLength:newData.length];
    XCTAssertTrue([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    XCTAssertEqualObjects(randomData, readData, @"Data should have been unchanged");
}

- (void)testFailedWriteWhenReadOnly {
    
    long rowId = [self.db longForQuery:@"select rowid from blobs"];
    
    NSError *error = nil;
    FMBlob *blob = [[FMBlob alloc] initWithDatabase:self.db dbName:@"main" tableName:@"blobs" columnName:@"data" rowId:rowId mode:FMBlobOpenModeRead error:&error];
    XCTAssertNotNil(blob, @"Error (%ld): %@", error.code, error.localizedDescription);
    
    // Generate and write new random data
    NSMutableData *newData = [NSMutableData dataWithLength:randomData.length];
    arc4random_buf(newData.mutableBytes, newData.length);
    
    XCTAssertNotEqualObjects(newData, randomData);
    
    XCTAssertFalse([blob writeFromBuffer:newData.bytes length:newData.length atOffset:0 error:&error], @"Write should have failed");
    
    // Readback new data
    NSMutableData *readData = [NSMutableData dataWithLength:newData.length];
    XCTAssertTrue([blob readIntoBuffer:readData.mutableBytes length:readData.length atOffset:0 error:&error], @"Error (%ld): %@", error.code, error.localizedDescription);
    
    XCTAssertEqualObjects(randomData, readData, @"Data should have been unchanged");
}

@end
