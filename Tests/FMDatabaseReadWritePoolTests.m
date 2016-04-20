//
//  FMDatabaseReadWritePoolTests.m
//  fmdb
//
//  Created by Kevin Wooten on 6/14/2014.
//  Adapted from version by Graham Dennis.
//
//

#import <XCTest/XCTest.h>
#import "FMDatabaseReadWritePool.h"

@interface FMDatabaseReadWritePoolTests : FMDBTempDBTests

@property FMDatabaseReadWritePool *pool;

@end

@implementation FMDatabaseReadWritePoolTests

+ (void)populateDatabase:(FMDatabase *)db
{
    [db executeUpdate:@"create table easy (a text)"];
    [db executeUpdate:@"create table easy2 (a text)"];

    [db executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1001]];
    [db executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1002]];
    [db executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1003]];

    [db executeUpdate:@"create table likefoo (foo text)"];
    [db executeUpdate:@"insert into likefoo values ('hi')"];
    [db executeUpdate:@"insert into likefoo values ('hello')"];
    [db executeUpdate:@"insert into likefoo values ('not')"];
}

- (void)setUp
{
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
    
    [NSFileManager.defaultManager removeItemAtPath:self.databasePath error:nil];
    
    self.pool = [FMDatabaseReadWritePool databasePoolWithPath:self.databasePath];
    XCTAssertNotNil(self.pool, @"Unable to open database");

    [self.pool inTransaction:^(FMDatabase *db, BOOL *rollback) {
        [FMDatabaseReadWritePoolTests populateDatabase:db];
    }];
    
    [[self pool] setDelegate:self];
    
}

- (void)tearDown
{
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
    
    [[self pool] close];
}

- (void)testPoolIsInitiallyEmpty
{
    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)1, @"Pool should have a single (write) database upon creation");
}

- (void)testDatabaseCreation
{
    __block FMDatabase *db1;
    
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        
        XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)2, @"Should only have two databases at this point");
        
        db1 = db;
        
    }];
    
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        XCTAssertEqualObjects(db, db1, @"We should get the same database back because there was no need to create a new one");
        
        [self.pool inReadableDatabase:^(FMDatabase *db2) {
            XCTAssertNotEqualObjects(db2, db, @"We should get a different database because the first was in use.");
        }];
        
    }];
    
    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)3);
    
    [self.pool releaseAllDatabases];

    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)1, @"We should be back to one database again");
}

- (void)testCheckedInCheckoutOutCount
{
    [self.pool inWritableDatabase:^(FMDatabase *aDb) {
        
        XCTAssertEqual([self.pool countOfCheckedInReadableDatabases],   (NSUInteger)0);
        XCTAssertEqual([self.pool countOfCheckedOutReadableDatabases],  (NSUInteger)0);
        
        XCTAssertTrue(([aDb executeUpdate:@"insert into easy (a) values (?)", @"hi"]));
        
        // just for fun.
        FMResultSet *rs = [aDb executeQuery:@"select * from easy"];
        XCTAssertNotNil(rs);
        XCTAssertTrue([rs next]);
        while ([rs next]) { ; } // whatevers.
        
        XCTAssertEqual([self.pool countOfOpenDatabases],                (NSUInteger)1);
        XCTAssertEqual([self.pool countOfCheckedInReadableDatabases],   (NSUInteger)0);
        XCTAssertEqual([self.pool countOfCheckedOutReadableDatabases],  (NSUInteger)0);
    }];
    
    XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)1);
}

- (void)testMaximumDatabaseLimit
{
    [self.pool setMaximumNumberOfDatabasesToCreate:2];
    
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        [self.pool inReadableDatabase:^(FMDatabase *db2) {
            [self.pool inReadableDatabase:^(FMDatabase *db3) {
                XCTAssertEqual([self.pool countOfOpenDatabases], (NSUInteger)2);
                XCTAssertNil(db3, @"The third database must be nil because we have a maximum of 2 databases in the pool");
            }];
            
        }];
    }];
}

- (void)testTransaction
{
    [self.pool inTransaction:^(FMDatabase *adb, BOOL *rollback) {
        [adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1001]];
        [adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1002]];
        [adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1003]];
        
        XCTAssertEqual([self.pool countOfOpenDatabases],        (NSUInteger)1);
        XCTAssertEqual([self.pool countOfCheckedInReadableDatabases],   (NSUInteger)0);
        XCTAssertEqual([self.pool countOfCheckedOutReadableDatabases],  (NSUInteger)0);
    }];

    XCTAssertEqual([self.pool countOfOpenDatabases],        (NSUInteger)1);
    XCTAssertEqual([self.pool countOfCheckedInReadableDatabases],   (NSUInteger)0);
    XCTAssertEqual([self.pool countOfCheckedOutReadableDatabases],  (NSUInteger)0);
}

- (void)testSelect
{
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1001]];
        XCTAssertNotNil(rs);
        XCTAssertTrue ([rs next]);
        XCTAssertFalse([rs next]);
    }];
}

- (void)testTransactionRollback
{
    [self.pool inDeferredTransaction:^(FMDatabase *adb, BOOL *rollback) {
        XCTAssertTrue(([adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1004]]));
        XCTAssertTrue(([adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1005]]));
        XCTAssertTrue([[adb executeQuery:@"select * from easy where a == '1004'"] next], @"1004 should be in database");
        
        *rollback = YES;
    }];
    
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        XCTAssertFalse([[db executeQuery:@"select * from easy where a == '1004'"] next], @"1004 should not be in database");
    }];

    XCTAssertEqual([self.pool countOfOpenDatabases],                (NSUInteger)2);
    XCTAssertEqual([self.pool countOfCheckedInReadableDatabases],   (NSUInteger)1);
    XCTAssertEqual([self.pool countOfCheckedOutReadableDatabases],  (NSUInteger)0);
}

- (void)testSavepoint
{
    NSError *err = [self.pool inSavePoint:^(FMDatabase *db, BOOL *rollback) {
        [db executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1006]];
    }];
    
    XCTAssertNil(err);
}

- (void)testNestedSavepointRollback
{
    NSError *err = [self.pool inSavePoint:^(FMDatabase *adb, BOOL *rollback) {
        XCTAssertFalse([adb hadError]);
        XCTAssertTrue(([adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1009]]));
        
        [adb inSavePoint:^(BOOL *arollback) {
            XCTAssertTrue(([adb executeUpdate:@"insert into easy values (?)", [NSNumber numberWithInt:1010]]));
            *arollback = YES;
        }];
    }];
    
    
    XCTAssertNil(err);
    
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1009]];
        XCTAssertTrue ([rs next]);
        XCTAssertFalse([rs next]); // close it out.
        
        rs = [db executeQuery:@"select * from easy where a = ?", [NSNumber numberWithInt:1010]];
        XCTAssertFalse([rs next]);
    }];
}

- (void)testLikeStringQuery
{
    [self.pool inReadableDatabase:^(FMDatabase *db) {
        int count = 0;
        FMResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
        while ([rsl next]) {
            count++;
        }
        
        XCTAssertEqual(count, 2);
        
        count = 0;
        rsl = [db executeQuery:@"select * from likefoo where foo like ?", @"h%"];
        while ([rsl next]) {
            count++;
        }
        
        XCTAssertEqual(count, 2);
        
    }];
}

- (void)testStressTest
{
    size_t ops = 128;
    
    dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    
    dispatch_apply(ops, dqueue, ^(size_t nby) {
        
        // just mix things up a bit for demonstration purposes.
        if (nby % 2 == 1) {
            
            [NSThread sleepForTimeInterval:.001];
        }
        
        [self.pool inReadableDatabase:^(FMDatabase *db) {
            FMResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
            XCTAssertNotNil(rsl);
            int i = 0;
            while ([rsl next]) {
                i++;
                if (nby % 3 == 1) {
                    [NSThread sleepForTimeInterval:.0005];
                }
            }
            XCTAssertEqual(i, 2);
        }];
    });
    
    XCTAssert([self.pool countOfOpenDatabases] < 64, @"There should be significantly less than 64 databases after that stress test");
}


- (BOOL)databasePool:(FMDatabaseReadWritePool*)pool shouldAddDatabaseToPool:(FMDatabase*)database {
    [database setMaxBusyRetryTimeInterval:10];
    // [database setCrashOnErrors:YES];
    return YES;
}

- (void)testReadWriteStressTest
{
    int ops = 16;
    
    dispatch_queue_t dqueue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    
    dispatch_apply(ops, dqueue, ^(size_t nby) {
        
        // just mix things up a bit for demonstration purposes.
        if (nby % 2 == 1) {
            [NSThread sleepForTimeInterval:.01];
            
            [self.pool inTransaction:^(FMDatabase *db, BOOL *rollback) {
                FMResultSet *rsl = [db executeQuery:@"select * from likefoo where foo like 'h%'"];
                XCTAssertNotNil(rsl);
                while ([rsl next]) {
                    ;// whatever.
                }
                
            }];
            
        }
        
        if (nby % 3 == 1) {
            [NSThread sleepForTimeInterval:.01];
        }
        
        [self.pool inTransaction:^(FMDatabase *db, BOOL *rollback) {
            XCTAssertTrue([db executeUpdate:@"insert into likefoo values ('1')"]);
            XCTAssertTrue([db executeUpdate:@"insert into likefoo values ('2')"]);
            XCTAssertTrue([db executeUpdate:@"insert into likefoo values ('3')"]);
        }];
    });
    
    [self.pool releaseAllDatabases];
    
    [self.pool inWritableDatabase:^(FMDatabase *db) {
        XCTAssertTrue([db executeUpdate:@"insert into likefoo values ('1')"]);
    }];
}


-(void) testConcurrentAccess
{
    [self.pool inWritableDatabase:^(FMDatabase *db) {
        
        XCTAssertTrue([db executeStatements:@"DROP TABLE IF EXISTS test; CREATE TABLE test(value);"]);
        
    }];
    
    dispatch_queue_t q = dispatch_queue_create("Readers/Writers", DISPATCH_QUEUE_CONCURRENT);
    __block BOOL finished = NO;
    
    for (int c=0; c < 5; ++c) {
        
        dispatch_async(q, ^{
            
            while (!finished) {
                
                [self.pool inReadableDatabase:^(FMDatabase *db) {
                    
                    FMResultSet *resultSet = [db executeQuery:@"SELECT value FROM test"];
                    XCTAssertNotNil(resultSet, @"DB query returned nil result set");
                    XCTAssertTrue(resultSet.next, @"Statement next failed");
                    [resultSet close];
                    XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
                    
                }];
                
            }
            
        });
        
    }
    
    for (int c=0; c < 5000; ++c) {
        
        [self.pool inWritableDatabase:^(FMDatabase *db) {
            
            BOOL res = [db executeUpdate:@"INSERT INTO test(value) VALUES (?)", @(c)];
            XCTAssertTrue(res, @"DB update failed");
            XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
            
        }];
        
        usleep(100);
        
    }
    
    finished = YES;
    
    dispatch_barrier_sync(q, ^{
    });
    
}

-(void) testConcurrentAccessNoCache
{
    self.pool.cacheConnections = NO;
    
    [self.pool inWritableDatabase:^(FMDatabase *db) {
        
        XCTAssertTrue([db executeStatements:@"DROP TABLE IF EXISTS test; CREATE TABLE test(value);"]);
        
    }];
    
    dispatch_queue_t q = dispatch_queue_create("Readers/Writers", DISPATCH_QUEUE_CONCURRENT);
    __block BOOL finished = NO;
    
    for (int c=0; c < 5; ++c) {
        
        dispatch_async(q, ^{
            
            while (!finished) {
                
                [self.pool inReadableDatabase:^(FMDatabase *db) {
                    
                    FMResultSet *resultSet = [db executeQuery:@"SELECT value FROM test"];
                    XCTAssertNotNil(resultSet, @"DB query returned nil result set");
                    XCTAssertTrue(resultSet.next, @"Statement next failed");
                    [resultSet close];
                    XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
                    
                }];
                
            }
            
        });
        
    }
    
    for (int c=0; c < 100; ++c) {
        
        [self.pool inWritableDatabase:^(FMDatabase *db) {
            
            BOOL res = [db executeUpdate:@"INSERT INTO test(value) VALUES (?)", @(c)];
            XCTAssertTrue(res, @"DB update failed");
            XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
            
        }];
        
        usleep(100);
        
    }
    
    finished = YES;
    
    dispatch_barrier_sync(q, ^{
    });

}

-(void) testConcurrentAccess2
{
    
    [self.pool inWritableDatabase:^(FMDatabase *db) {
        
        XCTAssertTrue([db executeStatements:@"DROP TABLE IF EXISTS test; CREATE TABLE test(id INTEGER PRIMARY KEY, name TEXT);"]);
        XCTAssertTrue([db executeUpdate:@"INSERT INTO test(id,name) VALUES(0,'test0')"]);
        XCTAssertTrue([db executeUpdate:@"INSERT INTO test(id,name) VALUES(1,'test1')"]);
        XCTAssertTrue([db executeUpdate:@"INSERT INTO test(id,name) VALUES(2,'test2')"]);
        XCTAssertTrue([db executeUpdate:@"INSERT INTO test(id,name) VALUES(3,'test3')"]);
        XCTAssertTrue([db executeUpdate:@"INSERT INTO test(id,name) VALUES(4,'test4')"]);
        
    }];
    
    dispatch_queue_t q = dispatch_queue_create("Readers/Writers", DISPATCH_QUEUE_CONCURRENT);
    __block BOOL finished = NO;
    
    for (int c=0; c < 5; ++c) {
        
        dispatch_async(q, ^{
            
            [self.pool inReadableDatabase:^(FMDatabase *db) {
                
                FMResultSet *resultSet = [db executeQuery:@"SELECT name FROM test WHERE id = ?", @(c)];
                XCTAssertNotNil(resultSet, @"DB query returned nil result set");
                XCTAssertTrue(resultSet.next, @"Statement next failed");
                
                __block NSString *cur = [resultSet stringForColumnIndex:0];
                [resultSet close];
                
                XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
                
                while (!finished) {
                    
                    FMResultSet *resultSet = [db executeQuery:@"SELECT name FROM test WHERE id = ?", @(c)];
                    XCTAssertNotNil(resultSet, @"DB query returned nil result set");
                    XCTAssertTrue(resultSet.next, @"Statement next failed");
                    
                    NSString *now = [resultSet stringForColumnIndex:0];
                    XCTAssertNotNil(now);
                    
                    if (![now isEqualToString:cur]) {
                        cur = now;
                    }
                    
                    [resultSet close];          
                    
                    XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
                }
                
            }];
            
        });
        
    }
    
    __block BOOL lastResult = true;
    for (int c=10; c < 5000 && lastResult; ++c) {
        
        [self.pool inWritableDatabase:^(FMDatabase *db) {
            
            NSString *name = [NSString stringWithFormat:@"test%d", c];
            NSInteger idx = rand() % 5;
            
            lastResult = [db executeUpdate:@"UPDATE test SET name=? WHERE id=?", name, @(idx)];
            XCTAssertTrue(lastResult, @"DB update failed");
            XCTAssertEqual(db.lastErrorCode, 0, @"Unexpected error");
            
        }];
        
        usleep(500);
        
    }
    
    finished = YES;
    
    dispatch_barrier_sync(q, ^{
    });
    
}

@end
