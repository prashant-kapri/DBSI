//
#include <iostream>
#include <gtest/gtest.h>
#include "Pipe.h"
#include "Record.h"
#include "File.h"
#include "BigQ.h"
#include "DBFile.h"

#define bufferSize 5000000

Pipe in(bufferSize);
Pipe out(bufferSize);

TEST(BigQTest_1, TestingForCustomerTable)
{
    std::cout << "BigQGtest for customer tbl." << std::endl;
    ComparisonEngine ce;
    Record tempRecord;
    Record records[2];
    Record *next = NULL, *prev = NULL;
    int c = 0;
    bool isSorted = true;
    FILE *db = fopen("/cise/homes/kapri.prashant/test/tpch-dbgen/customer.tbl", "r");
    Schema test("catalog", "customer");
    std::cout << "Inserting record to input pipe ..." << std::endl;
    while (tempRecord.SuckNextRecord(&test, db) == 1)
    {
        in.Insert(&tempRecord);
    }
    fclose(db);
    in.ShutDown();
    std::cout << "Inserting customer data to input pipe done." << std::endl;
    OrderMaker sortorder(&test);
    std::cout << "Now sorting customer table records ..." << std::endl;
    BigQ BigQ(in, out, sortorder, 10);
    std::cout << "Sorting completed now pushing to the output pipe ..." << std::endl;
    while (out.Remove(&records[c % 2]))
    {
        prev = next;
        next = &records[c % 2];
        if (next && prev)
        {
            if (ce.Compare(prev, next, &sortorder) == 1)
            {
                isSorted = false;
                break;
            }
        }
        c++;
    }
    ASSERT_TRUE(isSorted);
    std::cout << "Testing Completed Successfully for customer table" << std::endl;
}

TEST(BigQTest_2, TestingForSupplierTable)
{
    std::cout << "BigQGtest for supplier tbl." << std::endl;
    ComparisonEngine ce;
    Record tempRecord;
    Record records[2];
    Record *next = NULL, *prev = NULL;
    int c = 0;
    bool isSorted = true;
    FILE *db = fopen("/cise/homes/kapri.prashant/test/tpch-dbgen/supplier.tbl", "r");
    Schema test("catalog", "supplier");
    std::cout << "Inserting record to input pipe ..." << std::endl;
    while (tempRecord.SuckNextRecord(&test, db) == 1)
    {
        in.Insert(&tempRecord);
    }
    fclose(db);
    in.ShutDown();
    std::cout << "Inserting supplier data to input pipe done." << std::endl;
    OrderMaker sortorder(&test);
    std::cout << "Now sorting supplier table records ..." << std::endl;
    BigQ BigQ(in, out, sortorder, 10);
    std::cout << "Sorting completed now pushing to the output pipe ..." << std::endl;
    while (out.Remove(&records[c % 2]))
    {
        prev = next;
        next = &records[c % 2];
        if (next && prev)
        {
            if (ce.Compare(prev, next, &sortorder) == 1)
            {
                isSorted = false;
                break;
            }
        }
        c++;
    }
    ASSERT_TRUE(isSorted);
    std::cout << "Testing Completed Successfully for supplier table" << std::endl;
}

TEST(BigQTest_3, TestingForNationTable)
{
    std::cout << "BigQGtest for nation tbl." << std::endl;
    ComparisonEngine ce;
    Record tempRecord;
    Record records[2];
    Record *next = NULL, *prev = NULL;
    int c = 0;
    bool isSorted = true;
    FILE *db = fopen("/cise/homes/kapri.prashant/test/tpch-dbgen/nation.tbl", "r");
    Schema test("catalog", "nation");
    std::cout << "Inserting record to input pipe ..." << std::endl;
    while (tempRecord.SuckNextRecord(&test, db) == 1)
    {
        in.Insert(&tempRecord);
    }
    fclose(db);
    in.ShutDown();
    std::cout << "Inserting nation data to input pipe done." << std::endl;
    OrderMaker sortorder(&test);
    std::cout << "Now sorting nation table records ..." << std::endl;
    BigQ BigQ(in, out, sortorder, 10);
    std::cout << "Sorting completed now pushing to the output pipe ..." << std::endl;
    while (out.Remove(&records[c % 2]))
    {
        prev = next;
        next = &records[c % 2];
        if (next && prev)
        {
            if (ce.Compare(prev, next, &sortorder) == 1)
            {
                isSorted = false;
                break;
            }
        }
        c++;
    }
    ASSERT_TRUE(isSorted);
    std::cout << "Testing Completed Successfully for nation table" << std::endl;
}


TEST(BigQTest_4, TestingForRegionTable)
{
    std::cout << "BigQGtest for region tbl." << std::endl;
    ComparisonEngine ce;
    Record tempRecord;
    Record records[2];
    Record *next = NULL, *prev = NULL;
    int c = 0;
    bool isSorted = true;
    FILE *db = fopen("/cise/homes/kapri.prashant/test/tpch-dbgen/region.tbl", "r");
    Schema test("catalog", "region");
    std::cout << "Inserting record to input pipe ..." << std::endl;
    while (tempRecord.SuckNextRecord(&test, db) == 1)
    {
        in.Insert(&tempRecord);
    }
    fclose(db);
    in.ShutDown();
    std::cout << "Inserting region data to input pipe done." << std::endl;
    OrderMaker sortorder(&test);
    std::cout << "Now sorting region table records ..." << std::endl;
    BigQ BigQ(in, out, sortorder, 10);
    std::cout << "Sorting completed now pushing to the output pipe ..." << std::endl;
    while (out.Remove(&records[c % 2]))
    {
        prev = next;
        next = &records[c % 2];
        if (next && prev)
        {
            if (ce.Compare(prev, next, &sortorder) == 1)
            {
                isSorted = false;
                break;
            }
        }
        c++;
    }
    ASSERT_TRUE(isSorted);
    std::cout << "Testing Completed Successfully for region table" << std::endl;
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
