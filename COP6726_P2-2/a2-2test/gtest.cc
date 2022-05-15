#include <gtest/gtest.h>
#include "DBFile.h"
#include "Sorted.h"
#define PIPE 500000

DBFile db;
string f_path = "customer.bin";

TEST(DBFileCreation, CreateCustomerTbl)
{
	OrderMaker *order = new OrderMaker();
	struct
	{
		OrderMaker *order;
		int size;
	} SortInfo = {order, 48};

	cout << "Starting DB File creation ..." << endl;
	ASSERT_EQ(1, db.Create((char *)f_path.c_str(), sorted, &SortInfo));
	cout << "Customer table dbfile created." << endl;
}

TEST(DBFileOpen, OpenCustomerTbl)
{
	cout << "Opening db file ..." << endl;
	ASSERT_EQ(1, db.Open((char *)f_path.c_str()));
	cout << "Customer table file opened." << endl;
}

TEST(DBFileClose, CloseCustomerTbl)
{
	cout << "Closing db file ..." << endl;
	ASSERT_EQ(1, db.Close());
	remove("custimer.bin");
	remove("customer.bin.header");
	cout << "Customer table closed." << endl;
}

TEST(SortTest, SortedFunctionTest)
{
	std::cout << "Gtest for sorted function ..." << std::endl;

	FILE *dbFile = fopen("/cise/homes/kapri.prashant/test/tpch-dbgen/", "r");
	Record temp;
	Schema test("catalog", "customer");
	Pipe in(PIPE);
	Pipe out(PIPE);
	ComparisonEngine ce;
	OrderMaker sortorder(&test);

	int c = 0;
	bool isSorted = true;
	Record rec[2];
	Record *next = NULL, *prev = NULL;

	std::cout << "Reading data to ip pipe...." << std::endl;
	while (temp.SuckNextRecord(&test, dbFile) == 1)
	{
		in.Insert(&temp);
	}
	fclose(dbFile);
	in.ShutDown();
	std::cout << "Record fetching completed." << std::endl;

	std::cout << "Now starting to sort the data rec...." << std::endl;
	BigQ bq(in, out, sortorder, 10000);
	while (out.Remove(&rec[c % 2]))
	{
		prev = next;
		next = &rec[c % 2];
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
	std::cout << "Sort function testing completed" << std::endl;

}

int main(int argc, char **argv)
{
	std::cout << "Gtest" << std::endl;
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
