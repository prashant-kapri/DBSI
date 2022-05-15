#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" int yyparse(void);
extern struct AndList *final;

char *statisticsFile = "Statistics.txt";

TEST(StatisticalEstimationForQ11, Test1)
{

	bool estimationFlag = true;

	Statistics s;
        char *relName[] = { "part",  "lineitem"};

	s.Read(statisticsFile);
	
	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_container",40);

	s.AddRel(relName[1],6001215);
	s.AddAtt(relName[1], "l_partkey",200000);
	s.AddAtt(relName[1], "l_shipinstruct",4);
	s.AddAtt(relName[1], "l_shipmode",7);

	char *cnf = "(l_partkey = p_partkey) AND (l_shipmode = 'AIR' OR l_shipmode = 'AIR REG') AND (p_container ='SM BOX' OR p_container = 'SM PACK')  AND (l_shipinstruct = 'DELIVER IN PERSON')";

	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName,2);

	if(fabs(result-21432.9)>0.5){
		estimationFlag = false;
		cout<<"Read or write or last apply is not correct\n";
	}
	s.Apply(final, relName,2);
	
	s.Write(statisticsFile);
	

	ASSERT_TRUE(estimationFlag);
}

TEST(StatisticalEstimationForQ8, Test2)
{
    bool estimationFlag = true;
    Statistics s;
        char *relName[] = { "part",  "partsupp"};

	s.Read(statisticsFile);
	
	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey",200000);
	

	char *cnf = "(p_partkey=ps_partkey) AND (p_size =3 OR p_size=6 OR p_size =19)";

	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName,2);

	if(fabs(result-48000)>0.1){
		cout<<"error in estimating Q8\n";
        estimationFlag = false;
    }

    s.Apply(final, relName,2);
	
	s.Write(statisticsFile);

	ASSERT_TRUE(estimationFlag);

}

TEST(StatisticalEstimationForQ3, Test3)
{
	bool estimationFlag = true;

	Statistics s;
	char *relName[] = {"supplier","customer","nation"};

	s.Read(statisticsFile);
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_nationkey",25);

	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	s.CopyRel("nation","n1");
	s.CopyRel("nation","n2");
	s.CopyRel("supplier","s");
	s.CopyRel("customer","c");

	char *set1[] ={"s","n1"};
	char *cnf = "(s.s_nationkey = n1.n_nationkey)";
	yy_scan_string(cnf);
	yyparse();	
	s.Apply(final, set1, 2);
	
	char *set2[] ={"c","n2"};
	cnf = "(c.c_nationkey = n2.n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, set2, 2);

	char *set3[] = {"c","s","n1","n2"};
	cnf = " (n1.n_nationkey = n2.n_nationkey )";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, set3, 4);
	if(fabs(result-60000000.0)>0.1)
		estimationFlag = false;

	s.Apply(final, set3, 4);

	s.Write(statisticsFile);

	ASSERT_TRUE(estimationFlag);
}

TEST(StatisticalEstimationForQ7,Test4)
{
	bool estimationFlag = true;

	Statistics s;
        char *relName[] = { "orders", "lineitem"};

	// s.Read(fileName);
	

	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_orderkey",1500000);
	
	
	s.AddRel(relName[1],6001215);
	s.AddAtt(relName[1], "l_orderkey",1500000);
	s.AddAtt(relName[1], "l_receiptdate", 198455);
	

	char *cnf = "(l_receiptdate >'1995-02-01' ) AND (l_orderkey = o_orderkey)";

	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 2);

	if(fabs(result-2000405)>0.1){
		estimationFlag = false;
		cout<<"error in estimating Q7\n";
	}

	s.Apply(final, relName, 2);
	s.Write(statisticsFile);

	ASSERT_TRUE(estimationFlag);
}

int main(int argc, char **argv)
{
	std::cout << "Gtest" << std::endl;
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

