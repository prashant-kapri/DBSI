#include <iostream>
#include <gtest/gtest.h>
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>
#include "NodeTreeQuery.h"
#include "ParseTree.h"
#include "Statistics.h"

extern "C"
{
	int yyparse(void);
	struct YY_BUFFER_STATE *yy_scan_string(const char *);
}
extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

void predicateDetail(vector<AndList> &joinsVector, vector<AndList> &selectsVector, vector<AndList> &joinDepSelectsVector, Statistics statsObj)
{

	struct OrList *currentOrList;
	// iterating over the predicate information
	while (boolean != 0)
	{
		currentOrList = boolean->left;
		// join operation if both left and right is name with equals sign
		if (currentOrList && currentOrList->left->code == EQUALS && currentOrList->left->left->code == NAME && currentOrList->left->right->code == NAME)
		{
			AndList newAnd = *boolean;
			newAnd.rightAnd = 0;
			joinsVector.push_back(newAnd);
		}
		else
		{
			// operation on table attribute and literal
			currentOrList = boolean->left;
			if (currentOrList->left == NULL)
			{
				// predicate on only one table such as (a.b = 'c')
				AndList newAnd = *boolean;
				newAnd.rightAnd = NULL;
				selectsVector.push_back(newAnd);
			}
			else
			{
				// predicate on multiple tables such as (a.b = 'c' OR x.y = 'z')
				vector<string> tablesInvolvedVector;
				while (currentOrList != NULL)
				{
					Operand *oprnd = currentOrList->left->left;
					if (oprnd->code != NAME)
					{
						oprnd = currentOrList->left->right;
					}
					string relation;
					statsObj.parseRel(oprnd, relation);
					if (tablesInvolvedVector.size() == 0)
					{
						tablesInvolvedVector.push_back(relation);
					}
					else if (relation.compare(tablesInvolvedVector[0]) != 0)
					{
						tablesInvolvedVector.push_back(relation);
					}
					currentOrList = currentOrList->rightOr;
				}
				if (tablesInvolvedVector.size() > 1)
				{
					// multiple tables involved
					AndList newAnd = *boolean;
					newAnd.rightAnd = 0;
					joinDepSelectsVector.push_back(newAnd);
				}
				else
				{
					// single table involved
					AndList newAnd = *boolean;
					newAnd.rightAnd = 0;
					selectsVector.push_back(newAnd);
				}
			}
		}
		boolean = boolean->rightAnd;
	}
}

vector<AndList> joinsOrdOptimization(vector<AndList> joinsVector, Statistics *statsObj)
{
	// Left deep join Optimization is used
	vector<AndList> newOrdVector;
	newOrdVector.reserve(joinsVector.size());
	AndList join;
	double minValue = -1.0;
	double approxValue = 0.0;

	string rel1, rel2;
	int index = 0, minIdx = 0, c = 0;
	vector<string> joinedRel;

	// loop over the all join tables ends untill all are processed
	while (joinsVector.size() > 1)
	{
		while (joinsVector.size() > index)
		{
			join = joinsVector[index];
			// getting the relation name
			statsObj->parseRel(join.left->left->left, rel1);
			statsObj->parseRel(join.left->left->right, rel2);

			if (minValue == -1.0)
			{
				// initial case where the estimate cost is computed for first join
				char *relations[] = {(char *)rel1.c_str(), (char *)rel2.c_str()};
				minValue = statsObj->Estimate(&join, relations, 2);
				minIdx = index;
			}
			else
			{
				// join cost is computed
				char *relations[] = {(char *)rel1.c_str(), (char *)rel2.c_str()};
				approxValue = statsObj->Estimate(&join, relations, 2);
				// comparing the estimated with the already min cost and updating index accordingly
				if (minValue > approxValue)
				{
					minValue = approxValue;
					minIdx = index;
				}
			}
			index++;
		}

		joinedRel.push_back(rel1);
		joinedRel.push_back(rel2);
		// join operation of the smallest index is pushed to vector
		newOrdVector.push_back(joinsVector[minIdx]);

		minValue = -1.0;
		index = 0;
		c++;
		joinsVector.erase(joinsVector.begin() + minIdx);
	}
	// filling last cell with the initial join operation
	newOrdVector.insert(newOrdVector.begin() + c, joinsVector[0]);
	return newOrdVector;
}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

TEST(TestCase1, Test1)
{
	char *cnf = "SELECT SUM (n.n_nationkey) FROM nation AS n, region AS r WHERE(n.n_regionkey = r.r_regionkey) AND(n.n_name = 'UNITED STATES') ";
	yy_scan_string(cnf);
	yyparse();

	Statistics *s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while (tList)
	{
		if (tList->aliasAs)
		{
			relations.push_back(tList->aliasAs);
		}
		else
		{
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 2);

	predicateDetail(joinsVector, selectsVector, joinDepSelsVector, *s);


	ASSERT_TRUE(joinsVector.size() == 1);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);

	if (joinsVector.size() > 0)
	{
		joinsVector = joinsOrdOptimization(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 1);
		cout << "TEST 1 Passed" << endl;
	}
	cout <<"Test 1 completed ***************"<<endl;

}

TEST(TestCase2, Test2)
{
	char *cnf = "SELECT SUM(ps.ps_supplycost), s.s_suppkey FROM part AS p, supplier AS s, partsupp AS ps WHERE(p.p_partkey = ps.ps_partkey) AND(s.s_suppkey = ps.ps_suppkey) AND(s.s_acctbal > 2500.0) GROUP BY s.s_suppkey ";
	yy_scan_string(cnf);
	yyparse();

	Statistics *s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while (tList)
	{
		if (tList->aliasAs)
		{
			relations.push_back(tList->aliasAs);
		}
		else
		{
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 3);

	predicateDetail(joinsVector, selectsVector, joinDepSelsVector, *s);


	
	ASSERT_TRUE(joinsVector.size() == 2);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);

	if (joinsVector.size() > 1)
	{
		joinsVector = joinsOrdOptimization(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 2);
		cout << "TEST 2 Passed" << endl;
	}
	cout <<"Test 2 completed ***************"<<endl;

}

TEST(TestCase3, Test3)
{
	char *cnf = "SELECT n.n_name FROM nation AS n, region AS r WHERE(n.n_regionkey = r.r_regionkey) AND(n.n_nationkey > 5) ";
	yy_scan_string(cnf);
	yyparse();

	Statistics *s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while (tList)
	{
		if (tList->aliasAs)
		{
			relations.push_back(tList->aliasAs);
		}
		else
		{
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}
	cout<<relations.size();


	predicateDetail(joinsVector, selectsVector, joinDepSelsVector, *s);

	
	ASSERT_TRUE(joinsVector.size() == 1);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);

	if (joinsVector.size() == 1)
	{
		joinsVector = joinsOrdOptimization(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 1);
		cout << "TEST 3 Passed" << endl;
	}
	cout <<"Test 3 completed ***************"<<endl;

}

TEST(TestCase4, Test4)
{
	char *cnf = "SELECT SUM DISTINCT (n.n_nationkey + r.r_regionkey) FROM nation AS n, region AS r, customer AS c WHERE(n.n_regionkey = r.r_regionkey) AND(n.n_nationkey = c.c_nationkey) AND(n.n_nationkey > 10) GROUP BY r.r_regionkey ";
	yy_scan_string(cnf);
	yyparse();

	Statistics *s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while (tList)
	{
		if (tList->aliasAs)
		{
			relations.push_back(tList->aliasAs);
		}
		else
		{
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 3);

	predicateDetail(joinsVector, selectsVector, joinDepSelsVector, *s);

	ASSERT_TRUE(joinsVector.size() == 2);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);

	if (joinsVector.size() > 1)
	{
		joinsVector = joinsOrdOptimization(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 2);
		cout << "TEST 4 Passed" << endl;
	}
	cout <<"Test 4 completed ***************"<<endl;
}