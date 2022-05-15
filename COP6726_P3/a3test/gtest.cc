#include <gtest/gtest.h>
#include <iostream>
#include "test.h"
#include "RelOp.h"

DBFile dbf_ps, dbf_p, dbf_s;
Pipe _ps(pipesz), _p(pipesz), _s(pipesz);
CNF cnf_ps, cnf_p, cnf_s;
Record lit_ps, lit_p, lit_s;

int pAtts = 9;
int pipesz = 100;
int buffsz = 100;

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};
SelectFile SF_ps, SF_p, SF_s;
SelectPipe SP;

void init_SF_ps(char *pred_str, int numpgs)
{
	dbf_ps.Open(ps->path());
	get_cnf(pred_str, ps->schema(), cnf_ps, lit_ps);
	SF_ps.Use_n_Pages(numpgs);
}

void init_SF_s(char *pred_str, int numpgs)
{
	dbf_s.Open(s->path());
	get_cnf(pred_str, s->schema(), cnf_s, lit_s);
	SF_s.Use_n_Pages(numpgs);
}

void init_SF_p(char *pred_str, int numpgs)
{
	dbf_p.Open(p->path());
	get_cnf(pred_str, p->schema(), cnf_p, lit_p);
	SF_p.Use_n_Pages(numpgs);
}

int clear_pipe(Pipe &in_pipe, Schema *schema, bool print)
{
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove(&rec))
	{
		if (print)
		{
			rec.Print(schema);
		}
		cnt++;
	}
	return cnt;
}

TEST(Select_Pipe_Relational_Operator, testing_1)
{
	cout << "Running SelectPipe relational operation on PARTSUPP records" << endl;
	char *pred_ps = "(ps_supplycost < 1.03)";
	init_SF_ps(pred_ps, 100);
	SF_ps.Use_n_Pages(4);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();
	dbf_ps.Close();
	Pipe _out(pipesz);
	SP.Use_n_Pages(4);
	SP.Run(_ps, _out, cnf_ps, lit_ps);
	SP.WaitUntilDone();
	int cnt = clear_pipe(_out, ps->schema(), false);
	cout << "\n\n Test returned " << cnt << " records \n";
	(cnt > 0) ? ASSERT_TRUE(true) : ASSERT_FALSE(false);
}

TEST(Sum_Relational_Operator, testing_2)
{
	cout << "Running sum operation on SUPPLIER records: " << endl;
	char *pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s(pred_s, 100);
	Sum T;
	Pipe _out(1);
	Function func;
	char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
	get_cnf(str_sum, s->schema(), func);
	func.Print();
	T.Use_n_Pages(1);
	SF_s.Run(dbf_s, _s, cnf_s, lit_s);
	T.Run(_s, _out, func);
	SF_s.WaitUntilDone();
	T.WaitUntilDone();
	Schema out_sch("out_sch", 1, &DA);
	int cnt = clear_pipe(_out, &out_sch, false);
	cout << "\n\n Test returned " << cnt << " records \n";
	dbf_s.Close();
	(cnt == 1) ? ASSERT_TRUE(true) : ASSERT_FALSE(false);
}

TEST(Select_File_Relational_Operator, testing_3)
{

	cout << "Running SelectFile operation on PARTSUPP records" << endl;
	char *pred_ps = "(ps_supplycost < 1.03)";
	init_SF_ps(pred_ps, 100);
	SF_ps.Use_n_Pages(4);
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();
	int cnt = clear_pipe(_ps, ps->schema(), false);
	cout << "\n\n Test returned " << cnt << " records \n";
	dbf_ps.Close();
	(cnt > 0) ? ASSERT_TRUE(true) : ASSERT_FALSE(false);
}

TEST(Project_Relational_Operator, testing_4)
{
	cout << "Running project operation on PART records" << endl;
	char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
	init_SF_p(pred_p, 100);
	Project P_p;
	Pipe _out(pipesz);
	int keepMe[] = {0, 1, 7};
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	P_p.Use_n_Pages(buffsz);
	SF_p.Run(dbf_p, _p, cnf_p, lit_p);
	P_p.Run(_p, _out, keepMe, numAttsIn, numAttsOut);
	SF_p.WaitUntilDone();
	P_p.WaitUntilDone();
	Attribute att3[] = {IA, SA, DA};
	Schema out_sch("out_sch", numAttsOut, att3);
	int cnt = clear_pipe(_out, &out_sch, false);
	cout << "\n\n Test returned " << cnt << " records \n";
	dbf_p.Close();
	(cnt > 0) ? ASSERT_TRUE(true) : ASSERT_FALSE(false);
}

int main(int argc, char **argv)
{
	setup();
	std::cout << "Gtest for Relational Operator" << std::endl;
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
	cleanup();
}