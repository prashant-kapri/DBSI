#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include "RelStats.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string.h>
#include <cmath>
#include <utility>

using namespace std;

class Statistics
{

public:

	map<string, RelStats> relationList;
	map<string, RelStatSet> relationSets;
	map<string, string> table;
	
	vector<RelStatSet> setsList;


	Statistics();
	Statistics(Statistics &copyMe); 
	~Statistics();

	Statistics& operator= (Statistics& assignMe);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void AddAtt(char *relName, char *attName, int numDistincts);
	void AddRel(char *relName, int numTuples);
	void AddRel(char *relName, double numTuples);
	
	void CopyRel(char *oldName, char *newName);
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);

	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	double approxEstimate(struct AndList *parseTree, RelStatSet toEstimate, vector<int> &indexVec, double joinEstimate);
	
	double parseJoin(struct AndList *parseTree);
	double getTupleCount(string rel, double joinEstimate);

	void parseRelAttr(struct Operand *oper, string &rel, string &attribute);
	void parseRel(struct Operand *oper, string &rel);
	
};

#endif