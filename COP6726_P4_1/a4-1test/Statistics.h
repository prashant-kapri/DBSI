#ifndef STATISTICS_
#define STATISTICS_
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string.h>
#include <cmath>
#include <utility>
#include "ParseTree.h"

using namespace std;

typedef unordered_map<string, unordered_map<string, int>> stringHashmapParser;
typedef unordered_map<string, int> stringIntParser;

class Statistics
{

private:
	bool apply;
	stringHashmapParser *attrDataStore;
	stringIntParser *relDataStore;

public:
	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	
	void CopyRel(char *oldName, char *newName);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif