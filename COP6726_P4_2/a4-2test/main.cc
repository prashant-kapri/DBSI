
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "NodeTreeQuery.h"
#include <stdio.h>

using namespace std;

extern "C"
{
	int yyparse(void); // defined in y.tab.c
}

extern struct TableList *tables;
extern struct FuncOperator *finalFunction;
extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern struct NameList *attsToSelect;

extern int distinctAtts;
extern int distinctFunc;

vector<AndList> joinsOrdOptimization(vector<AndList> joinsVector, Statistics *statsObj)
{
	int idx = 0, minIdx = 0, c = 0;
	double minValue = -1.0;
	double approxValue = 0.0;

	vector<AndList> newOrdVector;
	vector<string> joinedRel;

	string rel1, rel2;
	AndList join;

	newOrdVector.reserve(joinsVector.size());
	while (joinsVector.size() > 1)
	{
		while (joinsVector.size() > idx)
		{
			join = joinsVector[idx];
			statsObj->parseRel(join.left->left->left, rel1);
			statsObj->parseRel(join.left->left->right, rel2);
			if (minValue == -1.0)
			{
				char *relations[] = {(char *)rel1.c_str(), (char *)rel2.c_str()};
				minValue = statsObj->Estimate(&join, relations, 2);
				minIdx = idx;
			}
			else
			{
				char *relations[] = {(char *)rel1.c_str(), (char *)rel2.c_str()};
				approxValue = statsObj->Estimate(&join, relations, 2);
				if (minValue > approxValue)
				{
					minValue = approxValue;
					minIdx = idx;
				}
			}
			idx++;
		}
		joinedRel.push_back(rel1);
		joinedRel.push_back(rel2);
		newOrdVector.push_back(joinsVector[minIdx]);
		minValue = -1.0;
		idx = 0;
		c++;

		joinsVector.erase(joinsVector.begin() + minIdx);
	}
	newOrdVector.insert(newOrdVector.begin() + c, joinsVector[0]);
	return newOrdVector;
}

void predicateDetail(vector<AndList> &joinsVector, vector<AndList> &selectsVector, vector<AndList> &joinDepSelectsVector, Statistics statsObj)
{
	struct OrList *currentOrList;
	while (boolean != 0)
	{
		currentOrList = boolean->left;
		if (currentOrList && currentOrList->left->code == EQUALS && currentOrList->left->left->code == NAME && currentOrList->left->right->code == NAME)
		{
			AndList newAnd = *boolean;
			newAnd.rightAnd = 0;
			joinsVector.push_back(newAnd);
		}
		else
		{
			currentOrList = boolean->left;
			if (currentOrList->left == NULL)
			{
				AndList newAnd = *boolean;
				newAnd.rightAnd = NULL;
				selectsVector.push_back(newAnd);
			}
			else
			{
				vector<string> tablesInvolvedVector;
				while (currentOrList != NULL)
				{
					string relation;
					Operand *oprnd = currentOrList->left->left;
					if (oprnd->code != NAME)
					{
						oprnd = currentOrList->left->right;
					}
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
					AndList newAnd = *boolean;
					newAnd.rightAnd = 0;
					joinDepSelectsVector.push_back(newAnd);
				}
				else
				{
					AndList newAnd = *boolean;
					newAnd.rightAnd = 0;
					selectsVector.push_back(newAnd);
				}
			}
		}
		boolean = boolean->rightAnd;
	}
}

int main()
{
	int pipeID = 1;
	Statistics *fileStats = new Statistics();
	// read statistics file
	fileStats->Read("Statistics.txt");
	cout << "Type SQL Query: ";

	vector<AndList> joinsVector, selectsVector, joinDepSelectsVector;
	map<string, double> joinCostsVector;

	vector<string> relations;
	string projectStartString;

	yyparse();
	TableList *tList = tables;
	while (tList)
	{
		tList->aliasAs ? relations.push_back(tList->aliasAs) : relations.push_back(tList->tableName);

		tList = tList->next;
	}

	predicateDetail(joinsVector, selectsVector, joinDepSelectsVector, *fileStats);

	cout << endl;

	cout << "No. Selects: " << selectsVector.size() << endl;
	cout << "No. Joins: " << joinsVector.size() << endl;

	NodeTreeQuery *parentNode = NULL;
	NodeTreeQuery *insertionNode = NULL;
	NodeTreeQuery *traverse;
	TableList *tableIdx = tables;
	map<string, NodeTreeQuery *> leaf;

	while (tableIdx != 0)
	{

		if (tableIdx->aliasAs != 0)
		{
			leaf.insert(make_pair(tableIdx->aliasAs, new NodeTreeQuery()));
			fileStats->CopyRel(tableIdx->tableName, tableIdx->aliasAs);
		}
		else
		{
			leaf.insert(pair<string, NodeTreeQuery *>(tableIdx->tableName, new NodeTreeQuery()));
		}

		insertionNode = leaf[tableIdx->aliasAs];
		insertionNode->schema = new Schema("catalog", tableIdx->tableName);
		if (tableIdx->aliasAs != 0)
		{
			insertionNode->schema->updateName(string(tableIdx->aliasAs));
		}
		parentNode = insertionNode;
		insertionNode->opPipeId = pipeID++;

		string s(tableIdx->tableName);
		string loc("bin/" + s + ".bin");
		insertionNode->fPath = strdup(loc.c_str());
		insertionNode->setNodeTreeQueryType(SELECTFILE);
		tableIdx = tableIdx->next;
	}
	int i = 0;
	AndList selectIdx;
	string tableName, attr;

	// select operation plan
	while (i < selectsVector.size())
	{
		selectIdx = selectsVector[i];
		(selectIdx.left->left->left->code == NAME) ? fileStats->parseRel(selectIdx.left->left->left, tableName)
												   : fileStats->parseRel(selectIdx.left->left->right, tableName);

		traverse = leaf[tableName];
		projectStartString = tableName;
		while (traverse->parent != NULL)
		{
			traverse = traverse->parent;
		}

		// tree node query building
		insertionNode = new NodeTreeQuery();
		traverse->parent = insertionNode;
		insertionNode->left = traverse;
		insertionNode->schema = traverse->schema;
		insertionNode->typeOfNode = SELECTPIPE;
		insertionNode->cnf = &selectsVector[i];
		insertionNode->leftPipeId = traverse->opPipeId;
		insertionNode->opPipeId = pipeID++;

		char *applyStats = strdup(tableName.c_str());
		fileStats->Apply(&selectIdx, &applyStats, 1);

		parentNode = insertionNode;
		i++;
	}

	// faster execution planning for joins
	if (joinsVector.size() > 1)
	{
		joinsVector = joinsOrdOptimization(joinsVector, fileStats);
	}

	string rel1, rel2;
	NodeTreeQuery *leftTreeNode;
	NodeTreeQuery *rightTreeNode;
	AndList currentJoin;

	// Join Operation plan
	int j = 0;
	while (j < joinsVector.size())
	{
		currentJoin = joinsVector[j];
		rel1 = "";
		fileStats->parseRel(currentJoin.left->left->left, rel1);
		rel2 = "";
		fileStats->parseRel(currentJoin.left->left->right, rel2);
		tableName = rel1;
		leftTreeNode = leaf[rel1];
		rightTreeNode = leaf[rel2];

		while (rightTreeNode->parent != NULL)
		{
			rightTreeNode = rightTreeNode->parent;
		}

		while (leftTreeNode->parent != NULL)
		{
			leftTreeNode = leftTreeNode->parent;
		}

		// tree node query building for join
		insertionNode = new NodeTreeQuery();
		insertionNode->typeOfNode = JOIN;
		insertionNode->leftPipeId = leftTreeNode->opPipeId;
		insertionNode->rightPipeId = rightTreeNode->opPipeId;
		insertionNode->opPipeId = pipeID++;
		insertionNode->cnf = &joinsVector[j];
		insertionNode->left = leftTreeNode;
		insertionNode->right = rightTreeNode;
		leftTreeNode->parent = insertionNode;
		rightTreeNode->parent = insertionNode;
		insertionNode->genSchema();

		parentNode = insertionNode;
		j++;
	}

	// select op on multiple table op
	int k = 0;
	while (k < joinDepSelectsVector.size())
	{
		traverse = parentNode;

		// tree node query building
		insertionNode = new NodeTreeQuery();
		traverse->parent = insertionNode;
		insertionNode->left = traverse;
		insertionNode->schema = traverse->schema;
		insertionNode->typeOfNode = SELECTPIPE;
		insertionNode->cnf = &joinDepSelectsVector[k];
		insertionNode->leftPipeId = traverse->opPipeId;
		insertionNode->opPipeId = pipeID++;

		parentNode = insertionNode;
		k++;
	}

	// aggregagte function operations query plan
	if (finalFunction != NULL)
	{
		// distinct operation
		if (distinctFunc)
		{
			insertionNode = new NodeTreeQuery();

			// building query tree node
			insertionNode->typeOfNode = DISTINCT;
			insertionNode->left = parentNode;
			insertionNode->leftPipeId = parentNode->opPipeId;
			insertionNode->opPipeId = pipeID++;
			insertionNode->schema = parentNode->schema;
			parentNode->parent = insertionNode;
			parentNode = insertionNode;
		}
		//  SUM operation
		if (groupingAtts == NULL)
		{
			insertionNode = new NodeTreeQuery();

			// tree node query building
			insertionNode->typeOfNode = SUM;
			insertionNode->left = parentNode;
			parentNode->parent = insertionNode;
			insertionNode->leftPipeId = parentNode->opPipeId;
			insertionNode->opPipeId = pipeID++;
			insertionNode->operatorFunc = finalFunction;
			insertionNode->schema = parentNode->schema;
			insertionNode->genFunc();
		}
		else
		{
			// group by operation
			insertionNode = new NodeTreeQuery();

			// tree node query building
			insertionNode->typeOfNode = GROUPBY;
			insertionNode->left = parentNode;
			insertionNode->parent = insertionNode;
			insertionNode->leftPipeId = parentNode->opPipeId;
			insertionNode->opPipeId = pipeID++;
			insertionNode->schema = parentNode->schema;
			insertionNode->oMaker = new OrderMaker();

			int noAttrGrp = 0;

			vector<int> attrsToGroup;
			vector<int> wType;
			NameList *groupTraversal = groupingAtts;
			// fetching attr for GroupBy
			while (groupTraversal)
			{
				noAttrGrp++;
				attrsToGroup.push_back(insertionNode->schema->Find(groupTraversal->name));
				wType.push_back(insertionNode->schema->FindType(groupTraversal->name));
				cout << "Grouping It: " << groupTraversal->name << endl;
				groupTraversal = groupTraversal->next;
			}
			insertionNode->genOrderMaker(noAttrGrp, attrsToGroup, wType);
			insertionNode->operatorFunc = finalFunction;
			insertionNode->genFunc();
		}

		parentNode = insertionNode;
	}

	// Non aggregated query distinct attr planning
	if (distinctAtts)
	{
		insertionNode = new NodeTreeQuery();

		// tree node query building
		insertionNode->typeOfNode = DISTINCT;
		insertionNode->left = parentNode;
		insertionNode->parent = insertionNode;
		insertionNode->leftPipeId = parentNode->opPipeId;
		insertionNode->opPipeId = pipeID++;
		insertionNode->schema = parentNode->schema;

		parentNode = insertionNode;
	}

	// project operation query planning
	if (attsToSelect != NULL)
	{
		traverse = parentNode;
		insertionNode = new NodeTreeQuery();

		// tree node query building
		insertionNode->typeOfNode = PROJECT;
		insertionNode->left = traverse;
		traverse->parent = insertionNode;
		insertionNode->leftPipeId = traverse->opPipeId;
		insertionNode->opPipeId = pipeID++;

		vector<int> idxOfattrsToKeep;

		string attr;
		Schema *o_schema = traverse->schema;
		NameList *attrTraversal = attsToSelect;

		// project attr filter operation
		while (attrTraversal != 0)
		{
			attr = attrTraversal->name;
			idxOfattrsToKeep.push_back(o_schema->Find(const_cast<char *>(attr.c_str())));
			attrTraversal = attrTraversal->next;
		}
		Schema *newSchema = new Schema(o_schema, idxOfattrsToKeep);

		insertionNode->schema = newSchema;
		insertionNode->schema->Print();
	}
	cout << "Printing Tree in Order: " << endl
		 << endl;

	if (insertionNode != NULL)
	{
		// query plan print function
		insertionNode->printInOrder();
	}
}
