#ifndef NODE_QUERY_TREE_H
#define NODE_QUERY_TREE_H

#include <iostream>
#include "Schema.h"
#include "DBFile.h"
#include "Heap.h"
#include "Sorted.h"
#include "RelOp.h"

using namespace std;

enum NodeTreeQueryType
{
    SELECTFILE,
    SELECTPIPE,
    PROJECT,
    JOIN,
    SUM,
    GROUPBY,
    DISTINCT,
    WRITEOUT
};

class NodeTreeQuery
{

public:

    DBFile *db;
    NodeTreeQueryType typeOfNode;
    NodeTreeQuery *parent, *left, *right;

    SelectFile *sf;
    SelectPipe *sp;
    Project *p;
    Join *j;
    Sum *s;
    GroupBy *gb;
    DuplicateRemoval *dr;
    WriteOut *wo;

    Schema *schema;
    OrderMaker *oMaker;
    FuncOperator *operatorFunc;
    Function *function;
    AndList *cnf;
    CNF *operatorForCNF;
    string fPath;

    int leftPipeId, rightPipeId, opPipeId;
    int noAttrIp, noAttrOp;
    int *attributesToKeep, numberOfAttributesToKeep;
    Pipe *leftPipeIp, *rightPipeIp, *outputPipe;

    NodeTreeQuery();
    ~NodeTreeQuery();
    void Run();
    void printNode();
    void WaitUntilDone();

    void genFunc();
    void printFunction();

    void genSchema();
    void printCNF();

    void genOrderMaker(int numberOfAttributes, vector<int> attributes, vector<int> types);
    void printInOrder();

    string getTypeName();
    void setNodeTreeQueryType(NodeTreeQueryType type);
    NodeTreeQueryType getNodeTreeQueryType();


};

#endif