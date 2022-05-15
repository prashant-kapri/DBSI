#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

typedef struct
{
	Pipe *ip;
	Pipe *op;
	int numAttsInput;
	int numAttsOutput;
	int *keepMe;
} ProjectArg;

typedef struct
{
	Pipe *ipL;
	Pipe *ipR;
	Pipe *op;
	CNF *selOp;
	Record *literal;
	int runLen;
} JoinArg;

typedef struct
{
	Pipe *ip;
	Pipe *op;
	OrderMaker *order;
	int runLen;
} DuplicateRemovalArg;

typedef struct
{
	Pipe *ip;
	FILE *outFile;
	Schema *schema;
} WriteOutArg;

class RelationalOp
{
public:
	virtual void WaitUntilDone() = 0;

	virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp
{

private:
	pthread_t worker;

public:
	void Run(DBFile &inFile, Pipe &op, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class SelectPipe : public RelationalOp
{
private:
	pthread_t worker;

public:
	void Run(Pipe &ip, Pipe &op, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};
class Project : public RelationalOp
{
public:
	void Run(Pipe &ip, Pipe &op, int *keepMe, int numAttributeIp, int numAttributeOp);
	void WaitUntilDone();
	void Use_n_Pages(int n);

private:
	pthread_t workerThread;
};

class Sum : public RelationalOp
{
private:
	pthread_t worker;

public:
	void Run(Pipe &ip, Pipe &op, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};
class GroupBy : public RelationalOp
{
private:
	pthread_t worker;

public:
	int use_n_pages = 16;
	void Run(Pipe &ip, Pipe &op, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Join : public RelationalOp
{
public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &op, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);

private:
	pthread_t workerThread;
	int runLen;
};

class DuplicateRemoval : public RelationalOp
{
public:
	void Run(Pipe &ip, Pipe &op, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);

private:
	pthread_t workerThread;
	int runLen;
};

class WriteOut : public RelationalOp
{
public:
	void Run(Pipe &ip, FILE *outFile, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);

private:
	pthread_t workerThread;
};

void *ProjectWorker(void *arg);

void *JoinWorker(void *arg);

void JoinWorker_AddMergedRecord(Record *leftRecord, Record *rightRecord, Pipe *pipe);

void JoinWorker_Merge(JoinArg *joinArg, OrderMaker *leftOrder, OrderMaker *rightOrder);

void JoinWorker_BlockNested(JoinArg *joinArg);

void *DuplicateRemovalWorker(void *arg);

void *WriteOutWorker(void *arg);

#endif