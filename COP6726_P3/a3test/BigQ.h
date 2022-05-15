#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

// Struct used as arguement for worker thread
typedef struct
{
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
} WorkerArg;

// It exhibit run for merging
class Run
{

public:
	Run(File *file, int startPage, int runLength);
	int UpdateTopRecord();
	Record *topRecord;

private:
	File *fileBase;
	Page bufferPage;
	int startPage;
	int runLength;
	int curPage;
};

// Comparing start for two CNF
class RecordComparer
{

public:
	bool operator()(Record *left, Record *right);
	RecordComparer(OrderMaker *order);

private:
	OrderMaker *order;
};

// Compare Record for given CNF
class RunComparer
{

public:
	bool operator()(Run *left, Run *right);
	RunComparer(OrderMaker *order);

private:
	OrderMaker *order;
};

// used for retrieving record from input pipe and sorting record and then putting to output pipe
void *workerMain(void *arg);

class BigQ
{

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();
	void BigQMain();
	void recordQueueToRun(priority_queue<Record *, vector<Record *>, RecordComparer> &recordQueue,
						  priority_queue<Run *, vector<Run *>, RunComparer> &runQueue, File &file, Page &bufferPage, int &pageIndex);

private:
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
};
#endif
