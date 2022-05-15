#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <queue>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
using namespace std;

typedef struct
{
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	int runlen;
} BigQWorker;

void *bigQWorkerMain(void *arg); // used for retrieving record from input pipe and sorting record and then putting to output pipe

class Start // Start exhibit run for merging
{
public:
	int Next();
	Record *top;
	Start(File *file, int pStart, int size);

private:
	File *fBuffer;
	int pStart;
	int size;
	int pCurrent;
	Page pBuffer;
};

class CompareAB // Comparing start for two CNF
{
public:
	bool operator()(Start *left, Start *right);
	CompareAB(OrderMaker *sortorder);

private:
	OrderMaker *sortorder;
};

class CompareRecords // Compare Record for given CNF
{
public:
	bool operator()(Record *left, Record *right);
	CompareRecords(OrderMaker *sortorder);

private:
	OrderMaker *sortorder;
};

void *runRecordQueue(priority_queue<Record *, vector<Record *>, CompareRecords> &recordQ,
					 priority_queue<Start *, vector<Start *>, CompareAB> &startQ, File &file, Page &pBuffer, int &pIndex); // used for taking page of record and pushing it to priority queue

class BigQ
{
public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();
};

#endif