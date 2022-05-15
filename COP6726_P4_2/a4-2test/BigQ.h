#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
using namespace std;

class BigQ
{

private:
	Pipe *inputPipe;		 
	Pipe *outputPipe;		 
	OrderMaker *sortedOrder; 
	int *runLength;			 
	File *runsFile;			 
	vector<int> runPointers; 
	pthread_t workerthread;  

public:
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();
};

class BigQInfo
{
public:
	Pipe *input;			
	Pipe *output;			 
	OrderMaker *sortedOrder;
	int runlen;				 
	File *runsFile;			
public:
	void sortRun(vector<Record *> &);
	int addRuntoFile(vector<Record *> &);
};

#endif
