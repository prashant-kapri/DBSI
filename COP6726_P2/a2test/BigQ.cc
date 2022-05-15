#include "BigQ.h"

// Used for puting records into a run, which is disk file based
void *runRecordQueue(priority_queue<Record *, vector<Record *>, CompareRecords> &recordQ,
					   priority_queue<Start *, vector<Start *>, CompareAB> &startQ, File &file, Page &pBuffer, int &pIndex)
{

	pBuffer.EmptyItOut();
	int startIndex = pIndex;
	while (!recordQ.empty())
	{
		Record *tmpRecord = new Record;
		//copy the record to a temp file, then pop it from queue and append it to the buffer to add page 
		tmpRecord->Copy(recordQ.top());
		recordQ.pop();
		if (pBuffer.Append(tmpRecord) == 0)
		{
			file.AddPage(&pBuffer, pIndex++);
			pBuffer.EmptyItOut();
			pBuffer.Append(tmpRecord);
		}
	}
	file.AddPage(&pBuffer, pIndex++);
	pBuffer.EmptyItOut();
	Start *run = new Start(&file, startIndex, pIndex - startIndex);
	startQ.push(run);
	return NULL;
}

void *bigQWorkerMain(void *arg)
{
	BigQWorker *workerArg = (BigQWorker *)arg;
	
	// Buffer page for disk based file
	Page pBuffer;
	int pIndex = 0;
	int pageCounter = 0;

	vector<Record *> recBuff;
	Record currentRec;

	// sorting disk based file
	File file;
	char *fileName = "tmp.bin";
	file.Open(0, fileName);
	
	priority_queue<Start *, vector<Start *>, CompareAB> startQ(workerArg->sortorder);
	priority_queue<Record *, vector<Record *>, CompareRecords> recordQ(workerArg->sortorder);
	
	// retrieve records from input pipe
	while (workerArg->in->Remove(&currentRec) == 1)
	{
		Record *tmpRecord = new Record;
		tmpRecord->Copy(&currentRec);
		// Add to another page if current page is full
		if (pBuffer.Append(&currentRec) == 0)
		{
			pageCounter++;
			pBuffer.EmptyItOut();

			// Add to another run if current run is full
			if (pageCounter == workerArg->runlen)
			{
				runRecordQueue(recordQ, startQ, file, pBuffer, pIndex);
				recordQ = priority_queue<Record *, vector<Record *>, CompareRecords>(workerArg->sortorder);
				pageCounter = 0;
			}

			pBuffer.Append(&currentRec);
		}

		recordQ.push(tmpRecord);
	}
	if (!recordQ.empty())
	{
		runRecordQueue(recordQ, startQ, file, pBuffer, pIndex);
		recordQ = priority_queue<Record *, vector<Record *>, CompareRecords>(workerArg->sortorder);
	}
	while (!startQ.empty())
	{
		Start *run = startQ.top();
		startQ.pop();
		workerArg->out->Insert(run->top);
		if (run->Next() == 1)
		{
			startQ.push(run);
		}
	}
	file.Close();
	workerArg->out->ShutDown();
	return NULL;
}


Start::Start(File *file, int start, int length)
{
	pStart = start;
	size = length;
	fBuffer = file;

	pCurrent = start;
	fBuffer->GetPage(&pBuffer, pStart);
	
	top = new Record;
	Next();
}

int Start::Next()
{
	if (pBuffer.GetFirst(top) == 0)
	{
		pCurrent++;
		if (pCurrent == pStart + size)
		{
			return 0;
		}
		pBuffer.EmptyItOut();
		fBuffer->GetPage(&pBuffer, pCurrent);
		pBuffer.GetFirst(top);
	}
	return 1;
}

CompareAB::CompareAB(OrderMaker *orderMaker)
{
	sortorder = orderMaker;
}

bool CompareAB::operator()(Start *left, Start *right)
{
	ComparisonEngine ce;
	return (ce.Compare(left->top, right->top, sortorder) >=0 ? true : false);
}

CompareRecords::CompareRecords(OrderMaker *orderMaker)
{
	sortorder = orderMaker;
}

bool CompareRecords::operator()(Record *left, Record *right)
{
	ComparisonEngine ce;
	return (ce.Compare (left, right, sortorder) >= 0) ? true : false;
}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	pthread_t slave;
	BigQWorker* workerArg = new BigQWorker;
    workerArg->in = &in;
    workerArg->out = &out;
    workerArg->sortorder = &sortorder;
    workerArg->runlen = runlen;
	pthread_create(&slave, NULL, bigQWorkerMain, (void *)workerArg);
	pthread_join(slave, NULL);

	out.ShutDown();
}

BigQ::~BigQ()
{
}