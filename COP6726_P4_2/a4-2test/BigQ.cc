#include "BigQ.h"
#include <vector>
#include <queue>
#include <algorithm>

void *worker(void *args);

struct PriorityQueue_Record
{
	Record *currRecord;
	int pageNum;
	int bufferNum;
};

class PriorityQueue_Comparator
{
private:
	OrderMaker *sortOrder;
	ComparisonEngine cmp;

public:
	PriorityQueue_Comparator(OrderMaker *sortOrder)
	{
		this->sortOrder = sortOrder;
	}
	bool operator()(PriorityQueue_Record *r1, PriorityQueue_Record *r2)
	{
		if (cmp.Compare(r1->currRecord, r2->currRecord, sortOrder) > 0)
		{
			return true;
		}
		return false;
	}
};

class RecordComparator
{
private:
	OrderMaker *sortOrder;
	ComparisonEngine cmp;

public:
	RecordComparator(OrderMaker *sortOrder)
	{
		this->sortOrder = sortOrder;
	}
	bool operator()(Record *r1, Record *r2)
	{
		if (cmp.Compare(r1, r2, sortOrder) < 0)
			return true;
		return false;
	}
};

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	BigQInfo *diffBigQ = new BigQInfo();
	diffBigQ->input = &in;
	diffBigQ->output = &out;
	diffBigQ->sortedOrder = &sortorder;
	diffBigQ->runlen = runlen;

	pthread_create(&workerthread, NULL, &worker, (void *)diffBigQ);
}
BigQ::~BigQ()
{
}

void *worker(void *args)
{
	BigQInfo *mybigqInfo = new BigQInfo();
	mybigqInfo = (BigQInfo *)args;

	char filename[50];

	timespec time;
	clock_gettime(CLOCK_REALTIME, &time);
	int timestamp = time.tv_sec * 1000000000 + time.tv_nsec;
	if (timestamp < 0)
		timestamp *= -1;

	sprintf(filename, "bigq__%d.bin", timestamp);

	mybigqInfo->runsFile = new File();
	mybigqInfo->runsFile->Open(0, filename);
	mybigqInfo->runsFile->Close();
	mybigqInfo->runsFile->Open(1, filename);

	int runlen = mybigqInfo->runlen;
	int pagesInRun = 0;
	Page *buffer = new Page();
	Record *currRecord = new Record();
	vector<int> runPointers;
	runPointers.push_back(0);
	vector<Record *> curr_run_vector;
	int num_of_runs = 0;
	Record *tempRec = new Record();

	while (mybigqInfo->input->Remove(currRecord))
	{

		if (pagesInRun < runlen)
		{
			int result = buffer->Append(currRecord);
			if (result == 0)
			{
				pagesInRun++;
				while (buffer->GetFirst(tempRec))
				{
					curr_run_vector.push_back(tempRec);
					tempRec = new Record();
				}
				buffer->EmptyItOut();
				buffer->Append(currRecord);
			}
		}
		else
		{
			pagesInRun = 0;
			mybigqInfo->sortRun(curr_run_vector);
			num_of_runs++;
			int runhead = mybigqInfo->addRuntoFile(curr_run_vector);
			runPointers.push_back(runhead);
			curr_run_vector.clear();
			buffer->Append(currRecord);
		}
	}

	num_of_runs++;
	while (buffer->GetFirst(tempRec))
	{
		curr_run_vector.push_back(tempRec);
		tempRec = new Record();
	}
	mybigqInfo->sortRun(curr_run_vector);
	int runhead = mybigqInfo->addRuntoFile(curr_run_vector);
	runPointers.push_back(runhead);
	curr_run_vector.clear();
	mybigqInfo->runsFile->Close();

	mybigqInfo->runsFile = new File();
	mybigqInfo->runsFile->Open(1, filename);
	typedef priority_queue<PriorityQueue_Record *, std::vector<PriorityQueue_Record *>, PriorityQueue_Comparator> pq_merger_type;
	pq_merger_type pq_merger(mybigqInfo->sortedOrder);
	Page *runBuffers[num_of_runs];
	PriorityQueue_Record *pq_record = new PriorityQueue_Record();
	currRecord = new Record();
	int i = 0;

	while (i < num_of_runs)
	{
		runBuffers[i] = new Page();
		mybigqInfo->runsFile->GetPage(runBuffers[i], runPointers[i]);
		runBuffers[i]->GetFirst(currRecord);
		pq_record->currRecord = currRecord;
		pq_record->pageNum = runPointers[i];
		pq_record->bufferNum = i;
		pq_merger.push(pq_record);
		pq_record = new PriorityQueue_Record();
		currRecord = new Record();
		i++;
	}

	while (!pq_merger.empty())
	{
		pq_record = pq_merger.top();
		int pageNum = pq_record->pageNum;
		int bufferNum = pq_record->bufferNum;

		mybigqInfo->output->Insert(pq_record->currRecord);
		pq_merger.pop();

		Record *newRecord = new Record();
		if (runBuffers[bufferNum]->GetFirst(newRecord) == 0)
		{

			pageNum = pageNum + 1;
			// moving to next page in the current run of the min record in priority queue
			if (pageNum < (mybigqInfo->runsFile->GetLength() - 1) && (pageNum < runPointers[bufferNum + 1]))
			{
				runBuffers[bufferNum]->EmptyItOut();
				mybigqInfo->runsFile->GetPage(runBuffers[bufferNum], pageNum);

				if (runBuffers[bufferNum]->GetFirst(newRecord) != 0)
				{
					pq_record->currRecord = newRecord;
					pq_record->bufferNum = bufferNum;
					pq_record->pageNum = pageNum;
					pq_merger.push(pq_record);
				}
			}
		}
		else
		{
			pq_record->currRecord = newRecord;
			pq_record->bufferNum = bufferNum;
			pq_record->pageNum = pageNum;
			pq_merger.push(pq_record);
		}
	}

	mybigqInfo->runsFile->Close();
	mybigqInfo->output->ShutDown();
	remove(filename); // remove the bins file no longer needed
	pthread_exit(NULL);
}

void BigQInfo::sortRun(vector<Record *> &vector)
{
	sort(vector.begin(), vector.end(), RecordComparator(sortedOrder));
}





int BigQInfo::addRuntoFile(vector<Record *> &vector)
{

	Page *buffer = new Page();

	for (int i = 0; i < vector.size(); i++)
	{
		if (buffer->Append(vector[i]) == 0)
		{
			if (this->runsFile->GetLength() == 0)
			{
				this->runsFile->AddPage(buffer, 0);
			}
			else
			{
				this->runsFile->AddPage(buffer, this->runsFile->GetLength() - 1);
			}
			buffer->EmptyItOut();
			buffer->Append(vector[i]);
		}
	}
	if (this->runsFile->GetLength() == 0)
	{
		this->runsFile->AddPage(buffer, 0);
	}
	else
	{
		this->runsFile->AddPage(buffer, this->runsFile->GetLength() - 1);
	}
	return this->runsFile->GetLength() - 1;
}

