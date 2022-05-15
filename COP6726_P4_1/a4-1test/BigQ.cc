#include "BigQ.h"
#include "DBFile.h"

// Used for puting records into a run, which is disk file based
void BigQ::recordQueueToRun(priority_queue<Record *, vector<Record *>, RecordComparer> &recordQueue,
                            priority_queue<Run *, vector<Run *>, RunComparer> &runQueue, File &file, Page &bufferPage, int &pageIndex)
{
    bufferPage.EmptyItOut();
    int startIndex = pageIndex;
    while (!recordQueue.empty())
    {
        Record *tmpRecord = new Record;
        tmpRecord->Copy(recordQueue.top());
        recordQueue.pop();
        if (bufferPage.Append(tmpRecord) == 0)
        {
            file.AddPage(&bufferPage, pageIndex++);
            bufferPage.EmptyItOut();
            bufferPage.Append(tmpRecord);
        }
    }
    file.AddPage(&bufferPage, pageIndex++);
    bufferPage.EmptyItOut();
    Run *run = new Run(&file, startIndex, pageIndex - startIndex);
    runQueue.push(run);
}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
    pthread_t worker;
    this->in = &in;
    this->out = &out;
    this->order = &sortorder;
    this->runlen = runlen;
    pthread_create(&worker, NULL, workerMain, (void *)this);
}

BigQ::~BigQ()
{
}
void *workerMain(void *arg)
{
    ((BigQ *)arg)->BigQMain();
    return NULL;
}

void BigQ::BigQMain()
{
    vector<Record *> recBuff;
    Record curRecord;

    priority_queue<Run *, vector<Run *>, RunComparer> runQueue(this->order);
    priority_queue<Record *, vector<Record *>, RecordComparer> recordQueue(this->order);
    File file;

    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;
    char *fileName = new char[100];

    sprintf(fileName, "%d.bin", pthread_self());
    file.Open(0, fileName);

    while (this->in->Remove(&curRecord) == 1)
    {
        Record *tmpRecord = new Record;
        tmpRecord->Copy(&curRecord);
        if (bufferPage.Append(&curRecord) == 0)
        {
            pageCounter++;
            bufferPage.EmptyItOut();

            if (pageCounter == this->runlen)
            {
                recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
                recordQueue = priority_queue<Record *, vector<Record *>, RecordComparer>(this->order);
                pageCounter = 0;
            }
            bufferPage.Append(&curRecord);
        }
        recordQueue.push(tmpRecord);
    }
    if (!recordQueue.empty())
    {
        recordQueueToRun(recordQueue, runQueue, file, bufferPage, pageIndex);
        recordQueue = priority_queue<Record *, vector<Record *>, RecordComparer>(this->order);
    }
    // DBFile dbFileHeap;
    // dbFileHeap.Create("tempDifFile.bin", heap, nullptr);
    Run *run;
    Schema schema("catalog", "supplier");
    while (!runQueue.empty())
    {
        run = runQueue.top();
        runQueue.pop();
        // dbFileHeap.Add(*(run->topRecord));
        Record *waitToInsert = new Record();
        waitToInsert->Copy(run->topRecord);
        this->out->Insert(waitToInsert);
        if (run->UpdateTopRecord() == 1)
        {
            runQueue.push(run);
        }
    }
    // dbFileHeap.Close();
    file.Close();
    this->out->ShutDown();
    remove(fileName);
}

Run::Run(File *file, int start, int length)
{
    fileBase = file;
    startPage = start;
    runLength = length;
    curPage = start;
    fileBase->GetPage(&bufferPage, startPage);
    topRecord = new Record;
    UpdateTopRecord();
}

int Run::UpdateTopRecord()
{
    if (bufferPage.GetFirst(topRecord) == 0)
    {
        curPage++;
        if (curPage >= startPage + runLength)
        {
            return 0;
        }
        bufferPage.EmptyItOut();
        fileBase->GetPage(&bufferPage, curPage);
        bufferPage.GetFirst(topRecord);
    }
    return 1;
}

RecordComparer::RecordComparer(OrderMaker *orderMaker)
{
    order = orderMaker;
}

bool RecordComparer::operator()(Record *left, Record *right)
{
    ComparisonEngine comparisonEngine;
    comparisonEngine.Compare(left, right, order) >= 0 ? true : false;
}

RunComparer::RunComparer(OrderMaker *orderMaker)
{
    order = orderMaker;
}

bool RunComparer::operator()(Run *left, Run *right)
{
    ComparisonEngine comparisonEngine;
    (comparisonEngine.Compare(left->topRecord, right->topRecord, order) >= 0) ? true : false;
}
