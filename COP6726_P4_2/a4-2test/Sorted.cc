#include "Sorted.h"
#include "Comparison.h"
#include <fstream>
#include <string.h>
#include "Heap.h"

SortedFile::SortedFile(/* args */)
{
    this->fileBuffer = new File();     // file instance
    this->writeBuffer = new Page();    // page instance, write buffer
    this->readBuffer = new Page();     // page instance, read buffer
    this->isEmptyWriteBuffer = true;   // to check if write buffer is empty
    this->endoffile = false;           // to check end of file has reached
    this->pageIdx = 0;                 // setting the pageIdx intially to zero
    this->sortedFileMode = R;          // initially mode of file is set to Reading
    this->bigQ = NULL;                 // BigQ is kept NULL initially
    this->inputPipe = new Pipe(1000);  // new inputpipe is created with size 1000
    this->outputPipe = new Pipe(1000); // new ouputpipe is created with size 1000
    this->queryOrderMaker = NULL;      // query Order Maker used for GetNext
    this->isQueryChanged = true;       // initial query change is set to true
}

SortedFile::SortedFile(OrderMaker *sortOrder, int runlen)
{
    this->fileBuffer = new File();   // file instance
    this->writeBuffer = new Page();  // page instance, write buffer
    this->readBuffer = new Page();   // page instance, read buffer
    this->isEmptyWriteBuffer = true; // to check if write buffer is empty
    this->endoffile = false;         // to check end of file has reached
    this->pageIdx = 0;               // setting the pageIdx intially to zero
    this->sortInfo = new SortInfo(); // setting the sorted order and runlen
    this->sortInfo->myOrder = sortOrder;
    this->sortInfo->runLength = runlen;
    this->sortedFileMode = R;          // initially mode of file is set to Reading
    this->bigQ = NULL;                 // BigQ is kept NULL initially
    this->inputPipe = new Pipe(1000);  // new inputpipe is created with size 1000
    this->outputPipe = new Pipe(1000); // new ouputpipe is created with size 1000
    this->queryOrderMaker = NULL;      // query Order Maker used for GetNext
    this->isQueryChanged = true;       // initial query change is set to true
}

int SortedFile::Create(char *f_path, fType f_type, void *startup)
{
    file_path = f_path;
    char metadata[100];
    strcpy(metadata, f_path);
    strcat(metadata, "-metadata.header");
    this->isEmptyWriteBuffer = true;
    ofstream fileOutputStream;
    fileOutputStream.open(metadata);
    SortInfo *varSortInfo = ((SortInfo *)startup);
    fileOutputStream << "sorted" << endl;
    fileOutputStream << varSortInfo->runLength << endl;
    varSortInfo->myOrder->Print_metadata(fileOutputStream);
    fileBuffer->Open(0, (char *)f_path);
    fileOutputStream.close();
    return 1;
}

int SortedFile::Open(char *fpath)
{
    file_path = fpath;
    this->fileBuffer->Open(1, (char *)fpath);
    this->endoffile = false;
    this->pageIdx = 0;
    return 1;
}

int SortedFile::Close()
{
    if (sortedFileMode == W)
    {
        sortedFileMode = R;
        this->Merge();
    }
    fileBuffer->Close();

    return 1;
}

void SortedFile::Load(Schema &myschema, char *loadpath)
{

    if (sortedFileMode == R)
    {
        sortedFileMode = W;
        inputPipe = new Pipe(1000);
        outputPipe = new Pipe(1000);
        bigQ = new BigQ(*inputPipe, *outputPipe, *sortInfo->myOrder, sortInfo->runLength);
    }

    FILE *fileInputStream = fopen(loadpath, "r");
    Record currRecord;
    while (currRecord.SuckNextRecord(&myschema, fileInputStream))
    {
        inputPipe->Insert(&currRecord);
    }
    fclose(fileInputStream);
}

void SortedFile::MoveFirst()
{

    if (sortedFileMode == W)
    {
        this->Merge();
        sortedFileMode = R;
    }

    this->fileBuffer->GetPage(this->readBuffer, 0);
    this->isQueryChanged = true;
}

void SortedFile::Add(Record &addme)
{

    if (this->sortedFileMode == R)
    {

        this->sortedFileMode = W;
        this->inputPipe = new Pipe(100);
        this->outputPipe = new Pipe(100);
        this->bigQ = new BigQ(*inputPipe, *outputPipe, *sortInfo->myOrder, sortInfo->runLength);
    }
    inputPipe->Insert(&addme);
    this->isQueryChanged = true;
}

void SortedFile::Merge()
{

    inputPipe->ShutDown();

    Heap *mergeFile = new Heap();
    mergeFile->Create("mergefile.bin", heap, NULL);

    Record *currRec = new Record();
    Record *currRecPQ = new Record();
    readBuffer->EmptyItOut();

    if (fileBuffer->GetLength() == 0)
    {
        while (outputPipe->Remove(currRecPQ))
        {
            mergeFile->Add(*currRecPQ);
        }
    }
    else
    {
        fileBuffer->GetPage(readBuffer, 0);
        pageIdx = 1;
        ComparisonEngine cmp;
        int pipeResult = outputPipe->Remove(currRecPQ);
        int fileResult = readBuffer->GetFirst(currRec);

        while (fileResult && pipeResult)
        {

            if (cmp.Compare(currRec, currRecPQ, sortInfo->myOrder) > 0)
            {
                mergeFile->Add(*currRecPQ);
                pipeResult = outputPipe->Remove(currRecPQ);
            }
            else if (cmp.Compare(currRec, currRecPQ, sortInfo->myOrder) <= 0)
            {
                mergeFile->Add(*currRec);
                fileResult = readBuffer->GetFirst(currRec);

                if (!fileResult && pageIdx < (fileBuffer->GetLength() - 1))
                {
                    delete readBuffer;
                    readBuffer = new Page();
                    fileBuffer->GetPage(readBuffer, pageIdx);
                    fileResult = readBuffer->GetFirst(currRec);
                    pageIdx++;
                }
            }
        }
        while (pipeResult)
        {
            mergeFile->Add(*currRecPQ);
            pipeResult = outputPipe->Remove(currRecPQ);
        }
        while (fileResult)
        {
            mergeFile->Add(*currRec);
            fileResult = readBuffer->GetFirst(currRec);

            if (!fileResult && pageIdx < (fileBuffer->GetLength() - 1))
            {
                delete readBuffer;
                readBuffer = new Page();
                fileBuffer->GetPage(readBuffer, pageIdx);
                fileResult = readBuffer->GetFirst(currRec);
                pageIdx++;
            }
        }
    }
    mergeFile->Close();

    fileBuffer->Close();
    rename("mergefile.bin", file_path);
    this->Open(file_path);
    delete inputPipe;
    delete outputPipe;
    delete bigQ;
    inputPipe = NULL;
    outputPipe = NULL;
    bigQ = NULL;
}

int SortedFile::GetNext(Record &fetchme)
{
    if (sortedFileMode == W)
    {
        sortedFileMode = R;
        this->Merge();
    }
    if (this->endoffile == false)
    {
        int result = this->readBuffer->GetFirst(&fetchme);
        if (result == 0)
        {
            pageIdx++;
            if (pageIdx >= this->fileBuffer->GetLength() - 1)
            {
                this->endoffile = true;
                return 0; // return 0 as end of file has reached
            }
            else
            {
                this->fileBuffer->GetPage(this->readBuffer, pageIdx);
                this->readBuffer->GetFirst(&fetchme);
            }
        }
        return 1; // return 1 on success, next record is fed into fetchme
    }
    return 0; // return 0 as end of file has reached
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{

    if (sortedFileMode == W)
    {
        sortedFileMode = R;
        this->Merge();
    }
    ComparisonEngine cmpEng;

    OrderMaker queryOrder, queryOrderForLiteral;
    OrderMaker::makeQueryOrderMaker(sortInfo->myOrder, cnf, queryOrder, queryOrderForLiteral);
    queryOrderMaker = cnf.getQueryOrderMaker(*(sortInfo->myOrder));
    if (!binarySearch(fetchme, &queryOrder, literal, &queryOrderForLiteral, cmpEng))
    {
        return 0;
    }
    do
    {
        if (cmpEng.Compare(&fetchme, &literal, &cnf))
            return 1;
    } while (GetNext(fetchme));

    return 0;
}

int SortedFile::binarySearch(Record &fetchme, OrderMaker *queryOrder, Record &literal, OrderMaker *queryOrderForLiteral, ComparisonEngine &cmpEng)
{

    if (!GetNext(fetchme))
        return 0;

    int resultFlag = cmpEng.Compare(&fetchme, queryOrder, &literal, queryOrderForLiteral);

    if (resultFlag == 0)
        return 1; //  Found

    if (resultFlag > 0)
        return 0;

    int lowPageIndex = pageIdx, highPageIndex = fileBuffer->GetLength() - 1, midPageIndex = (lowPageIndex + highPageIndex) / 2;

    while (lowPageIndex < midPageIndex)
    {

        midPageIndex = (lowPageIndex + highPageIndex) / 2;
        fileBuffer->GetPage(readBuffer, midPageIndex);
        pageIdx = midPageIndex + 1;
        if (!GetNext(fetchme))
            printf("No Record to be fetched");
        resultFlag = cmpEng.Compare(&fetchme, queryOrder, &literal, queryOrderForLiteral);
        if (resultFlag < 0)
            lowPageIndex = midPageIndex;
        else if (resultFlag > 0)
            highPageIndex = midPageIndex - 1;
        else
            highPageIndex = midPageIndex;
    }
    fileBuffer->GetPage(readBuffer, lowPageIndex);
    pageIdx = lowPageIndex + 1;
    do
    {
        if (!GetNext(fetchme))
            return 0;
        resultFlag = cmpEng.Compare(&fetchme, queryOrder, &literal, queryOrderForLiteral);
    } while (resultFlag < 0);

    return ((resultFlag == 0) ? 1 : 0);
}

SortedFile::~SortedFile()
{
}
