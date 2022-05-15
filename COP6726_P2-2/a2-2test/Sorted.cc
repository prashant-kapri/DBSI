#include "Sorted.h"
#include "BigQ.h"
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>

// stub file .. replace it with your own DBFileSort.cc

DBFileSort::DBFileSort()
{
    isWriting = 0;
    pIndex = 0;
}

int DBFileSort ::Start(Record *l, Record *literal, Comparison *c)
{
    // used for quering CNF and compare it according to different attributes type.
    char *a, *b;
    char *left_bits = l->GetBits();
    char *lit_bits = literal->GetBits();
    int aInt, bInt, tempRes;
    double aDouble, bDouble;

    // comparing first value
    (c->operand1 == Left) ? (a = left_bits + ((int *)left_bits)[c->whichAtt1 + 1]) : (a = lit_bits + ((int *)lit_bits)[c->whichAtt1 + 1]);
    // comparing second value
    (c->operand2 == Left) ? (b = left_bits + ((int *)left_bits)[c->whichAtt2 + 1]) : (b = lit_bits + ((int *)lit_bits)[c->whichAtt2 + 1]);
   
    // now check the type and the comparison operation
    switch (c->attType)
    {
    // all the comparison cases with int data type
    case Int:

        aInt = *((int *)a);
        bInt = *((int *)b);
        switch (c->op)
        {
        case Equals:
            return (aInt == bInt);
            break;
        case GreaterThan:
            return (aInt > bInt);
            break;
        case LessThan:
            return (aInt < bInt);
            break;
        }
        break;
    //similarly with double
    case Double:
        aDouble = *((double *)a);
        bDouble = *((double *)b);
        switch (c->op)
        {
        case Equals:
            return (aDouble == bDouble);
            break;
        case GreaterThan:
            return (aDouble > bDouble);
            break;
        case LessThan:
            return (aDouble < bDouble);
            break;
        }
        break;

    // now default case with strings
    default:
        tempRes = strcmp(a, b);
        switch (c->op)
        {
        case Equals:
            return tempRes == 0;
            break;
        case GreaterThan:
            return tempRes > 0;
            break;
        case LessThan:
            return tempRes < 0;
            break;
        }
        break;
    }
}

void DBFileSort::sortWrite()
{
    // If the file is in read mode a new instance of BigQ is created
    // else the data is added to the existing BigQ file
    boundCalc = 0;
    if (isWriting == 0)
    {
        BigQWorker *workerArg = new BigQWorker;
        workerArg->in = in;
        workerArg->out = out;
        workerArg->sortorder = orderMaker;
        workerArg->runlen = size;
        thread = new pthread_t();
        pthread_create(thread, NULL, bigQWorkerMain, (void *)workerArg);
        isWriting = 1;
    }
}

void DBFileSort::sortRead()
{
    // if writing is still going on it call the merge function to merge the sorted file with records from the BigQ output pipe
    //  and create a temporary file with differences
    if (isWriting == 1)
    {
        Record rec1;
        Record rec2;

        boundCalc = 0;
        isWriting = 0;
        ComparisonEngine ce;

        in->ShutDown();
        if (thread != nullptr)
        {
            pthread_join(*thread, NULL);
            delete thread;
        }
        char *f_merge = "temporaryFileAfterMerging.bin";
        char *f_dif = "temporaryFilewithDifference.bin";

        // file after merging
        DBFile temporaryMergedFile;
        temporaryMergedFile.Create(f_merge, heap, nullptr);

        // file with difference
        DBFile temporaryFileWithDifference;
        temporaryFileWithDifference.Open(f_dif);

        this->MoveFirst();
        int m = temporaryFileWithDifference.GetNext(rec1);
        int n = this->GetNext(rec2);
        while (m && n)
        {
            (ce.Compare(&rec1, &rec2, orderMaker) < 0) ? (temporaryMergedFile.Add(rec1),
                                                          m = temporaryFileWithDifference.GetNext(rec1))
                                                       : (temporaryMergedFile.Add(rec2),
                                                          n = this->GetNext(rec2));
        }
        while (m)
        {
            temporaryMergedFile.Add(rec1);
            m = temporaryFileWithDifference.GetNext(rec1);
        }
        while (n)
        {
            temporaryMergedFile.Add(rec2);
            n = this->GetNext(rec2);
        }
        // closing all the file
        temporaryFileWithDifference.Close();
        temporaryMergedFile.Close();
        fBuffer.Close();
        // removing the memory
        remove(f_dif);
        remove(outputPath);
        rename(f_merge, outputPath);
    }
}

int DBFileSort::Create(char *f_path, fType f_type, void *startup)
{
    // generates a bin file as well as a metadata file which is sorted and has the sort order
    fBuffer.Open(0, const_cast<char *>(f_path));
    outputPath = f_path;
    pIndex = 0;
    isWriting = 0;
    MoveFirst();
    return 1;
}

int DBFileSort::Open(char *f_path)
{
    // open the sorted bin file
    fBuffer.Open(1, const_cast<char *>(f_path));
    outputPath = f_path;
    pIndex = 0;
    isWriting = 0;
    MoveFirst();
    return 1;
}

int DBFileSort::Close()
{
    // calls the sort read function and close the bin file
    sortRead();
    pBuffer.EmptyItOut();
    if (out != nullptr)
    {
        delete out;
    }
    if (in != nullptr)
    {
        delete in;
    }
    fBuffer.Close();
    return 1;
}

void DBFileSort::Load(Schema &f_schema, char *loadpath)
{
    // loads the records from the table into the input pipe and pass it for the BigQ class
    ComparisonEngine ce;
    Record temp;
    FILE *tbl = fopen(loadpath, "r");
    while (temp.SuckNextRecord(&f_schema, tbl))
    {
        this->Add(temp);
    }
    fclose(tbl);
}

void DBFileSort::MoveFirst()
{
    // points to the first set of record in the sorted File using sort read function.
    sortRead();
    pIndex = 0;
    pBuffer.EmptyItOut();
    if (fBuffer.GetLength() > 0)
    {
        fBuffer.GetPage(&pBuffer, pIndex);
    }
}

void DBFileSort::Add(Record &rec)
{
    // add records to the sorted DBFile using sort write function
    sortWrite();
    in->Insert(&rec);
}

int DBFileSort::GetNext(Record &fetchme)
{
    // returns the record from the Sorted File instance using sort read function
    sortRead();
    if (pBuffer.GetFirst(&fetchme) == 0)
    {
        pIndex++;
        if (pIndex >= fBuffer.GetLength() - 1)
        {
            return 0;
        }
        pBuffer.EmptyItOut();
        fBuffer.GetPage(&pBuffer, pIndex);
        pBuffer.GetFirst(&fetchme);
    }
    return 1;
}

int DBFileSort::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    // If the record from the Sorted File matches the filter CNF using binary search, this method returns it.
    if (!boundCalc)
    {
        set<int> attributeSet;
        boundCalc = 1;
        for (int i = 0; i < orderMaker->numAtts; i++)
        {
            attributeSet.insert(orderMaker->whichAtts[i]);
        }
        int gl = 0, gh = fBuffer.GetLength() - 2;
        // binary search
        for (int i = 0; i < cnf.numAnds; i++)
        {
            for (int j = 0; j < cnf.orLens[i]; j++)
            {
                if (attributeSet.find(cnf.orList[i][j].whichAtt1) == attributeSet.end())
                    continue;
                if (cnf.orList[i][j].op == LessThan)
                {
                    int l = 0, r = fBuffer.GetLength() - 2;
                    Record rec;
                    while (l < r)
                    {
                        int mid = l + (r - l + 1) / 2;
                        fBuffer.GetPage(&pBuffer, mid);
                        pBuffer.GetFirst(&rec);
                        int res = Start(&rec, &literal, &cnf.orList[i][j]);
                        res != 0 ? l = mid : r = mid - 1;
                    }
                    gh = min(gh, r);
                }
                else if (cnf.orList[i][j].op == GreaterThan)
                {
                    int l = 0, r = fBuffer.GetLength() - 2;
                    Record rec;
                    while (l < r)
                    {
                        int mid = l + (r - l) / 2;
                        fBuffer.GetPage(&pBuffer, mid);
                        pBuffer.GetFirst(&rec);
                        int res = Start(&rec, &literal, &cnf.orList[i][j]);
                        res != 0 ? r = mid : l = mid + 1;
                    }
                    gl = max(gl, l);
                }
            }
        }
        gl = gl - 1;
        gl = max(0, gl);
        // updating the new lb and ub after binary search
        lb = gl;
        ub = gh;
        pIndex = gl;
    }

    ComparisonEngine ce;
    while (GetNext(fetchme))
    {
        if (pIndex > ub)
            return 0;
        if (ce.Compare(&fetchme, &literal, &cnf))
            return 1;
    }
    return 0;
}
