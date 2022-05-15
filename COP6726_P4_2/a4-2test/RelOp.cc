#include "RelOp.h"
#include <iostream>
#include "BigQ.h"

//Projection relational operator logic
void *ProjectWorker(void *arg)
{
    ProjectArg *projectArg = (ProjectArg *)arg;
    Record recP;
    while (projectArg->ip->Remove(&recP))
    {
        Record *tempRecord = new Record;
        tempRecord->Consume(&recP);
        tempRecord->Project(projectArg->keepMe, projectArg->numAttsOutput, projectArg->numAttsInput);
        projectArg->op->Insert(tempRecord);
    }
    projectArg->op->ShutDown();
    return NULL;
}
void Project::Run(Pipe &ip, Pipe &op, int *keepMe, int numAttsInput, int numAttsOutput)
{
    ProjectArg *projectArg = new ProjectArg;
    projectArg->ip = &ip;
    projectArg->op = &op;
    projectArg->numAttsInput = numAttsInput;
    projectArg->numAttsOutput = numAttsOutput;
    projectArg->keepMe = keepMe;
    pthread_create(&workerThread, NULL, ProjectWorker, (void *)projectArg);
}
void Project::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}
void Project::Use_n_Pages(int n)
{
}

//writeOut relational operator logic
void *WriteOutWorker(void *arg)
{
    WriteOutArg *writeOutArg = (WriteOutArg *)arg;
    Record current;
    while (writeOutArg->ip->Remove(&current) == 1)
    {
        int numOfAtts = writeOutArg->schema->GetNumAtts();
        Attribute *attribute = writeOutArg->schema->GetAtts();
        for (int i = 0; i < numOfAtts; i++)
        {
            fprintf(writeOutArg->outFile, "%s:", attribute[i].name);
            int pointer = ((int *)current.bits)[i + 1];
            fprintf(writeOutArg->outFile, "[");
            if (attribute[i].myType == Int)
            {
                int *writeOutInt = (int *)&(current.bits[pointer]);
                fprintf(writeOutArg->outFile, "%d", *writeOutInt);
            }
            else if (attribute[i].myType == Double)
            {
                double *writeOutDouble = (double *)&(current.bits[pointer]);
                fprintf(writeOutArg->outFile, "%f", *writeOutDouble);
            }
            else if (attribute[i].myType == String)
            {
                char *writeOutString = (char *)&(current.bits[pointer]);
                fprintf(writeOutArg->outFile, "%s", writeOutString);
            }
            fprintf(writeOutArg->outFile, "]");
            if (i != numOfAtts - 1)
                fprintf(writeOutArg->outFile, ", ");
        }
        fprintf(writeOutArg->outFile, "\n");
    }
    return NULL;
}
void WriteOut::Run(Pipe &ip, FILE *outFile, Schema &mySchema)
{
    WriteOutArg *writeOutArg = new WriteOutArg;
    writeOutArg->schema = &mySchema;
    writeOutArg->ip = &ip;
    writeOutArg->outFile = outFile;
    pthread_create(&workerThread, NULL, WriteOutWorker, (void *)writeOutArg);
}
void WriteOut::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}
void WriteOut::Use_n_Pages(int n)
{
}
 
typedef struct {
    DBFile &inFile;
    Pipe &op;
    CNF &selOp;
    Record &literal;
} WorkerArg1;
void* workerMain1(void* arg){
    Record rec;
    WorkerArg1* workerArg = (WorkerArg1*) arg;
    while(workerArg->inFile.GetNext(rec, workerArg->selOp, workerArg->literal)){
        workerArg->op.Insert(&rec);
    }
    workerArg->op.ShutDown();
    return NULL;
}
void SelectFile::Run (DBFile &inFile, Pipe &op, CNF &selOp, Record &literal) {
    WorkerArg1* workerArg = new WorkerArg1{inFile, op, selOp, literal};
    pthread_create(&worker, NULL, workerMain1, (void*) workerArg);
}
void SelectFile::WaitUntilDone () {
	pthread_join (worker, NULL);
}
void SelectFile::Use_n_Pages (int runlen) {

}

//initializing structure for Select Pipe relational operator.
typedef struct {
    Pipe &ip;
    Pipe &op;
    CNF &selOp;
    Record &literal;
} WorkerArg2;
void* workerMain2(void*arg){
    ComparisonEngine comp;
    WorkerArg2* workerArg = (WorkerArg2*) arg;
    Record rec;
    while(workerArg->ip.Remove(&rec)){
        if(comp.Compare(&rec, &workerArg->literal, &workerArg->selOp)){
            workerArg->op.Insert(&rec);
        }
    }
    workerArg->op.ShutDown();
    return NULL;
}
void SelectPipe::Run (Pipe &ip, Pipe &op, CNF &selOp, Record &literal) {
    WorkerArg2* workerArg = new WorkerArg2{ip, op, selOp, literal};
    pthread_create(&worker, NULL, workerMain2, (void*) workerArg);
}
void SelectPipe::WaitUntilDone () {
	pthread_join (worker, NULL);
}
void SelectPipe::Use_n_Pages (int runlen) {

}


// //initializing structure for Sum relational operator.
typedef struct {
    Pipe &ip;
    Pipe &op;
    Function &computeMe;
} WorkerArg3;
void* workerMain3(void*arg){
    ComparisonEngine comp;
    Record rec;
    Type t;
    WorkerArg3* workerArg = (WorkerArg3*) arg;
    
    int intSum = 0, intResult = 0;
    double doubleSum = 0.0, doubleResult = 0.0;
    
    while(workerArg->ip.Remove(&rec)){
        t = workerArg->computeMe.Apply(rec, intResult, doubleResult);
        t == Int ? intSum += intResult : doubleSum += doubleResult;
    }

    Attribute DA = {"SUM", t};
    Schema out_sch ("out_sch", 1, &DA);
    Record res;
    char charsRes[100];
    t == Int ?
        sprintf(charsRes, "%d|", intSum):
        sprintf(charsRes, "%lf|", doubleSum);

    res.ComposeRecord(&out_sch, charsRes);
    workerArg->op.Insert(&res);
    workerArg->op.ShutDown();
    return NULL;
}
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    WorkerArg3* workerArg = new WorkerArg3{inPipe, outPipe, computeMe};
    pthread_create(&worker, NULL, workerMain3, (void*) workerArg);
}
void Sum::WaitUntilDone (){
    pthread_join(worker, NULL);
}
void Sum::Use_n_Pages (int n){

}



// //initializing structure for Groupby relational operator.
typedef struct {
    Pipe &ip;
    Pipe &op;
    OrderMaker &groupAtts;
    Function &computeMe;
    int use_n_pages;
} WorkerArg4;
void* workerMain4(void*arg){
    ComparisonEngine cmp;
    WorkerArg4* workerArg = (WorkerArg4*) arg;
    Record prev;
    Record cur;
    Type t;

    int intRes = 0, intSum = 0;
    double doubleRes = 0.0, doubleSum = 0.0;
    bool firstTime = true;
    
    Pipe sorted(100);
    BigQ bigQ(workerArg->ip, sorted, workerArg->groupAtts, workerArg->use_n_pages);
    
    Attribute DA = {"SUM", t};
    Schema out_sch ("out_sch", 1, &DA);
    while(sorted.Remove(&cur)){
        if(!firstTime && cmp.Compare(&cur, &prev, &workerArg->groupAtts)!=0){
            cout<<"==="<<endl;
            Record res;
            char charsRes[100];
            t == Int ? 
                sprintf(charsRes, "%d|", intSum):
                sprintf(charsRes, "%lf|", doubleSum);
            
            res.ComposeRecord(&out_sch, charsRes);
            workerArg->op.Insert(&res);

            intSum = 0;
            doubleSum = 0.0;
        }
        firstTime = false;
        t = workerArg->computeMe.Apply(cur, intRes, doubleRes);
        t == Int ? intSum += intRes : doubleSum += doubleRes;
        prev.Copy(&cur);
    }
    Record res;
    char charsRes[100];
    t == Int ?
        sprintf(charsRes, "%d|", intSum):
        sprintf(charsRes, "%lf|", doubleSum);
    
    res.ComposeRecord(&out_sch, charsRes);
    workerArg->op.Insert(&res);
    workerArg->op.ShutDown();
    return NULL;
}
void GroupBy::Run (Pipe &ip, Pipe &op, OrderMaker &groupAtts, Function &computeMe){
    WorkerArg4* workerArg = new WorkerArg4{ip, op, groupAtts, computeMe, use_n_pages};
    pthread_create(&worker, NULL, workerMain4, (void*) workerArg);
}
void GroupBy::WaitUntilDone (){
    pthread_join(worker, NULL);
}
void GroupBy::Use_n_Pages (int n){
    use_n_pages = n;
}



//join relational operator logic
void Join::Run(Pipe &ipL, Pipe &ipR, Pipe &op, CNF &selOp, Record &literal)
{
    JoinArg *joinArgument = new JoinArg;
    joinArgument->op = &op;
    joinArgument->selOp = &selOp;
    joinArgument->literal = &literal;
    joinArgument->ipL = &ipL;
    joinArgument->ipR = &ipR;

    this->runLen <= 0 ? joinArgument->runLen = 8 : joinArgument->runLen = this->runLen;
    pthread_create(&workerThread, NULL, JoinWorker, (void *)joinArgument);
}
void *JoinWorker(void *arg)
{
    JoinArg *joinArgument = (JoinArg *)arg;
    OrderMaker leftOrder, rightOrder;
    joinArgument->selOp->GetSortOrders(leftOrder, rightOrder);
    (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) ? JoinWorker_Merge(joinArgument, &leftOrder, &rightOrder) : JoinWorker_BlockNested(joinArgument);
    joinArgument->op->ShutDown();
    return NULL;
}
// It is used for merging record
void JoinWorker_AddMergedRecord(Record *leftRecord, Record *rightRecord, Pipe *pipe)
{
    Record mergeRec;
    int numAttributeL = ((((int *)leftRecord->bits)[1]) / sizeof(int)) - 1;
    int numAttributeR = ((((int *)rightRecord->bits)[1]) / sizeof(int)) - 1;
    int *attsToKeep = new int[numAttributeL + numAttributeR];
    for (int i = 0; i < numAttributeL; i++)
        attsToKeep[i] = i;
    for (int i = numAttributeL; i < numAttributeL + numAttributeR; i++)
        attsToKeep[i] = i - numAttributeL;
    mergeRec.MergeRecords(leftRecord, rightRecord, numAttributeL, numAttributeR, attsToKeep, numAttributeL + numAttributeR, numAttributeL);
    pipe->Insert(&mergeRec);
}
// Merge sorting for join operation
void JoinWorker_Merge(JoinArg *joinArg, OrderMaker *leftOrder, OrderMaker *rightOrder)
{
    bool isFinish = false;
    ComparisonEngine ce;
    Record leftRec;
    Record rightRec;
    
    Pipe *sortedLp = new Pipe(1000);
    Pipe *sortedRp = new Pipe(1000);
    
    BigQ *tempL = new BigQ(*(joinArg->ipL), *sortedLp, *leftOrder, joinArg->runLen);
    BigQ *tempR = new BigQ(*(joinArg->ipR), *sortedRp, *rightOrder, joinArg->runLen);
    
    if (sortedRp->Remove(&rightRec) == 0)
        isFinish = true;
    if (sortedLp->Remove(&leftRec) == 0)
        isFinish = true;

    while (!isFinish)
    {
        int compareRed = ce.Compare(&leftRec, leftOrder, &rightRec, rightOrder);
        if (compareRed == 0)
        {
            vector<Record *> vectorLeft;
            vector<Record *> vectorRight;
            while (true)
            {
                Record *oldLeftRecord = new Record;
                oldLeftRecord->Consume(&leftRec);
                vectorLeft.push_back(oldLeftRecord);
                if (sortedLp->Remove(&leftRec) == 0)
                {
                    isFinish = true;
                    break;
                }
                if (ce.Compare(&leftRec, oldLeftRecord, leftOrder) != 0)
                {
                    break;
                }
            }
            while (true)
            {
                Record *oldRightRecord = new Record;
                oldRightRecord->Consume(&rightRec);
                vectorRight.push_back(oldRightRecord);
                if (sortedRp->Remove(&rightRec) == 0)
                {
                    isFinish = true;
                    break;
                }
                if (ce.Compare(&rightRec, oldRightRecord, rightOrder) != 0)
                {
                    break;
                }
            }
            for (int i = 0; i < vectorLeft.size(); i++)
            {
                for (int j = 0; j < vectorRight.size(); j++)
                {
                    JoinWorker_AddMergedRecord(vectorLeft[i], vectorRight[j], joinArg->op);
                }
            }
            vectorLeft.clear();
            vectorRight.clear();
        }
        else if (compareRed > 0)
        {
            if (sortedRp->Remove(&rightRec) == 0)
                isFinish = true;
        }
        else
        {
            if (sortedLp->Remove(&leftRec) == 0)
                isFinish = true;
        }
    }
    cout << "Completed scanning from sorted pipe" << endl;
    while (sortedLp->Remove(&leftRec))
        ;
    while (sortedRp->Remove(&rightRec))
        ;
}
// default method for join : nested join
void JoinWorker_BlockNested(JoinArg *joinArg)
{
    DBFile tempFile;
    ComparisonEngine ce;
    Record record;
    Record recOrd1, recOrd2;
    
    char *fileName = new char[100];
    sprintf(fileName, "BlockNestedTemp%d.bin", pthread_self());

    tempFile.Create(fileName, heap, NULL);
    tempFile.Open(fileName);

    while (joinArg->ipL->Remove(&record))
        tempFile.Add(record);

    while (joinArg->ipR->Remove(&recOrd1))
    {
        tempFile.MoveFirst();
        while (tempFile.GetNext(record))
        {
            if (ce.Compare(&recOrd1, &recOrd2, joinArg->literal, joinArg->selOp))
            {
                JoinWorker_AddMergedRecord(&recOrd1, &recOrd2, joinArg->op);
            }
        }
    }
}
void Join::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}
void Join::Use_n_Pages(int n)
{
    this->runLen = n;
}

//Duplicate removal relational operator logic
void *DuplicateRemovalWorker(void *arg)
{
    Pipe *sortedPipe = new Pipe(1000);
    ComparisonEngine ce;
    Record current, last;

    DuplicateRemovalArg *duplicateRemovalArg = (DuplicateRemovalArg *)arg;
    BigQ *bq = new BigQ(*(duplicateRemovalArg->ip), *sortedPipe, *(duplicateRemovalArg->order), duplicateRemovalArg->runLen);
    
    sortedPipe->Remove(&last);
    Schema schema("catalog", "partsupp");
    while (sortedPipe->Remove(&current))
    {
        if (ce.Compare(&last, &current, duplicateRemovalArg->order) != 0)
        {
            Record *tempRecord = new Record;
            tempRecord->Consume(&last);
            duplicateRemovalArg->op->Insert(tempRecord);
            last.Consume(&current);
        }
    }
    duplicateRemovalArg->op->Insert(&last);
    duplicateRemovalArg->op->ShutDown();
    return NULL;
}
void DuplicateRemoval::Run(Pipe &ip, Pipe &op, Schema &mySchema)
{
    DuplicateRemovalArg *duplicateRemovalArg = new DuplicateRemovalArg;
    OrderMaker *order = new OrderMaker(&mySchema);
    duplicateRemovalArg->ip = &ip;
    duplicateRemovalArg->op = &op;
    duplicateRemovalArg->order = order;
    this->runLen <= 0 ? duplicateRemovalArg->runLen = 8 : duplicateRemovalArg->runLen = this->runLen;
    pthread_create(&workerThread, NULL, DuplicateRemovalWorker, (void *)duplicateRemovalArg);
}
void DuplicateRemoval::WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}
void DuplicateRemoval::Use_n_Pages(int n)
{
    this->runLen = n;
}



