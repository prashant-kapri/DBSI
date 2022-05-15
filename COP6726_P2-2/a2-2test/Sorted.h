#ifndef SORTED_H
#define SORTED_H

#include "BigQ.h"
#include "DBFile.h"
#include <queue>

class DBFileSort : public DBFileGeneric
{
    friend class DBFile;

private:
    File fBuffer; //file buffer
    int lb; //lowerbound
    int ub; // upperbound
    int boundCalc = 0; // bound calculation
    int size; // size of the run length
    int isWriting; //to keep track of read mode and write mode

    char *outputPath = nullptr;

    Page pBuffer;
    off_t pIndex;

    Pipe *in = new Pipe(100);
    Pipe *out = new Pipe(100);
    
    pthread_t *thread = nullptr;
    OrderMaker *orderMaker = nullptr;


    static void *consumer(void *arg);
    int Start(Record *left, Record *literal, Comparison *c);

    void sortWrite();
    void sortRead();

public:
    DBFileSort();

    int Create(char *fpath, fType f_type, void *startup);
    int Open(char *fpath);
    int Close();
    void Load(Schema &myschema, char *loadpath);
    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif // SORTED_H
