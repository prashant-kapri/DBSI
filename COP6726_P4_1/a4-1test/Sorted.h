#ifndef A2_2TEST_DBFILESORT_H
#define A2_2TEST_DBFILESORT_H
#include "DBFile.h"
#include <queue>
#include "BigQ.h"


class DBFileSort :public DBFileGeneric{
    friend class DBFile;
private:
    File fBuffer;
    Page pBuffer;
    off_t pIndex;
    int isWriting;
    char* outputPath = nullptr;
    int boundCalc = 0;
    int lb;
    int ub;

    Pipe* in = new Pipe(100);
    Pipe* out = new Pipe(100);
    pthread_t* thread = nullptr;

    OrderMaker* orderMaker = nullptr;
    int size;

    void sortWrite();
    void sortRead();

    static void *consumer (void *arg);
    int Start (Record *left, Record *literal, Comparison *c);
public:
    DBFileSort ();

    int Create (char *fpath, fType f_type, void *startup);
    int Open (char *fpath);
    int Close ();

    void Load (Schema &myschema, char *loadpath);

    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};


#endif 