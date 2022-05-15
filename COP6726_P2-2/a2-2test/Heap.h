#ifndef HEAP_H
#define HEAP_H
#include "DBFile.h"

class DBFileHeap : public DBFileGeneric
{
private:
    File *fBuffer;       // instance of file
    int pIndex;          // page index
    bool eof;            // end of file index
    Page *rBuffer;       // read buffer
    Page *wBuffer;       // write buffer
    bool isEmptywBuffer; // write buffer empty condition

public:
    DBFileHeap();

    int Create(char *fpath, fType f_type, void *startup);
    int Open(char *fpath);
    int Close();

    void Load(Schema &myschema, char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif // HEAP_H
