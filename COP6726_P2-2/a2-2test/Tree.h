#ifndef TREE_H
#define TREE_H
#include "DBFile.h"

class DBFileTree :public DBFileGeneric{
public:
    DBFileTree ();

    int Create (char *fpath, fType f_type, void *startup);
    int Open (char *fpath);
    int Close ();

    void Load (Schema &myschema, char *loadpath);

    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};


#endif //TREE_H
