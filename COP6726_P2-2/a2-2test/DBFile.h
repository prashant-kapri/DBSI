#ifndef DBFILE_H
#define DBFILE_H

#include "DBFileGeneric.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef struct
{
	OrderMaker *order;
	int size;
} SortInfo;

typedef enum
{
	heap,
	sorted,
	tree
} fType;

class DBFileGeneric
{
public:
	virtual int Create(char *fpath, fType f_type, void *startup) = 0;
	virtual int Open(char *fpath) = 0;
	virtual int Close() = 0;
	virtual void Load(Schema &myschema, char *loadpath) = 0;
	virtual void MoveFirst() = 0;
	virtual void Add(Record &addme) = 0;
	virtual int GetNext(Record &fetchme) = 0;
	virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class DBFile
{
private:
	DBFileGeneric *x;

public:
	DBFile();

	int Create(char *fpath, fType file_type, void *startup);
	int Open(char *fpath);
	int Close();
	void Load(Schema &myschema, char *loadpath);
	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};
#endif
