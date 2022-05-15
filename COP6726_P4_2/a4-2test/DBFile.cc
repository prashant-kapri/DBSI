#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Heap.h"
#include "Sorted.h"

// stub file .. replace it with your own DBFile.cc

/* Constructor */
DBFile::DBFile(){
	this->x = NULL;
}

int DBFile::Create(char *f_path, fType f_type, void *startup){
    // create a meta file based on the file type
	if (f_type == heap)
	{
		x = new Heap();
	}
	else if (f_type == sorted)
	{
		x = new SortedFile();
	}
	return x->Create(f_path, f_type, startup);
}

int DBFile::Open(char *f_path){
    // open the db file and compare the attribute data type.

	char metadata[100];
	strcpy(metadata, f_path);
	strcat(metadata, "-metadata.header");
	ifstream fileInputStream;
	fileInputStream.open(metadata);
	string line;
	getline(fileInputStream, line);
	if (line == "heap")
	{
		x = new Heap();
		return x->Open(f_path);
	}
	else if (line == "sorted")
	{
		getline(fileInputStream, line);
		int runLength = stoi(line);
		OrderMaker *sortOrder = new OrderMaker(fileInputStream);
		x = new SortedFile(sortOrder, runLength);
		return x->Open(f_path);
	}
	fileInputStream.close();
	return 0;
}

int DBFile::Close(){
	x->Close();
    delete x; // free memory

}

void DBFile::Load(Schema &f_schema, char *loadpath){
	x->Load(f_schema, loadpath); //load data to internal variable
}


void DBFile::MoveFirst(){
	x->MoveFirst();// point to first data record
}



void DBFile::Add(Record &rec){
	x->Add(rec);// add record to internal variable
}

int DBFile::GetNext(Record &fetchme){
	return x->GetNext(fetchme);//return the next record
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){
    return x->GetNext(fetchme, cnf, literal); //if the record match the cnf after comparision return it the the internal variable
}
