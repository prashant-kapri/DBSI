
#include "DBFile.h"
#include "Heap.h"
#include "Sorted.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Tree.h"
#include "Defs.h"
#include "Schema.h"
#include "File.h"
#include <iostream>
#include <fstream>
#include "Comparison.h"
#include "ComparisonEngine.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    // create a meta file based on the file type
    char metaFileName[100];
    sprintf (metaFileName, "%s.meta", f_path);
    ofstream metaFile;
    metaFile.open(metaFileName);
    OrderMaker* orderMaker = nullptr;
    int size = 0;
    if(f_type==sorted){
        metaFile << "sorted"<< endl;
        x = new DBFileSort();
    }
    else if(f_type == heap){
        metaFile << "heap" << endl;
        x = new DBFileHeap();
    }
    else if(f_type==tree){
        metaFile << "tree"<< endl;
        x = new DBFileTree();
    }
    if(startup!= nullptr) {
        SortInfo* sortInfo = ((SortInfo*)startup);
        size = sortInfo->size;
        metaFile << size << endl;
        orderMaker = sortInfo->order;
        metaFile << orderMaker->numAtts << endl;
        for (int i = 0; i < orderMaker->numAtts; i++) {
            metaFile << orderMaker->whichAtts[i] << endl;
            if(orderMaker->whichTypes[i] == String)
                metaFile << "String" << endl;
            else if (orderMaker->whichTypes[i] == Double)
                metaFile << "Double" << endl;
            else if (orderMaker->whichTypes[i] == Int)
                metaFile << "Int" << endl;

        }
        if(f_type==sorted){
            ((DBFileSort*)x)->orderMaker = orderMaker;
            ((DBFileSort*)x)->size = size;
        }
        else if(f_type==tree || f_type == heap) {

        }
    }
    metaFile.close();
    int result = x->Create(f_path, f_type, startup);
    return result; 
}

int DBFile::Open (char *f_path) {
    // open the db file and compare the attribute data type.
    OrderMaker* orderMaker = new OrderMaker();
    string s;
    char metaFileName[100];
    
    sprintf (metaFileName, "%s.meta", f_path);
    ifstream metaFile(metaFileName);
    getline(metaFile, s);

    if(s.compare("sorted")==0){
        x = new DBFileSort();
        string temporary;

        getline(metaFile, temporary);
        int size = stoi(temporary);
        temporary.clear();

        getline(metaFile, temporary);
        orderMaker->numAtts = stoi(temporary);

        for(int i=0; i<orderMaker->numAtts; i++){
            temporary.clear();

            getline(metaFile, temporary);
            orderMaker->whichAtts[i] = stoi(temporary);
            temporary.clear();

            getline(metaFile, temporary);
            if(temporary.compare("String")==0){
                orderMaker->whichTypes[i] = String;
            }
            else if(temporary.compare("Double")==0){
                orderMaker->whichTypes[i] = Double;
            }
            else if(temporary.compare("Int")==0){
                orderMaker->whichTypes[i] = Int;
            }
              
        }
        ((DBFileSort*)x)->orderMaker = orderMaker;
        ((DBFileSort*)x)->size = size;
        orderMaker->Print();
    }
    else if(s.compare("heap")==0){
        x = new DBFileHeap();
    }
    else if(s.compare("tree")==0){
        x = new DBFileTree();
    }
    metaFile.close();

    int result = x->Open(f_path);
    return result; 
}

int DBFile::Close () {
    int result = x->Close();
    delete x; // free memory
    return result; // close the file and free internal variable
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    x->Load(f_schema, loadpath); //load data to internal variable
}

void DBFile::MoveFirst () {
    x->MoveFirst(); // point to first data record
}

void DBFile::Add (Record &rec) {
    x->Add(rec); // add record to internal variable
}

int DBFile::GetNext (Record &fetchme) {
    return x->GetNext(fetchme); //return the next record
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return x->GetNext(fetchme, cnf, literal); //if the record match the cnf after comparision return it the the internal variable
}
