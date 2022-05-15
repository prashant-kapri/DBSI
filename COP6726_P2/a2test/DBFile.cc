#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "string.h"
#include "fstream"
#include<stdio.h>

// stub file .. replace it with your own DBFile.cc

/*Constructor*/
DBFile::DBFile () {
    this->fBuffer = new File();  
    this->pIndex = 0;         
    this->eof = false;            
    this->rBuffer = new Page(); 
    this->wBuffer = new Page(); 
    this->isEmptywBuffer = true;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char data[100];
    this->isEmptywBuffer = true;
    ofstream streamOutputFile;
    strcpy( data , f_path);
    strcat( data , ".header");
    streamOutputFile.open(data);
    if ( f_type == heap ){
        streamOutputFile;
    }
    fBuffer->Open(0, (char *)f_path);  
    streamOutputFile.close();
    return 1; //The return value from Create is a 1 on success.
}


/*load the DBFile instance .tbl files using suchNextRecord from record.h */
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE  *streamInputFile = fopen(loadpath , "r");
    Record temp;
    ComparisonEngine comp;
    while( temp.SuckNextRecord( &f_schema , streamInputFile ) == 1 ){
        this->Add(temp);
    }
    fclose(streamInputFile);
}

/*Assumes Heap file exists and DBFile instance points to the Heap file */
int DBFile::Open (const char *f_path) {
    this->fBuffer->Open(1, (char *)f_path);
    this->eof = false;
    this->pIndex = 0;
    return 1; //return 1 on success
}

/*Method points to the first record in the file*/
void DBFile::MoveFirst () {
    this->fBuffer->GetPage(this->rBuffer, 0);
}

/*Closes the file */
int DBFile::Close () {
    //if its still writing then wait for it to finish
    if( this->isEmptywBuffer == false ){
        if ((*fBuffer).GetLength() == 0){
        this->fBuffer->AddPage(wBuffer, 0);
        }
        else {
        this->fBuffer->AddPage(wBuffer, ((*fBuffer).GetLength()) - 1 );
        }
        wBuffer->EmptyItOut();
    }
    this->fBuffer->Close();
    delete fBuffer;
    delete wBuffer;
    delete rBuffer;
    isEmptywBuffer = true;
    return 1; // returns 1 on successful file closed
}

// /*add new record to end of file*/
void DBFile::Add (Record &rec) {
    Record write;
    write.Consume(&rec);
    this->isEmptywBuffer = false;
    if(  wBuffer->Append(&write) == 0 ){
        if((*fBuffer).GetLength() == 0 ){
            fBuffer->AddPage(wBuffer , 0);
        }
        else{
            fBuffer->AddPage(wBuffer , ((*fBuffer).GetLength())-1); 
        }
        wBuffer->EmptyItOut();
        wBuffer->Append(&write);
    }
}




/* gets the next record present in heap file and returns */
int DBFile::GetNext (Record &fetchme) {
    if( this->eof == false ){
        int result = this->rBuffer->GetFirst(&fetchme);
        if( result == 0 ){
            pIndex++;
            if( pIndex >= this->fBuffer->GetLength()-1 ){
                this->eof = true;
                return 0; // return 0 as end of file has reached
            }else{
                this->fBuffer->GetPage(this->rBuffer,pIndex);
                this->rBuffer->GetFirst(&fetchme);
            }
        }
        return 1; // return 1 on success, next record is fed into fetchme
    }
    return 0; // return 0 as end of file has reached
}

/*gets the next record from the Heap file if the accepted by the predicate*/
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while( GetNext(fetchme) ){
        if( comp.Compare(&fetchme,&literal,&cnf) )
            return 1;  // record accepted by the predicate
    }
    return 0; // eof reached
}
