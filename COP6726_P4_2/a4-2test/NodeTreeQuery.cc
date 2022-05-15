#ifndef QUERY_TREE_NODE_CC
#define QUERY_TREE_NODE_CC

#include "NodeTreeQuery.h"

NodeTreeQuery::NodeTreeQuery() {

    db = NULL;
    parent = NULL;
    left = NULL;
    right = NULL;

    sf = NULL;
    sp = NULL;
    p = NULL;
    j = NULL;
    s = NULL;
    gb = NULL;
    dr = NULL;
    wo = NULL;

    schema = NULL;
    oMaker = NULL;
    operatorFunc = NULL;
    function = NULL;    
    cnf = NULL;
    operatorForCNF = NULL;

    attributesToKeep = NULL;
    numberOfAttributesToKeep = NULL;
    leftPipeId = 0;
    rightPipeId = 0;
    opPipeId = 0;
}

NodeTreeQuery::~NodeTreeQuery(){}
void NodeTreeQuery::Run(){
    switch(typeOfNode) {
        case SELECTFILE:
        case SELECTPIPE:
        case PROJECT:
        case JOIN:
        case SUM:
        case GROUPBY:
        case DISTINCT:
        case WRITEOUT:
            break;

    }

}


void NodeTreeQuery::printNode() {
    cout<<" *********** "<<endl;
    cout<< getTypeName()<<" operation"<<endl;
    switch (typeOfNode) {
        case SELECTFILE:
            //Select Operation
            cout<<"Input Pipe "<<leftPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            printCNF();
            break;
        
        case SELECTPIPE:
            //Pipe Operation
            cout<<"Input Pipe "<<leftPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<"SELECTION CNF :"<<endl;
            printCNF();
            break;
        
        case PROJECT:
            //Project Operation
            cout<<"Input Pipe "<<leftPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"************"<<endl;
            break;
        
        case JOIN:
            //Join Operation
            cout<<"Left Input Pipe "<<leftPipeId<<endl;
            cout<<"Right Input Pipe "<<rightPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"CNF: "<<endl;
            printCNF();
            break;
        
        case SUM:
            //Sum Operation
            cout<<"Left Input Pipe "<<leftPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"FUNCTION: "<<endl;
            printFunction();
            break;
        
        case DISTINCT:
            //Distinct Operation
            cout<<"Left Input Pipe "<<leftPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            break;
        
        case GROUPBY:
            //GroupBy Operation
            cout<<"Left Input Pipe "<<leftPipeId<<endl;
            cout<<"Output Pipe "<<opPipeId<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"GROUPING ON: "<<endl;
            oMaker->Print();
            cout<<endl<<"FUNCTION: "<<endl;
            printFunction();
            break;
        
        case WRITEOUT:
            //Write-Out Operation
            cout<<"Left Input Pipe "<<leftPipeId<<endl;
            cout<<"Output File "<<fPath<<endl;
            break;
    }

}

void NodeTreeQuery::WaitUntilDone() {

    switch(typeOfNode) {
        
        case SELECTFILE:
            sf->WaitUntilDone();
            break;
        case SELECTPIPE:
            sp->WaitUntilDone();
            break;
        case PROJECT:
        case JOIN:
        case SUM:
        case GROUPBY:
        case DISTINCT:
        case WRITEOUT:
            break;

    }

}



void NodeTreeQuery::genFunc(){
    function = new Function();
    function->GrowFromParseTree(operatorFunc, *schema);
}


void NodeTreeQuery::printFunction(){
    function->Print();
}


void NodeTreeQuery::genSchema() {
    Schema *rightSchema = right->schema;
    Schema *leftSchema = left->schema;
    schema = new Schema(leftSchema, rightSchema);
}

void NodeTreeQuery::printCNF(){

    if(cnf) {
        struct OrList *currOr;
        struct ComparisonOp *currentOp;
        struct AndList *currAnd = cnf;

        while(currAnd) {
            currOr = currAnd->left;
            if(currAnd->left) {
                cout<<"(";
            }
            while(currOr) {
                currentOp = currOr->left;
                if(currentOp) {
                    if(currentOp->left) {
                        cout<<currentOp->left->value;
                    }
                    switch(currentOp->code) {
                        case 5:
                            cout<<" < ";
                            break;
                        case 6:
                            cout<<" > ";
                            break;
                        case 7:
                            cout<<" = ";
                            break;
                    }
                    if(currentOp->right) {
                        cout<<currentOp->right->value;
                    }
                }
                if(currOr->rightOr) {
                    cout<<" OR ";
                }
                currOr = currOr->rightOr;
            }
            if(currAnd->left) {
                cout<<")";
            }
            if(currAnd->rightAnd) {
                cout<<" AND ";
            }
            currAnd = currAnd->rightAnd;
        }
    }
    cout<<endl;
}

void NodeTreeQuery::genOrderMaker(int numberOfAttributes, vector<int> attributes, vector<int> types) {
    int i = 0;
    oMaker = new OrderMaker();
    oMaker->numAtts = numberOfAttributes;
    while(i<attributes.size()){
        oMaker->whichAtts[i] = attributes[i];
        oMaker->whichTypes[i] = (Type)types[i];
        i++;
    }
}

void NodeTreeQuery::printInOrder(){
    if(left != NULL)
        left->printInOrder();
    if(right != NULL)
        right->printInOrder();
    printNode();
}

string NodeTreeQuery::getTypeName() {
    string typeName;
    switch(typeOfNode) {
        
        case SELECTFILE:
            typeName = "SELECT FILE";
            break;
        case SELECTPIPE:
            typeName = "SELECT PIPE";
            break;
        case PROJECT:
            typeName = "PROJECT";
            break;
        case JOIN:
            typeName = "JOIN";
            break;
        case SUM:
            typeName = "SUM";
            break;
        case GROUPBY:
            typeName = "GROUP BY";
            break;
        case DISTINCT:
            typeName = "DISTINCT";
            break;
        case WRITEOUT:
            typeName = "WRITE";
            break;  
        
    }
    return typeName;

}

void NodeTreeQuery::setNodeTreeQueryType(NodeTreeQueryType type) {
    typeOfNode = type;
}

NodeTreeQueryType NodeTreeQuery::getNodeTreeQueryType() {
    return typeOfNode;
}


#endif