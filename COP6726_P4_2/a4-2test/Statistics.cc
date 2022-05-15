#include "Statistics.h"
#include <set>

Statistics::Statistics()
{
    table["n"] = "nation";
    table["r"] = "region";
    table["p"] = "part";
    table["c"] = "customer";
    table["ps"] = "partsupp";
    table["s"] = "supplier";
    table["l"] = "lineitem";
    table["o"] = "orders";
}
Statistics &Statistics::operator=(Statistics &atempignMe)
{

    if (&atempignMe != this)
    {
        relationList = atempignMe.relationList;
        relationSets = atempignMe.relationSets;
        table = atempignMe.table;
    }
    return *this;
}
Statistics::Statistics(Statistics &copyMe)
{
    this->relationSets = copyMe.relationSets;
    this->relationList = copyMe.relationList;
    this->table = copyMe.table;
}

Statistics::~Statistics()
{
}

void Statistics::Read(char *fromWhere)
{
    ifstream fRead(fromWhere);

    if (fRead.is_open())
    {
        int noSets = 0, noStats = 0;
        string ipLine;
        stringstream temp;
        string relationStr;

        if (fRead.good())
        {
            getline(fRead, ipLine);
            temp.str(ipLine);
            if (!(temp >> noStats))
            {
                noStats = 0;
            }
            temp.clear();
            temp.str("");
            int i = 0, k = 0;
            while (i < noStats)
            {
                RelStats tempStats;
                getline(fRead, ipLine);
                if (ipLine.compare("") == 0)
                {
                    getline(fRead, ipLine);
                }
                relationStr = ipLine;
                fRead >> tempStats;
                relationList[relationStr] = tempStats;
                i++;
            }
            getline(fRead, ipLine);
            while (ipLine.compare("#####") != 0)
            {
                getline(fRead, ipLine);
            }
            getline(fRead, ipLine);
            getline(fRead, ipLine);
            temp.str(ipLine);
            if (!(temp >> noSets))
            {
                noSets = 0;
            }
            getline(fRead, ipLine);
            while (k < noSets)
            {
                RelStatSet tempRelSet;
                vector<string> setRelVec;

                fRead >> tempRelSet;
                tempRelSet.getRel(setRelVec);
                setsList.push_back(tempRelSet);
                vector<string>::iterator it = setRelVec.begin();
                while (it != setRelVec.end())
                {
                    relationSets[*it] = tempRelSet;
                    it++;
                }
                k++;
            }
        }
        fRead.close();
    }
}

void Statistics::Write(char *fromWhere)
{
    ofstream fWrite(fromWhere);
    if (fWrite.is_open() && setsList.size() > 0)
    {
        fWrite << relationList.size() << endl
               << endl;
        map<string, RelStats>::iterator k = relationList.begin();
        while (k != relationList.end())
        {
            if (fWrite.good())
            {
                fWrite << k->first << endl
                       << k->second << endl;
            }
            k++;
        }
        fWrite << "#####\n"
               << endl;
        fWrite << setsList.size() << endl
               << endl;
        int i = 0;
        while (i < (int)setsList.size())
        {
            RelStatSet r = setsList[i];
            fWrite << r << endl;
            i++;
        }
        fWrite.close();
    }
}

// Method to add the relation to the hash structure with its properties and number of distinct tuples for faster lookup
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string relationStr(relName), attributeName(attName);
    if (relationList.find(relationStr) != relationList.end())
    {
        relationList[relationStr].addAttr(attributeName, numDistincts);
    }
}

// Method to add the relation to the hash structure for lookup together with its number of tuple having int data type
void Statistics::AddRel(char *relName, int numTuples)
{
    string relationStr(relName);
    if (relationList.find(relationStr) != relationList.end())
    {
        relationList[relationStr].updateRowCnt((double)numTuples);
    }
    else
    {
        RelStatSet tempStatSet(relationStr);
        RelStats tempStatistics((double)numTuples);

        relationList[relationStr] = tempStatistics;
        relationSets[relationStr] = tempStatSet;

        setsList.push_back(tempStatSet);
    }
}

// Method to add the relation to the hash structure for lookup together with its number of tuple having double data type
void Statistics::AddRel(char *relName, double numTuples)
{
    string relationStr(relName);
    if (relationList.find(relationStr) != relationList.end())
    {
        relationList[relationStr].updateRowCnt(numTuples);
    }
    else
    {
        RelStatSet tempStatSet(relationStr);
        RelStats tempStatistics(numTuples);

        relationList[relationStr] = tempStatistics;
        relationSets[relationStr] = tempStatSet;

        setsList.push_back(tempStatSet);
    }
}

// Method to copy relation under different name
void Statistics::CopyRel(char *oldName, char *newName)
{
    string former(oldName), latter(newName);
    if (relationList.find(former) != relationList.end())
    {
        relationList[latter] = relationList[former];
        RelStatSet newStatSet(newName);
        relationSets[latter] = newStatSet;

        setsList.push_back(newStatSet);
    }
}

// This method is the same as calling apply with the apply flag, which is the same as the est operation.
void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    RelStatSet tempSet;

    vector<int> idxVec;
    vector<RelStatSet> copyVector;
    vector<string> relationList;

    int idx = 0, i = 0, j = 0, k = 0, a = 0;
    int oldSize = setsList.size();
    int newSize = oldSize - (int)idxVec.size() + 1;

    double joinEstimate = 0.0;
    double est = approxEstimate(parseTree, tempSet, idxVec, joinEstimate);

    while (i < numToJoin)
    {
        tempSet.addRelSet(relNames[i]);
        i++;
    }

    joinEstimate = parseJoin(parseTree);
    if (idxVec.size() > 1)
    {
        while (j < oldSize)
        {
            if (idxVec[idx] == j)
            {
                idx++;
            }
            else
            {
                copyVector.push_back(setsList[j]);
            }
            j++;
        }
        setsList.clear();
        while (k < newSize - 1)
        {
            setsList.push_back(copyVector[k]);
            k++;
        }
        tempSet.updatenTuples(est);
        setsList.push_back(tempSet);
        copyVector.clear();
        tempSet.getRel(relationList);
        while (a < tempSet.size())
        {
            relationSets[relationList[a]] = tempSet;
            a++;
        }
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    RelStatSet relationSet;
    vector<int> idxVec;

    double est = 0.0;
    double joinEst = 0.0;
    int i = 0;

    while (i < numToJoin)
    {
        relationSet.addRelSet(relNames[i]);
        i++;
    }

    joinEst = parseJoin(parseTree);
    est = approxEstimate(parseTree, relationSet, idxVec, joinEst);

    return est;
}

double Statistics::approxEstimate(struct AndList *parseTree, RelStatSet toEstimate, vector<int> &idxVec, double joinEstimate)
{

    double est = 0.0;
    int noRel = 0;
    int intersect = 0;
    int size = (int)setsList.size();
    int idx = 0;

    while (idx < size)
    {
        intersect = setsList[idx].intersect(toEstimate);
        if (intersect == -1)
        {
            idxVec.clear();
            noRel = 0;
            break;
        }
        else if (intersect > 0)
        {
            noRel = noRel + intersect;
            idxVec.push_back(idx);
            if (noRel == toEstimate.size())
            {
                break;
            }
        }
        idx++;
    }

    if (noRel > 0)
    {
        struct AndList *currAnd = parseTree;
        struct OrList *currOr;
        struct ComparisonOp *currOp;

        RelStats r1;
        string rel1;
        string attr1;
        est = 1.0;

        bool hasJoin = false;
        long nTuples = 0.0l;

        if (joinEstimate > 0)
        {
            est = joinEstimate;
        }

        while (currAnd)
        {
            struct OrList *tmpCurrentOr = currOr;
            vector<string> vecCheck;

            string lName;
            currOr = currAnd->left;

            while (tmpCurrentOr)
            {
                if (vecCheck.size() == 0)
                {
                    lName = tmpCurrentOr->left->left->value;
                    vecCheck.push_back(lName);
                }
                else
                {
                    if (vecCheck[0].compare(tmpCurrentOr->left->left->value) != 0)
                    {
                        lName = tmpCurrentOr->left->left->value;
                        vecCheck.push_back(lName);
                    }
                }
                tmpCurrentOr = tmpCurrentOr->rightOr;
            }

            bool sOrVal = currOr->rightOr == NULL;
            bool indOrV = vecCheck.size() > 1;
            double tmpOr = 0.0;

            if (indOrV)
            {
                tmpOr = 1.0;
            }

            while (currOr)
            {
                currOp = currOr->left;
                Operand *op = currOp->left;
                if (op->code != NAME)
                {
                    op = currOp->right;
                }
                parseRelAttr(op, rel1, attr1);
                r1 = relationList[rel1];
                if (currOp->code == EQUALS)
                {
                    if (sOrVal)
                    {
                        if (currOp->right->code == NAME && currOp->left->code == NAME)
                        {
                            tmpOr = 1.0;
                            hasJoin = true;
                        }
                        else
                        {
                            double const calculate = (1.0l / r1[attr1]);
                            tmpOr += calculate;
                        }
                    }
                    else
                    {
                        if (indOrV)
                        {
                            double const calculate = 1.0l - (1.0l / r1[attr1]);
                            tmpOr *= calculate;
                        }
                        else
                        {
                            double const calculate = 1.0l / r1[attr1];
                            tmpOr += calculate;
                        }
                    }
                }
                else
                {
                    if (sOrVal)
                    {
                        double const calculate = 1.0l / 3.0l;
                        tmpOr += calculate;
                    }
                    else
                    {
                        if (indOrV)
                        {
                            double calculate = 1.0l - (1.0l / 3.0l);
                            tmpOr *= calculate;
                        }
                        else
                        {
                            double calculate = 1.0l / 3.0l;
                            tmpOr += calculate;
                        }
                    }
                }
                if (!hasJoin)
                {
                    (relationSets[rel1].getnTuples() == -1) ? nTuples = r1.getnRows()
                                                            : nTuples = relationSets[rel1].getnTuples();
                }
                currOr = currOr->rightOr;
            }
            if (sOrVal)
            {
                est *= tmpOr;
            }
            else
            {
                (indOrV) ? est *= (1 - tmpOr)
                         : est *= tmpOr;
            }
            currAnd = currAnd->rightAnd;
        }
        if (!hasJoin)
        {
            est = nTuples * est;
        }
    }
    return est;
}

double Statistics::parseJoin(struct AndList *parseTree)
{
    double value = 0.0, temp = 0.0;
    if (parseTree)
    {
        bool done = false;
        string rels1, rels2, rels3, attrt1, attrt2;
        RelStats relationStats1, relationStats2;

        struct AndList *currAnd = parseTree;
        struct OrList *currOr;
        struct ComparisonOp *currOp;

        while (currAnd && !done)
        {
            currOr = currAnd->left;
            while (currOr && !done)
            {
                currOp = currOr->left;
                if (currOp)
                {
                    if (currOp->code == EQUALS && currOp->left->code == currOp->right->code)
                    {
                        done = true;
                        parseRelAttr(currOp->left, rels1, attrt1);
                        parseRelAttr(currOp->right, rels2, attrt2);

                        relationStats1 = relationList[rels1];
                        relationStats2 = relationList[rels2];
                        value = getTupleCount(rels1, temp) * getTupleCount(rels2, temp);

                        if (relationStats1[attrt1] < relationStats2[attrt2])
                        {
                            value = value / (double)relationStats2[attrt2];
                        }
                        else if (relationStats1[attrt1] >= relationStats2[attrt2])
                        {
                            value = value / (double)relationStats1[attrt1];
                        }
                    }
                }
                currOr = currOr->rightOr;
            }
            currAnd = currAnd->rightAnd;
        }
    }
    return value;
}

double Statistics::getTupleCount(string rel, double joinEstimate)
{
    double value = joinEstimate;
    if (value == 0.0)
        value = relationSets[rel].getnTuples();
    if (value == -1.0)
        value = (double)relationList[rel].getnRows();
    return value;
}

void Statistics::parseRel(struct Operand *oprnd, string &rel)
{
    string relationStr;
    stringstream temp;
    string value(oprnd->value);

    for (int idx = 0; value[idx] != '_'; idx++)
    {
        if (value[idx] == '.')
        {
            rel = temp.str();
            return;
        }
        temp << value[idx];
    }
}

void Statistics::parseRelAttr(struct Operand *oprnd, string &rel, string &attr)
{
    int idx = 0;
    stringstream temp;
    string relationStr;
    string value(oprnd->value);

    for (idx = 0; value[idx] != '_'; idx++)
    {
        if (value[idx] == '.')
        {
            rel = temp.str();
            break;
        }
        temp << value[idx];
    }

    if (value[idx] == '.')
        attr = value.substr(idx + 1);
    else
    {
        attr = value;
        relationStr = temp.str();
        rel = table[relationStr];
    }
}
