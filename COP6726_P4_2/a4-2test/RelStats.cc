#ifndef REL_STATS_CC
#define REL_STATS_CC
#include "RelStats.h"

RelStats::RelStats()
{
    nRows = 0;
}

RelStats &RelStats::operator=(RelStats &relStats)
{
    if (&relStats != this)
    {
        this->nRows = relStats.nRows;
        this->attr.clear();
        this->attr = relStats.attr;
    }
    return *this;
}

RelStats::RelStats(double numOfRows)
{
    nRows = numOfRows;
}

RelStats::~RelStats()
{
    attr.clear();
}

ostream &operator<<(ostream &os, RelStats &relStats)
{
    os << relStats.nRows << endl;
    os << relStats.attr.size() << endl;
    map<string, int>::iterator it = relStats.attr.begin();
    while (it != relStats.attr.end())
    {
        os << it->first << endl
           << it->second << endl;
        it++;
    }
    return os;
}

istream &operator>>(istream &is, RelStats &relStats)
{
    string temp;
    stringstream tempStream("");
    string attrName;

    int distinctCnt = 0;
    int noItr = 0;
    int i = 0;

    getline(is, temp);
    tempStream.str(temp);
    if (!(tempStream >> relStats.nRows))
    {
        relStats.nRows = 0;
    }
    tempStream.clear();
    tempStream.str("");

    getline(is, temp);
    tempStream.str(temp);
    if (!(tempStream >> noItr))
    {
        noItr = 0;
    }
    tempStream.clear();
    tempStream.str("");

    while (i < noItr)
    {
        getline(is, temp);
        attrName = temp;
        getline(is, temp);
        tempStream.str(temp);
        if (!(tempStream >> distinctCnt))
        {
            distinctCnt = 0;
        }
        tempStream.clear();
        tempStream.str("");
        relStats.attr[attrName] = distinctCnt;
        i++;
    }
    return is;
}

void RelStats::updateRowCnt(double numOfRows)
{
    nRows = numOfRows;
}

void RelStats::addAttr(string attribute, int distinctCnt)
{
    attr[attribute] = distinctCnt;
}

int RelStats::operator[](string attribute)
{
    if (attr.find(attribute) == attr.end())
        return -2;
    else if (attr[attribute] == -1)
        return nRows;
    else
        return attr[attribute];
}

double RelStats::getnRows()
{
    return nRows;
}

RelStatSet::RelStatSet()
{
    nTuples = -1.0;
}

RelStatSet &RelStatSet::operator=(RelStatSet &rtempStream)
{
    if (&rtempStream != this)
    {
        this->nTuples = rtempStream.nTuples;
        this->setOfJoins = rtempStream.setOfJoins;
    }
    return *this;
}

RelStatSet::RelStatSet(string rel)
{
    setOfJoins.insert(rel);
    nTuples = -1.0;
}

RelStatSet::~RelStatSet()
{
}

ostream &operator<<(ostream &os, RelStatSet &relStatSet)
{
    os << relStatSet.nTuples << endl;
    os << relStatSet.setOfJoins.size() << endl;
    set<string>::iterator it = relStatSet.setOfJoins.begin();
    while (it != relStatSet.setOfJoins.end())
    {
        os << *it << endl;
        it++;
    }
    return os;
}

istream &operator>>(istream &is, RelStatSet &relStatSet)
{
    string temp;
    stringstream tempStream;

    double nTuples = 0;
    int noRel = 0;
    int i = 0;
    getline(is, temp);
    tempStream.str(temp);
    if (!(tempStream >> nTuples))
        nTuples = 0.0;

    tempStream.str("");
    tempStream.clear();

    relStatSet.updatenTuples(nTuples);

    getline(is, temp);
    tempStream.str(temp);
    if (!(tempStream >> noRel))
        noRel = 0;

    tempStream.str("");
    tempStream.clear();

    while (i < noRel)
    {
        getline(is, temp);
        relStatSet.setOfJoins.insert(temp);
        i++;
    }
    return is;
}

void RelStatSet::addRelSet(string rel)
{
    setOfJoins.insert(rel);
}

void RelStatSet::getRel(vector<string> &setsVec)
{
    set<string>::iterator it = setOfJoins.begin();
    while (it != setOfJoins.end())
    {
        setsVec.push_back(*it);
        it++;
    }
}

void RelStatSet::printRel()
{
    set<string>::iterator it = setOfJoins.begin();
    while (it != setOfJoins.end())
    {
        cout << *it << endl;
        it++;
    }
}

int RelStatSet::size()
{
    return setOfJoins.size();
}

int RelStatSet::intersect(RelStatSet relStatSet)
{
    RelStatSet volatileRtempStream;
    int result = volatileRtempStream.size();
    set<string>::iterator it = relStatSet.setOfJoins.begin();
    while (it != relStatSet.setOfJoins.end())
    {
        if (this->setOfJoins.find(*it) != this->setOfJoins.end())
        {
            volatileRtempStream.addRelSet(*it);
        }
        it++;
    }
    if (result == 0 || result == size())
        return result;
    else
        return -1;
}

void RelStatSet::merge(RelStatSet relStatSet)
{
    set<string>::iterator it = relStatSet.setOfJoins.begin();
    while (it != relStatSet.setOfJoins.end())
    {
        addRelSet(*it);
        it++;
    }
}

double RelStatSet::getnTuples()
{
    return nTuples;
}

void RelStatSet::updatenTuples(double numTuples)
{
    nTuples = numTuples;
}

#endif