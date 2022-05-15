#ifndef REL_STATS_H
#define REL_STATS_H

#include <iostream>
#include <vector>
#include <sstream>
#include <map>
#include <set>

using namespace std;

class RelStats
{
    double nRows;
    map<string, int> attr;

public:
    RelStats();
    RelStats &operator=(RelStats &relStat);
    RelStats(double numOfRows);
    ~RelStats();

    friend ostream &operator<<(ostream &os, RelStats &relStats);
    friend istream &operator>>(istream &is, RelStats &relStats);

    void updateRowCnt(double numOfRows);
    void addAttr(string attribute, int distinctVal);

    int operator[](string attribute);
    double getnRows();
};

class RelStatSet
{
    double nTuples;
    set<string> setOfJoins;

public:
    RelStatSet();
    RelStatSet &operator=(RelStatSet &rss);
    RelStatSet(string rel);
    ~RelStatSet();

    friend ostream &operator<<(ostream &os, RelStatSet &set);
    friend istream &operator>>(istream &is, RelStatSet &set);

    void addRelSet(string rel);
    void getRel(vector<string> &sets);
    void printRel();

    int size();
    int intersect(RelStatSet rss);
    void merge(RelStatSet rss);

    double getnTuples();
    void updatenTuples(double numOfTuples);
};

#endif