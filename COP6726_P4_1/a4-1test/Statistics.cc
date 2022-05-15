#include "Statistics.h"
#include <set>

Statistics::Statistics()
{
    apply = false;
    attrDataStore = new stringHashmapParser();
    relDataStore = new stringIntParser();
}

Statistics::Statistics(Statistics &copyMe)
{
    attrDataStore = new stringHashmapParser(*(copyMe.attrDataStore));
    relDataStore = new stringIntParser(*(copyMe.relDataStore));
}

Statistics::~Statistics()
{
    delete relDataStore;
    delete attrDataStore;
}

// This method is used to write relation data with attribute data in hash map structure format
void Statistics::Write(char *fromWhere)
{
    remove(fromWhere);
    int c = 0;
    ofstream opFileWriter;
    string fileName(fromWhere);

    opFileWriter.open(fileName.c_str(), ios::out);
    int relDataStoreSize = relDataStore->size();

    opFileWriter << relDataStoreSize << endl;
    stringIntParser::iterator itr = relDataStore->begin();
    while (itr != relDataStore->end())
    {
        opFileWriter << itr->first.c_str() << "#" << itr->second << endl;
        itr++;
    }

    stringHashmapParser::iterator it = attrDataStore->begin();
    while (it != attrDataStore->end())
    {
        stringIntParser::iterator jt = it->second.begin();
        while (jt != it->second.end())
        {
            c++;
            jt++;
        }
        it++;
    }

    opFileWriter << c << endl;
    stringHashmapParser::iterator i = attrDataStore->begin();
    while (i != attrDataStore->end())
    {
        stringIntParser::iterator j = i->second.begin();
        while (j != i->second.end())
        {
            opFileWriter << (*i).first.c_str() << " " << (*j).first.c_str() << " " << (*j).second << endl;
            j++;
        }
        i++;
    }

    opFileWriter.close();
}

// This method is used for reading the relation and attribute data from the statistics file.
void Statistics::Read(char *fromWhere)
{
    string ipFileName(fromWhere);
    string ip;
    ifstream fileReaderIp;
    ifstream ipFile(fromWhere);
    string relationName, attributeName, numDistincts;
    if (!ipFile)
    {
        cout << "File doesn't exit!";
        return;
    }

    fileReaderIp.open(ipFileName.c_str(), ios::in);
    fileReaderIp >> ip;
    int relDataStoreSize = atoi(ip.c_str());
    relDataStore->clear();
    while (relDataStoreSize--)
    {
        fileReaderIp >> ip;
        size_t splitPos = ip.find_first_of("#");
        string left = ip.substr(0, splitPos);
        string right = ip.substr(splitPos + 1);
        int rightAsInt = atoi(right.c_str());
        (*relDataStore)[left] = rightAsInt;
    }

    fileReaderIp >> ip;
    attrDataStore->clear();
    fileReaderIp >> relationName >> attributeName >> numDistincts;
    while (!fileReaderIp.eof())
    {
        int numDistinctsInt = atoi(numDistincts.c_str());
        (*attrDataStore)[relationName][attributeName] = numDistinctsInt;
        fileReaderIp >> relationName >> attributeName >> numDistincts;
    }

    fileReaderIp.close();
}

// Method to add the relation to the hash structure for lookup together with its number of tuples
void Statistics::AddRel(char *relName, int numTuples)
{
    string strRelName(relName);
    if (relDataStore->find(strRelName) != relDataStore->end())
    {
        cout << "Data Record Available" << endl;
        relDataStore->erase(strRelName);
        relDataStore->insert(make_pair(strRelName, numTuples));
    }
    else
    {
        relDataStore->insert(make_pair(strRelName, numTuples));
    }
}

// Method to add the relation to the hash structure with its properties and number of distinct tuples for faster lookup
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string strRelName(relName), attStringName(attName);
    (numDistincts != -1) ? (*attrDataStore)[strRelName][attStringName] = numDistincts : (*attrDataStore)[strRelName][attStringName] = relDataStore->at(strRelName);
}

// This method is the same as calling apply with the apply flag, which is the same as the estimate operation.
void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    apply = true;
    Estimate(parseTree, relNames, numToJoin);
    apply = false;
}

// Method to copy relation under different name
void Statistics::CopyRel(char *oldName, char *newName)
{
    string former(oldName), latter(newName);
    int formerTuplesSize = (*relDataStore)[former];

    (*relDataStore)[latter] = formerTuplesSize;
    stringIntParser &formerAttributeData = (*attrDataStore)[former];
    stringIntParser::iterator formerAttributeInfo = formerAttributeData.begin();

    while (formerAttributeInfo != formerAttributeData.end())
    {
        string latterAttribute = latter;
        latterAttribute += "." + formerAttributeInfo->first;
        (*attrDataStore)[latter][latterAttribute] = formerAttributeInfo->second;
        formerAttributeInfo++;
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    struct AndList *andlist = parseTree;
    struct OrList *orlist;

    bool isdepth = false;
    bool done = false;
    bool isjoin = false;
    bool isjoinInvolved = false;

    string leftRel, rightRel;
    string joinleftRel, joinrightRel;
    string leftAttr, rightAttr, prev;
    stringIntParser relOpMap;

    double estimate = 0.0;
    double estORfraction = 1.0;
    double estANDfraction = 1.0;

    while (andlist != NULL)
    {
        estORfraction = 1.0;
        orlist = andlist->left;
        while (orlist != NULL)
        {
            isjoin = false;
            string joinAttrleft, joinAttrright;
            ComparisonOp *cmp = orlist->left;

            if (cmp->left->code == NAME)
            {

                leftAttr = cmp->left->value;
                if (!strcmp(leftAttr.c_str(), prev.c_str()))
                {
                    isdepth = true;
                }
                prev = leftAttr;

                joinAttrleft = cmp->left->value;
                stringHashmapParser::iterator mapEntry = attrDataStore->begin();
                while (mapEntry != attrDataStore->end())
                {
                    if ((*attrDataStore)[mapEntry->first].count(joinAttrleft) > 0)
                    {
                        leftRel = mapEntry->first;
                        break;
                    }
                    mapEntry++;
                }
            }
            else
            {
                cout << "Please correct your CNF, not a valid CNF" << endl;
                return 0;
            }
            if (cmp->right->code == NAME)
            {
                isjoin = true;
                isjoinInvolved = true;
                joinAttrright = cmp->right->value;
                stringHashmapParser::iterator mapEntry = attrDataStore->begin();
                while (mapEntry != attrDataStore->end())
                {
                    if ((*attrDataStore)[mapEntry->first].count(joinAttrright) > 0)
                    {
                        rightRel = mapEntry->first;
                        break;
                    }
                    mapEntry++;
                }
            }
            if (isjoin == true)
            {
                double rightDistinctCnt = (*attrDataStore)[rightRel][cmp->right->value];
                double leftDistinctCnt = (*attrDataStore)[leftRel][cmp->left->value];

                if (cmp->code == EQUALS)
                {
                    estORfraction *= (1.0 - (1.0 / max(leftDistinctCnt, rightDistinctCnt)));
                }

                joinrightRel = rightRel;
                joinleftRel = leftRel;
            }
            else
            {
                if (isdepth)
                {
                    if (!done)
                    {
                        estORfraction = 1.0 - estORfraction;
                        done = true;
                    }
                    if (cmp->code == EQUALS)
                    {
                        estORfraction += (1.0 / ((*attrDataStore)[leftRel][cmp->left->value]));
                        relOpMap[cmp->left->value] = cmp->code;
                    }
                    if (cmp->code == GREATER_THAN || cmp->code == LESS_THAN)
                    {
                        estORfraction += (1.0 / 3.0);
                        relOpMap[cmp->left->value] = cmp->code;
                    }
                }
                else
                {
                    // For single CNF with attribute and literals
                    if (cmp->code == EQUALS)
                    {
                        estORfraction *= (1.0 - (1.0 / (*attrDataStore)[leftRel][cmp->left->value]));
                        relOpMap[cmp->left->value] = cmp->code;
                    }
                    if (cmp->code == GREATER_THAN || cmp->code == LESS_THAN)
                    {
                        estORfraction *= (2.0 / 3.0);
                        relOpMap[cmp->left->value] = cmp->code;
                    }
                }
            }

            orlist = orlist->rightOr;
        }
        if (!isdepth)
        {
            estORfraction = 1.0 - estORfraction;
        }

        isdepth = false;
        done = false;
        estANDfraction *= estORfraction;
        andlist = andlist->rightAnd;
    }

    if (isjoinInvolved == true)
    {
        // when join involved for estimate calc
        double rightRecCnt = (*relDataStore)[joinrightRel];
        double leftRecCnt = (*relDataStore)[joinleftRel];
        estimate = leftRecCnt * rightRecCnt * estANDfraction;
    }
    else
    {
        // when no join involved in the  CNF
        double lefttuplecount = (*relDataStore)[leftRel];
        estimate = lefttuplecount * estANDfraction;
    }

    if (apply)
    {
        // here we add the estimate value to hash structures
        set<string> joinAttrSet;
        if (isjoinInvolved)
        {
            // Join rel op: case
            for (stringIntParser::iterator relOpMapitr = relOpMap.begin(); relOpMapitr != relOpMap.end(); relOpMapitr++)
            {
                for (int i = 0; i < relDataStore->size(); i++)
                {
                    if (relNames[i] == NULL)
                        continue;

                    int cnt = ((*attrDataStore)[relNames[i]]).count(relOpMapitr->first);

                    if (!cnt)
                        continue;

                    else if (cnt)
                    {
                        stringIntParser::iterator distinctCountMapitr = (*attrDataStore)[relNames[i]].begin();
                        while (distinctCountMapitr != (*attrDataStore)[relNames[i]].end())
                        {
                            if ((relOpMapitr->second == LESS_THAN) || (relOpMapitr->second == GREATER_THAN))
                            {
                                (*attrDataStore)[joinleftRel + "_" + joinrightRel][distinctCountMapitr->first] = (int)round((double)(distinctCountMapitr->second) / 3.0);
                            }
                            else if (relOpMapitr->second == EQUALS)
                            {

                                (relOpMapitr->first == distinctCountMapitr->first) ?

                                                                                   (*attrDataStore)[joinleftRel + "_" + joinrightRel][distinctCountMapitr->first] = 1
                                                                                   :

                                                                                   (*attrDataStore)[joinleftRel + "_" + joinrightRel][distinctCountMapitr->first] = min((int)round(estimate), distinctCountMapitr->second);
                            }
                            distinctCountMapitr++;
                        }

                        break;
                    }
                    else if (cnt > 1)
                    {
                        stringIntParser::iterator distinctCountMapitr = (*attrDataStore)[relNames[i]].begin();
                        while (distinctCountMapitr != (*attrDataStore)[relNames[i]].end())
                        {
                            if (relOpMapitr->second == EQUALS)
                            {
                                (relOpMapitr->first == distinctCountMapitr->first) ?

                                                                                   (*attrDataStore)[joinleftRel + "_" + joinrightRel][distinctCountMapitr->first] = cnt
                                                                                   :

                                                                                   (*attrDataStore)[joinleftRel + "_" + joinrightRel][distinctCountMapitr->first] = min((int)round(estimate), distinctCountMapitr->second);
                            }
                            distinctCountMapitr++;
                        }

                        break;
                    }
                    joinAttrSet.insert(relNames[i]);
                }
            }

            if (!joinAttrSet.count(joinleftRel))
            {
                // left join "only" rel op case
                stringIntParser::iterator entry = (*attrDataStore)[joinleftRel].begin();
                while (entry != (*attrDataStore)[joinleftRel].end())
                {
                    (*attrDataStore)[joinleftRel + "_" + joinrightRel][entry->first] = entry->second;
                    entry++;
                }
            }

            if (!joinAttrSet.count(joinrightRel))
            {
                // right join "only" rel op case
                stringIntParser::iterator entry = (*attrDataStore)[joinrightRel].begin();
                while (entry != (*attrDataStore)[joinrightRel].end())
                {
                    (*attrDataStore)[joinleftRel + "_" + joinrightRel][entry->first] = entry->second;
                    entry++;
                }
            }

            // updating hash structure with estimate value
            (*relDataStore)[joinleftRel + "_" + joinrightRel] = round(estimate);

            attrDataStore->erase(joinleftRel);
            attrDataStore->erase(joinrightRel);

            relDataStore->erase(joinleftRel);
            relDataStore->erase(joinrightRel);
        }
        else
        {
            // when no join involved in the CNF
            relDataStore->erase(leftRel);
            relDataStore->insert(make_pair(leftRel, round(estimate)));
        }
    }
    cout << "Estimated cost:" << estimate << endl;
    return estimate;
}
