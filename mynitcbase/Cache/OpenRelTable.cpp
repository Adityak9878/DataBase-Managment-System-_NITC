#include "OpenRelTable.h"

#include <cstring>
#include<cstdlib>
//#include<stdio.h>

AttrCacheEntry* createnewnode()
{
  AttrCacheEntry *node;
  node=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  //node->next=nullptr;
  return node;
}

AttrCacheEntry* createlinkedlist(int n)
{
  AttrCacheEntry *temp=nullptr,*t=nullptr;
  temp = createnewnode();
  n--;
  t=temp;
  while(n--){
    temp->next=createnewnode();
    temp=temp->next;
  }
  temp->next=nullptr;
  return t;
}

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  /**** Setting up Relation Cache entries ****/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /** setting up Relation Catalog relation in the Relation Cache Table**/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];

  for(int slotnum=0;slotnum<3;slotnum++){
    
    relCatBlock.getRecord(relCatRecord, slotnum/*RELCAT_SLOTNUM_FOR_RELCAT*/);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot =slotnum /*RELCAT_SLOTNUM_FOR_RELCAT*/;

  // allocate this on the heap because we want it to persist outside this function
    RelCacheTable::relCache[slotnum] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[slotnum]) = relCacheEntry;
  }
  /** setting up Attribute Catalog relation in the Relation Cache Table **/

  
  
  /**** Setting up Attribute cache entries ****/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /** setting up Relation Catalog relation in the Attribute Cache Table **/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry *attrCacheEntry,*Head=nullptr;
  // iterate through all the attributes of the relation catalog and create a linked
  int entry=0;

  for (int slotnum=0;slotnum<=2;slotnum++){
  int nofAttr=RelCacheTable::relCache[slotnum]->relCatEntry.numAttrs;
  Head=createlinkedlist(nofAttr);
  attrCacheEntry=Head;
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
    for(;nofAttr>0;entry++,nofAttr--)
    {
      attrCatBlock.getRecord(attrCatRecord,entry);

      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
      attrCacheEntry->recId.block = ATTRCAT_BLOCK;
      attrCacheEntry->recId.slot = entry;
     //attrCacheEntry.next=t->next;
     //t=&attrCacheEntry;
    // NOTE: allocate each entry dynamically using malloc

    // set the next field in the last entry to nullptr
      attrCacheEntry=attrCacheEntry->next;
  }
    AttrCacheTable::attrCache[slotnum] = Head/* head of the linked list */;
  }
  
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) 
{
  // if relname is RELCAT_RELNAME, return RELCAT_RELID
  if(strcmp(RELCAT_RELNAME,relName)==0)
    return RELCAT_RELID;
  // if relname is ATTRCAT_RELNAME, return ATTRCAT_RELID
  if(strcmp(ATTRCAT_RELNAME,relName)==0)
    return ATTRCAT_RELID;
  
  if(strcmp(relName,"Students")==0)
  return 2;

  return E_RELNOTOPEN;
}

OpenRelTable::~OpenRelTable() {
  // free all the memory that you allocated in the constructor
}


