#include "OpenRelTable.h"

#include <cstring>
#include<cstdlib>

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

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
  }

  for(int i=0;i<MAX_OPEN;i++)
  tableMetaInfo[i].free=true;

  /**** Setting up Relation Cache entries ****/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /** setting up Relation Catalog relation in the Relation Cache Table**/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];

  for(int slotnum=0;slotnum<2;slotnum++)
  {
    
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

  for (int slotnum=0;slotnum<2;slotnum++)
  {
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
// in the tableMetaInfo array
  //   set free = false for RELCAT_RELID and ATTRCAT_RELID
      tableMetaInfo[RELCAT_RELID].free=false;
      tableMetaInfo[ATTRCAT_RELID].free=false;
  //   set relname for RELCAT_RELID and ATTRCAT_RELID
      strcpy(tableMetaInfo[RELCAT_RELID].relName,"RELATIONCAT");
      strcpy(tableMetaInfo[ATTRCAT_RELID].relName,"ATTRIBUTCAT");
   
  
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
    int i=0;
    for(i=0;i<MAX_OPEN;i++)
    {
      if(tableMetaInfo[i].free)
      return i;
    }
    return E_CACHEFULL;
  // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
    int id=0;
    for(id=0;id<MAX_OPEN;id++)
    {
      if(strcmp(tableMetaInfo[id].relName,relName)==0)
        return id;
    }
    return E_RELNOTOPEN;

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
  int relId= OpenRelTable::getRelId(relName);
  if(relId != E_RELNOTOPEN){
    return relId;
  }

  relId=OpenRelTable::getFreeOpenRelTableEntry();

  if (relId==E_CACHEFULL){
    return E_CACHEFULL;
  }

  /** Setting up Relation Cache entry for the relation **/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/

  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID,RELCAT_ATTR_RELNAME,attrVal,EQ);

  if ( relcatRecId.block==-1||relcatRecId.slot==-1 ) {
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, relcatRecId.slot);

  RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block =relcatRecId.block;
  relCacheEntry.recId.slot =relcatRecId.slot/*RELCAT_SLOTNUM_FOR_RELCAT*/;

  // allocate this on the heap because we want it to persist outside this function
    RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = relCacheEntry;
  
  /** Setting up Attribute Cache entry for the relation **/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry *listHead=nullptr,*attrCacheEntry;
  int nofAttr=RelCacheTable::relCache[relId]->relCatEntry.numAttrs;
  listHead=createlinkedlist(nofAttr);
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
 
  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  attrCacheEntry=listHead;
  for(int slot=0;slot<nofAttr;slot++){
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      RecId attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,ATTRCAT_ATTR_RELNAME,attrVal,EQ);;

      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
      RecBuffer attrCatBlock(attrcatRecId.block);
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBlock.getRecord(attrCatRecord,attrcatRecId.slot);
      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
      attrCacheEntry->recId.block = attrcatRecId.block;
      attrCacheEntry->recId.slot = attrcatRecId.slot;
      attrCacheEntry=attrCacheEntry->next;
  }

  // set the relIdth entry of the AttrCacheTable to listHead.
   AttrCacheTable::attrCache[relId] = listHead;

  /** str Setting up metadata in the Open Relation Table for the relation****/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  OpenRelTable::tableMetaInfo[relId].free=false;
  (strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName));

  return relId;
}

// int OpenRelTable::getRelId(char relName[ATTR_SIZE]) 
// {
//   // if relname is RELCAT_RELNAME, return RELCAT_RELID
//   if(strcmp(RELCAT_RELNAME,relName)==0)
//     return RELCAT_RELID;
//   // if relname is ATTRCAT_RELNAME, return ATTRCAT_RELID
//   if(strcmp(ATTRCAT_RELNAME,relName)==0)
//     return ATTRCAT_RELID;
  
//   if(strcmp(relName,"Students")==0)
//   return 2;

//   return E_RELNOTOPEN;
// }

int OpenRelTable::closeRel(int relId) {
  if (relId==RELCAT_RELID||relId==ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  if ( 0 > relId ||MAX_OPEN <= relId ) {
    return E_OUTOFBOUND;
  }

  if (OpenRelTable::tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  free (RelCacheTable::relCache[relId]);
	AttrCacheEntry *head = AttrCacheTable::attrCache[relId];
	AttrCacheEntry *next = head->next;

	while (next) {
		free (head);
		head = next;
		next = next->next;
	}
	free(head);	

  // update `tableMetaInfo` to set `relId` as a free slot
  OpenRelTable::tableMetaInfo[relId].free=true;
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;
  return SUCCESS;
}


OpenRelTable::~OpenRelTable() 
{
  // free all the memory that you allocated in the constructor
  // close all open relations (from rel-id = 2 onwards. Why?)
  for (int i = 2; i < MAX_OPEN; ++i) 
  {
    if (!tableMetaInfo[i].free) 
    {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }
    for(int i=0;i<2;i++)
    {
    free (RelCacheTable::relCache[i]);
	  AttrCacheEntry *head = AttrCacheTable::attrCache[i];
	  AttrCacheEntry *next = head->next;

	  while (next) 
    {
		  free (head);
		  head = next;
		  next = next->next;
	  }
	    free(head);	
  }
  
}


