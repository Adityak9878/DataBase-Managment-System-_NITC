#include "OpenRelTable.h"

#include <cstring>
#include<cstdlib>
//#include<stdio.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

AttrCacheEntry* createnewnode(){
  AttrCacheEntry *node;
  node=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  //node->next=nullptr;
  return node;
}

AttrCacheEntry* createlinkedlist(int n){
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
    tableMetaInfo[i].free=true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];

  for(int slotnum=0;slotnum<2;slotnum++){
    
    relCatBlock.getRecord(relCatRecord, slotnum/*RELCAT_SLOTNUM_FOR_RELCAT*/);

    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot =slotnum /*RELCAT_SLOTNUM_FOR_RELCAT*/;

  // allocate this on the heap because we want it to persist outside this function
    RelCacheTable::relCache[slotnum] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[slotnum]) = relCacheEntry;
  }
  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

  
  
  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  struct AttrCacheEntry *attrCacheEntry,*Head=nullptr;
  // iterate through all the attributes of the relation catalog and create a linked
  int entry=0;
  for (int slotnum=0;slotnum<2;slotnum++){
  int nofAttr=RelCacheTable::relCache[slotnum]->relCatEntry.numAttrs;
  Head=createlinkedlist(nofAttr);
  attrCacheEntry=Head;
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
    for(;nofAttr>0;nofAttr--,entry++){
      attrCatBlock.getRecord(attrCatRecord,entry);

      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
      attrCacheEntry->recId.block = ATTRCAT_BLOCK;//doubt
      attrCacheEntry->recId.slot = entry;
     //attrCacheEntry.next=t->next;
     //t=&attrCacheEntry;
    // NOTE: allocate each entry dynamically using malloc

    // set the next field in the last entry to nullptr
      attrCacheEntry=attrCacheEntry->next;
  }
    AttrCacheTable::attrCache[slotnum] = Head/* head of the linked list */;
  }

  tableMetaInfo[0].free=false;
  (strcpy(tableMetaInfo[0].relName,RELCAT_RELNAME));
  tableMetaInfo[1].free=false;
  (strcpy(tableMetaInfo[1].relName,ATTRCAT_RELNAME));
}





int OpenRelTable::getFreeOpenRelTableEntry() {
  for(int relid=0;relid<MAX_OPEN;relid++){
    if(tableMetaInfo[relid].free){
      return relid;
    }
  }
  return E_CACHEFULL;
}



int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {
  for(int relid=0;relid<MAX_OPEN;relid++){
    if(strcmp( tableMetaInfo[relid].relName,relName)==0){
      return relid;
    }
  }
  return E_RELNOTOPEN;
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

  /****** Setting up Relation Cache entry for the relation ******/

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
  
  /****** Setting up Attribute Cache entry for the relation ******/

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

  /****** str Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  OpenRelTable::tableMetaInfo[relId].free=false;
  (strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName));

  return relId;
}

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
  RelCacheEntry *relcacheentry=RelCacheTable::relCache[relId];
  if(relcacheentry->dirty==true){
    RelCatEntry relcatentry = relcacheentry->relCatEntry;
    Attribute relcatrecord[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&relcatentry,relcatrecord);
    RecBuffer relCatBlock(relcacheentry->recId.block);
    relCatBlock.setRecord(relcatrecord,relcacheentry->recId.slot);
  }
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

OpenRelTable::~OpenRelTable() {
  // free all the memory that you allocated in the constructor
   for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }

  //free the memory allocated for rel-id 0 and 1 in the caches
  for(int i=0;i<2;i++){
    free (RelCacheTable::relCache[i]);
	  AttrCacheEntry *head = AttrCacheTable::attrCache[i];
	  AttrCacheEntry *next = head->next;

	  while (next) {
		  free (head);
		  head = next;
		  next = next->next;
	}
	    free(head);	
  }

}