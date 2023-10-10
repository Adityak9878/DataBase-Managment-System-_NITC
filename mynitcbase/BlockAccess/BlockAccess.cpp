#include "BlockAccess.h"

#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op)
{
   // get the previous search index of the relation relId from the relation cache
   // (use RelCacheTable::getSearchIndex() function)
   RecId prevRecId;
   int t = RelCacheTable::getSearchIndex(relId, &prevRecId);
   // Here use of t;
   //  let block and slot denote the record id of the record being currently checked
   int block = -1, slot = -1;
   // if the current search index record is invalid(i.e. both block and slot = -1)
   if (prevRecId.block == -1 && prevRecId.slot == -1)
   {
      // (no hits from previous search; search should start from the
      // first record itself)

      // get the first record block of the relation from the relation cache
      // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
      RelCatEntry relCatEntry;
      RelCacheTable::getRelCatEntry(relId, &relCatEntry);

      block = relCatEntry.firstBlk; // first record block of the relation
      slot = 0;
   }
   else
   {
      // (there is a hit from previous search; search should start from
      // the record next to the search index record)

      block = prevRecId.block;   // search index's block
      slot = prevRecId.slot + 1; // search index's slot + 1
   }

   /* The following code searches for the next record in the relation
      that satisfies the given condition
      We start from the record id (block, slot) and iterate over the remaining
      records of the relation
   */
   RelCatEntry relCatBuffer;
   RelCacheTable::getRelCatEntry(relId, &relCatBuffer);
   while (block != -1)
   {
      /* create a RecBuffer object for block (use RecBuffer Constructor for
         existing block) */
      RecBuffer currentBlock(block);

      HeadInfo head;
      currentBlock.getHeader(&head);
      // get the record with id (block, slot) using RecBuffer::getRecord()
      Attribute currentRecord[head.numAttrs];
      currentBlock.getRecord(currentRecord, slot);
      // get header of the block using RecBuffer::getHeader() function

      // get slot map of the block using RecBuffer::getSlotMap() function
      unsigned char slotMap[head.numSlots];
      currentBlock.getSlotMap(slotMap);

      if (slot >= relCatBuffer.numSlotsPerBlk /*the number of slots per block*/) //(i.e. no more slots in this block)
      {
         block = head.rblock; // right block of block
         slot = 0;
         continue; // continue to the beginning of this while loop
      }

      // if slot is free skip the loop
      // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
      if (slotMap[slot] == SLOT_UNOCCUPIED)
      {
         slot++;
         continue; // increment slot and continue to the next record slot
      }

      // compare record's attribut
      // e value to the the given attrVal as below:

      // firstly get the attribute offset for the attrName attribute
      AttrCatEntry attrcurbuf;
      // from the attribute cache entry of the relation using
      AttrCacheTable::getAttrCatEntry(relId, attrName, &attrcurbuf);
      /* use the attribute offset to get the value of the attribute from
         current record */
      int ofSet = attrcurbuf.offset;

      int cmpVal; // will store the difference between the attributes
                  // set cmpVal using compareAttrs()
      cmpVal = compareAttrs(currentRecord[ofSet], attrVal, attrcurbuf.attrType);
      /* Next task is to check whether this record satisfies the given condition.
         It is determined based on the output of previous comparison and
         the op value received.
         The following code sets the cond variable if the condition is satisfied.
      */
      if (
          (op == NE && cmpVal != 0) || // if op is "not equal to"
          (op == LT && cmpVal < 0) ||  // if op is "less than"
          (op == LE && cmpVal <= 0) || // if op is "less than or equal to"
          (op == EQ && cmpVal == 0) || // if op is "equal to"
          (op == GT && cmpVal > 0) ||  // if op is "greater than"
          (op == GE && cmpVal >= 0)    // if op is "greater than or equal to"
      )
      {
         /*
         set the search index in the relation cache as
         the record id of the record that satisfies the given condition
         (use RelCacheTable::setSearchIndex function)
         */
         RecId recId;
         recId.slot = slot;
         recId.block = block;
         RelCacheTable::setSearchIndex(relId, &recId);

         return RecId{block, slot};
      }

      slot++;
   }

   // no record in the relation with Id relid satisfies the given condition
   return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{
   /* reset the searchIndex of the relation catalog using
      RelCacheTable::resetSearchIndex() */
   RelCacheTable::resetSearchIndex(RELCAT_RELID);

   Attribute newRelationName; // set newRelationName with newName
   strcpy(newRelationName.sVal, newName);
   RecId srccheckrecid;
   srccheckrecid = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, newRelationName, EQ);
   // search the relation catalog for an entry with "RelName" = newRelationName
   if (srccheckrecid.block != -1 && srccheckrecid.slot != -1)
   {
      return E_RELEXIST;
   }
   // If relation with name newName already exists (result of linearSearch

   /* reset the searchIndex of the relation catalog using
      RelCacheTable::resetSearchIndex() */
   RelCacheTable::resetSearchIndex(RELCAT_RELID);

   Attribute oldRelationName; // set oldRelationName with oldName
   strcpy(oldRelationName.sVal, oldName);
   RecId srcrecid;
   srcrecid = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, oldRelationName, EQ);
   if (srcrecid.block == -1 && srcrecid.slot == -1)
   {
      return E_RELNOTEXIST;
   }
   // search the relation catalog for an entry with "RelName" = oldRelationName

   // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
   //    return E_RELNOTEXIST;

   /* get the relation catalog record of the relation to rename using a RecBuffer
      on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
   */

   RecBuffer relcatbuffer(srcrecid.block);
   Attribute relattribute[RELCAT_NO_ATTRS];
   relcatbuffer.getRecord(relattribute, srcrecid.slot);
   /* update the relation name attribute in the record with newName.
      (use RELCAT_REL_NAME_INDEX) */
   strcpy(relattribute[RELCAT_REL_NAME_INDEX].sVal, newName);
   // set back the record value using RecBuffer.setRecord
   relcatbuffer.setRecord(relattribute, srcrecid.slot);
   /*
   update all the attribute catalog entries in the attribute catalog corresponding
   to the relation with relation name oldName to the relation name newName
   */

   /* reset the searchIndex of the attribute catalog using
      RelCacheTable::resetSearchIndex() */
   RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

   // for i = 0 to numberOfAttributes :
   //     linearSearch on the attribute catalog for relName = oldRelationName
   //     get the record using RecBuffer.getRecord
   //
   //     update the relName field in the record to newName
   //     set back the record using RecBuffer.setRecord

   for (int slot = 0; slot < relattribute[RELCAT_NO_ATTRIBUTES_INDEX].nVal; slot++)
   {
      RecId recid;
      recid = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);
      RecBuffer attrcatbuffer(recid.block);
      Attribute attribute[ATTRCAT_NO_ATTRS];
      attrcatbuffer.getRecord(attribute, recid.slot);
      strcpy(attribute[ATTRCAT_REL_NAME_INDEX].sVal, newName);
      attrcatbuffer.setRecord(attribute, recid.slot);
   }

   return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE])
{

   /* reset the searchIndex of the relation catalog using
      RelCacheTable::resetSearchIndex() */
   RelCacheTable::resetSearchIndex(RELCAT_RELID);

   Attribute relNameAttr; // set relNameAttr to relName
   strcpy(relNameAttr.sVal, relName);
   RecId srccheckrecid;
   srccheckrecid = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
   if (srccheckrecid.block == -1 && srccheckrecid.slot == -1)
   {
      return E_RELNOTEXIST;
   }
   // Search for the relation with name relName in relation catalog using linearSearch()
   // If relation with name relName does not exist (search returns {-1,-1})
   //    return E_RELNOTEXIST;

   /* reset the searchIndex of the attribute catalog using
      RelCacheTable::resetSearchIndex() */
   RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

   /* declare variable attrToRenameRecId used to store the attr-cat recId
   of the attribute to rename */
   RecId attrToRenameRecId{-1, -1};
   RecId getindex{-1, -1};
   Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

   /* iterate over all Attribute Catalog Entry record corresponding to the
      relation to find the required attribute */
   while (true)
   {
      // linear search on the attribute catalog for RelName = relNameAttr
      attrToRenameRecId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
      // if there are no more attributes left to check (linearSearch returned {-1,-1})
      //     break;
      if (attrToRenameRecId.slot == -1 && attrToRenameRecId.block == -1)
         break; // return E_ATTRNOTEXIST;
      /* Get the record from the attribute catalog using RecBuffer.getRecord
        into attrCatEntryRecord */
      RecBuffer attrcatbuffer(attrToRenameRecId.block);
      attrcatbuffer.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
      // if attrCatEntryRecord.attrName = oldName
      //     attrToRenameRecId = block and slot of this record
      if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0)
      {
         getindex = attrToRenameRecId;
      }
      // if attrCatEntryRecord.attrName = newName
      //     return E_ATTREXIST;
      if (strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0) // error
         return E_ATTREXIST;
      ;
   }

   if (getindex.block == -1 && getindex.slot == -1)
      return E_ATTRNOTEXIST;

   // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
   /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
        attrToRenameRecId.slot */
   RecBuffer attrcatbuffer(getindex.block);
   attrcatbuffer.getRecord(attrCatEntryRecord, getindex.slot);
   //   update the AttrName of the record with newName
   strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
   //   set back the record with RecBuffer.setRecord
   attrcatbuffer.setRecord(attrCatEntryRecord, getindex.slot);
   return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record)
{
   // get the relation catalog entry from relation cache
   // ( use RelCacheTable::getRelCatEntry() of Cache Layer)
   RelCatEntry relCatEntry;
   RelCacheTable::getRelCatEntry(relId, &relCatEntry);
   int blockNum = relCatEntry.firstBlk /* first record block of the relation (from the rel-cat entry)*/;

   // rec_id will be used to store where the new record will be inserted
   RecId rec_id = {-1, -1};

   int numOfSlots = relCatEntry.numSlotsPerBlk /* number of slots per record block */;
   int numOfAttributes = relCatEntry.numAttrs /* number of attributes of the relation */;

   int prevBlockNum = -1 /* block number of the last element in the linked list = -1 */;

   /*
       Traversing the linked list of existing record blocks of the relation
       until a free slot is found OR
       until the end of the list is reached
   */
   while (blockNum != -1)
   {
      // create a RecBuffer object for blockNum (using appropriate constructor!)
      RecBuffer currentrecord(blockNum);
      // get header of block(blockNum) using RecBuffer::getHeader() function
      struct HeadInfo head;
      currentrecord.getHeader(&head);
      // get slot map of block(blockNum) using RecBuffer::getSlotMap() function
      unsigned char slotmap[numOfSlots];
      currentrecord.getSlotMap(slotmap);
      // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
      // (Free slot can be found by iterating over the slot map of the block)
      /* slot map stores SLOT_UNOCCUPIED if slot is free and
         SLOT_OCCUPIED if slot is occupied) */
      int slotindex = 0;
      for (; slotindex < numOfSlots; slotindex++)
      {
         if (slotmap[slotindex] == SLOT_UNOCCUPIED)
            break;
      }
      if (slotindex < numOfSlots)
      {
         rec_id.block = blockNum;
         rec_id.slot = slotindex;
         break;
      }

      /* if a free slot is found, set rec_id and discontinue the traversal
         of the linked list of record blocks (break from the loop) */

      /* otherwise, continue to check the next block by updating the
         block numbers as follows:
            update prevBlockNum = blockNum
            update blockNum = header.rblock (next element in the linked
                                             list of record blocks)
     */
      prevBlockNum = blockNum;
      blockNum = head.rblock;
   }

   //  if no free slot is found in existing record blocks (rec_id = {-1, -1})

   if (rec_id.block == -1 && rec_id.slot == -1)
   {
      // if relation is RELCAT, do not allocate any more blocks
      //     return E_MAXRELATIONS;
      if (relId == 0)
         return E_MAXRELATIONS;
      // Otherwise,
      // get a new record block (using the appropriate RecBuffer constructor!)
      // get the block number of the newly allocated block
      // (use BlockBuffer::getBlockNum() function)
      // let ret be the return value of getBlockNum() function call
      int newblockNum;
      RecBuffer newrecordblock;
      newblockNum = newrecordblock.getBlockNum();
      if (blockNum == E_DISKFULL)
      {
         return E_DISKFULL;
      }

      // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0
      rec_id.block = newblockNum;
      rec_id.slot = 0;
      /*
          set the header of the new record block such that it links with
          existing record blocks of the relation
          set the block's header as follows:
          blockType: REC, pblock: -1
          lblock
                = -1 (if linked list of existing record blocks was empty
                       i.e this is the first insertion into the relation)
                = prevBlockNum (otherwise),
          rblock: -1, numEntries: 0,
          numSlots: numOfSlots, numAttrs: numOfAttributes
          (use BlockBuffer::setHeader() function)
      */
      struct HeadInfo head;

      head.blockType = REC;
      head.pblock = -1;
      head.lblock = prevBlockNum;
      head.rblock = -1;
      head.numEntries = 0;
      head.numSlots = numOfSlots;
      head.numAttrs = numOfAttributes;

      newrecordblock.setHeader(&head);
      /*
          set block's slot map with all slots marked as free
          (i.e. store SLOT_UNOCCUPIED for all the entries)
          (use RecBuffer::setSlotMap() function)
      */
      unsigned char slotmap[numOfSlots];
      for (int slotindex = 0; slotindex < numOfSlots; slotindex++)
      {
         slotmap[slotindex] = SLOT_UNOCCUPIED;
      }
      newrecordblock.setSlotMap(slotmap);
      // if prevBlockNum != -1
      //   {
      //       // create a RecBuffer object for prevBlockNum
      //       // get the header of the block prevBlockNum and
      //       // update the rblock field of the header to the new block
      //       // number i.e. rec_id.block
      //       // (use BlockBuffer::setHeader() function)
      //   }
      if (prevBlockNum != -1)
      {
         RecBuffer prevrecord(prevBlockNum);
         HeadInfo head;
         prevrecord.getHeader(&head);
         head.rblock = newblockNum;
         prevrecord.setHeader(&head);
      }
      else
      {
         // update first block field in the relation catalog entry to the
         // RelCatEntry relCatentry;
         RelCacheTable::getRelCatEntry(relId, &relCatEntry);
         relCatEntry.firstBlk = newblockNum;
         RelCacheTable::setRelCatEntry(relId, &relCatEntry);
         // new block (using RelCacheTable::setRelCatEntry() function)
      }

      // update last block field in the relation catalog entry to the
      // new block (using RelCacheTable::setRelCatEntry() function)
      // RelCatEntry relCatEntry;
      RelCacheTable::getRelCatEntry(relId, &relCatEntry);
      relCatEntry.lastBlk = newblockNum;
      RelCacheTable::setRelCatEntry(relId, &relCatEntry);
   }

   // create a RecBuffer object for rec_id.block
   // insert the record into rec_id'th slot using RecBuffer.setRecord())
   RecBuffer recordblock(rec_id.block);
   recordblock.setRecord(record, rec_id.slot);

   /* update the slot map of the block by marking entry of the slot to
      which record was inserted as occupied) */
   // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
   // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)
   unsigned char slotmap[numOfSlots];
   recordblock.getSlotMap(slotmap);
   slotmap[rec_id.slot] = SLOT_OCCUPIED;
   recordblock.setSlotMap(slotmap);

   // increment the numEntries field in the header of the block to
   // which record was inserted
   // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
   struct HeadInfo head;
   recordblock.getHeader(&head);
   head.numEntries++;
   recordblock.setHeader(&head);

   // Increment the number of records field in the relation cache entry for
   // the relation. (use RelCacheTable::setRelCatEntry function)
   // RelCatEntry relCatEntry;
   RelCacheTable::getRelCatEntry(relId, &relCatEntry);
   relCatEntry.numRecs++;
   RelCacheTable::setRelCatEntry(relId, &relCatEntry);
   /* B+ Tree Insertions */
   // (the following section is only relevant once indexing has been implemented)

   int flag = SUCCESS;

   return flag;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
   // Declare a variable called recid to store the searched record
   RecId recId;

   /* search for the record id (recid) corresponding to the attribute with
   attribute name attrName, with value attrval and satisfying the condition op
   using linearSearch() */
   recId = linearSearch(relId, attrName, attrVal, op);

   if (recId.block == -1 && recId.slot == -1)
      return E_NOTFOUND;

   /* Copy the record with record id (recId) to the record buffer (record)
      For this Instantiate a RecBuffer class object using recId and
      call the appropriate method to fetch the record
   */
   RecBuffer currrecord(recId.block);

   currrecord.getRecord(record, recId.slot);

   return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE])
{
   int t = (strcmp(relName, RELCAT_RELNAME) && strcmp(relName, ATTRCAT_RELNAME));
   if (t == 0)
   {
      return E_NOTPERMITTED;
   }

   RelCacheTable::resetSearchIndex(RELCAT_RELID);

   Attribute relNameAttr;
   strcpy(relNameAttr.sVal, relName);

   RecId targetrecid;

   targetrecid = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);

   if (targetrecid.block == -1 && targetrecid.slot == -1)
      return E_RELNOTEXIST;

   Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

   RecBuffer todelbuffer(targetrecid.block);

   todelbuffer.getRecord(relCatEntryRecord, targetrecid.slot);

   int firstblock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
   int noattr = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

   int block = firstblock;
   while (block != -1)
   {
      RecBuffer bufferofblock(block);

      struct HeadInfo head;

      bufferofblock.getHeader(&head);

      block = head.rblock;

      bufferofblock.releaseBlock();
   }

   /***
       Deleting attribute catalog entries corresponding the relation and index
       blocks corresponding to the relation with relName on its attributes
   ***/

   // reset the searchIndex of the attribute catalog
   RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
   int numberOfAttributesDeleted = 0;

   while (true)
   {
      RecId attrCatRecId;

      attrCatRecId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

      if (attrCatRecId.block == -1 && attrCatRecId.slot == -1)
         break;

      numberOfAttributesDeleted++;

      RecBuffer todelatrecord(attrCatRecId.block);

      struct HeadInfo head;

      todelatrecord.getHeader(&head);

      Attribute attrecords[ATTRCAT_NO_ATTRS];

      todelatrecord.getRecord(attrecords, attrCatRecId.slot);

      // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
      int noslots = head.numSlots;
      unsigned char slotmap[noslots];

      todelatrecord.getSlotMap(slotmap);

      slotmap[attrCatRecId.slot] = SLOT_UNOCCUPIED;

      todelatrecord.setSlotMap(slotmap);

      head.numEntries--;

      todelatrecord.setHeader(&head);

      /* If number of entries become 0, releaseBlock is called after fixing
         the linked list.
      */

      if (head.numEntries == 0)
      {

         RecBuffer prevatrecord(head.lblock);

         struct HeadInfo prevhead;
         prevatrecord.getHeader(&prevhead);

         prevhead.rblock = head.rblock;

         prevatrecord.setHeader(&prevhead);

         if (head.rblock != -1)
         {
            RecBuffer nextatrecord(head.rblock);

            struct HeadInfo nexthead;
            nextatrecord.getHeader(&nexthead);

            nexthead.lblock = head.lblock;

            nextatrecord.setHeader(&nexthead);
         }
         else
         {
            // (the block being released is the "Last Block" of the relation.)
            /* update the Relation Catalog entry's LastBlock field for this
               relation with the block number of the previous block. */
            RelCatEntry relcatentry;
            RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relcatentry);
            relcatentry.lastBlk = head.lblock;
            RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relcatentry);
         }

         // (Since the attribute catalog will never be empty(why?), we do not
         //  need to handle the case of the linked list becoming empty - i.e
         //  every block of the attribute catalog gets released.)

         todelatrecord.releaseBlock();
      }
   }

   /*** Delete the entry corresponding to the relation from relation catalog ***/
   // Fetch the header of Relcat block
   int temp = block;

   RecBuffer relcatbuffer(RELCAT_BLOCK);
   HeadInfo relhead;

   relcatbuffer.getHeader(&relhead);

   /* Decrement the numEntries in the header of the block corresponding to the
      relation catalog entry and set it back */
   relhead.numEntries--;
   relcatbuffer.setHeader(&relhead);
   /* Get the slotmap in relation catalog, update it by marking the slot as
      free(SLOT_UNOCCUPIED) and set it back. */
   int nofslots = relhead.numSlots;
   unsigned char slotmap[nofslots];
   relcatbuffer.getSlotMap(slotmap);

   slotmap[targetrecid.slot] = SLOT_UNOCCUPIED;
   relcatbuffer.setSlotMap(slotmap);

   /*** Updating the Relation Cache Table ***/
   /** Update relation catalog record entry (number of records in relation
       catalog is decreased by 1) **/
   // Get the entry corresponding to relation catalog from the relation
   // cache and update the number of records and set it back
   // (using RelCacheTable::setRelCatEntry() function)

   RelCatEntry relcatentry;
   RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relcatentry);
   relcatentry.numRecs--;
   RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relcatentry);

   /** Update attribute catalog entry (number of records in attribute catalog
       is decreased by numberOfAttributesDeleted) **/
   // i.e., #Records = #Records - numberOfAttributesDeleted

   // Get the entry corresponding to attribute catalog from the relation
   // cache and update the number of records and set it back
   // (using RelCacheTable::setRelCatEntry() function)
   RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relcatentry);
   relcatentry.numRecs = relcatentry.numRecs - numberOfAttributesDeleted;
   RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relcatentry);

   return SUCCESS;
}