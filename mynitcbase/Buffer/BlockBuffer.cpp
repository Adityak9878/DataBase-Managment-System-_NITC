#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum) {
 // initialise this.blockNum with the argument
     this->blockNum=blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  //unsigned char buffer[BLOCK_SIZE];
   unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }


  // read the block at this.blockNum into the buffer
   //Disk::readBlock(buffer,this->blockNum);
  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries, bufferPtr+16/* fill this */, 4);
  memcpy(&head->numAttrs, bufferPtr+20/* fill this */, 4);
  memcpy(&head->rblock, bufferPtr+12/* fill this */, 4);
  memcpy(&head->lblock, bufferPtr+8/* fill this */, 4);
  memcpy(&head->pblock, bufferPtr + 4, 4);

  return SUCCESS;
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;

  // get the header using this.getHeader() function
  BlockBuffer::getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  // read the block at this.blockNum into a buffer
  
  //unsigned char buffer[BLOCK_SIZE];
  // Disk::readBlock(buffer,this->blockNum);
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   // return any errors that might have occured in the process
  }


  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */

  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr+32+slotCount+(recordSize*slotNum)/* calculate buffer + offset */;

  // load the record into the rec data structure
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret=loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret!=SUCCESS)
    return ret;

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    struct HeadInfo head;
    //getting header
	  BlockBuffer::getHeader(&head);
    //getting number of attributes
    int noAttrs=head.numAttrs;   
    //number of slots 
    int noslots=head.numSlots;

    if(slotNum>=noslots)
    return E_OUTOFBOUND;
    // if input slotNum is not in the permitted range return E_OUTOFBOUND.
    

    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
      int recordSize = noAttrs * ATTR_SIZE;
	    unsigned char *slotPointer = bufferPtr + (HEADER_SIZE + noslots + (recordSize * slotNum)); // calculate buffer + offset

      memcpy(slotPointer, rec, recordSize);
      // update dirty bit using setDirtyBit()
      int ans=StaticBuffer::setDirtyBit(this->blockNum);

    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */

      // if (ans != SUCCESS) 
      // {
		  //   //std::cout << "There is some error in the code!\n";
		  //   exit(1);
	    // }

     return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) 
{
  // check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  //checking
    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }
    
  // set the timestamp of the corresponding buffer to 0 and increment the
  // timestamps of all other occupied buffers in BufferMetaInfo.

    if(bufferNum!=E_BLOCKNOTINBUFFER)
    {
      for(int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++)
      {
        StaticBuffer::metainfo[bufferIndex].timeStamp+=1;
      }
      StaticBuffer::metainfo[bufferNum].timeStamp=0;
    }

    else
    {
      bufferNum=StaticBuffer::getBufferNum(this->blockNum);

      if(bufferNum==E_OUTOFBOUND)
      return E_OUTOFBOUND;

      Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);

    }
// store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}


/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
 // BlockBuffer::getHeader(&head);
  RecBuffer recordblock(this->blockNum);
  HeadInfo header;
  recordblock.getHeader(&header);

  int slotCount =header.numSlots /* number of slots in block from header */;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  
  //memcpy(slotMap,slotMapInBuffer,slotCount);
  for(int i=0;i<slotCount;i++){
      *(slotMap+i)=*(slotMapInBuffer+i);
  }

  return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {

    double diff;
    if (attrType == STRING)
        diff = strcmp(attr1.sVal, attr2.sVal);

    else
        diff = attr1.nVal - attr2.nVal;

    
    if (diff > 0 )  return 1;
    if (diff < 0 )  return -1;
    else return 0;
    
}