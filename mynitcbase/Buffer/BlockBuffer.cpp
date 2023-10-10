#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum) {
 // initialise this.blockNum with the argument
     this->blockNum=blockNum;
}

BlockBuffer::BlockBuffer(char blockTypeChar){
    // allocate a block on the disk and a buffer in memory to hold the new block of
    // given type using getFreeBlock function and get the return error codes if any.

  int blocktype = blockTypeChar == 'R' ? REC :
                  blockTypeChar == 'L' ? IND_LEAF : 
                  blockTypeChar == 'I' ? IND_INTERNAL : UNUSED_BLK;
  
  int block=getFreeBlock(blocktype);
  if(block==E_DISKFULL){
    std::cout << "Error: Block is not available\n";
		this->blockNum = block;
		return;
  }
    // set the blockNum field of the object to that of the allocated block
    this->blockNum = block;
    // number if the method returned a valid block number,
    // otherwise set the error code returned as the block number.

    // (The caller must check if the constructor allocatted block successfully
    // by checking the value of block number field.)
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}
RecBuffer::RecBuffer() : BlockBuffer('R'){}
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


int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char ** buffPtr) {
    /* check whether the block is already present in the buffer
       using StaticBuffer.getBufferNum() */
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }
    if(bufferNum!=E_BLOCKNOTINBUFFER){
      int index=0;
      for(;index<BUFFER_CAPACITY;index++){
      if(!StaticBuffer::metainfo[index].free){
        StaticBuffer::metainfo[index].timeStamp++;
      }
      StaticBuffer::metainfo[bufferNum].timeStamp=0;
  } 
  }
  else{
    bufferNum= StaticBuffer::getFreeBuffer(this->blockNum);
    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }
  *buffPtr=StaticBuffer::blocks[bufferNum];
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

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
  if(ret!=SUCCESS) 
    return ret;

  struct HeadInfo head;
  getHeader(&head);
  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

    // if input slotNum is not in the permitted range .
  if(slotNum<0||slotNum>=slotCount)
    return E_OUTOFBOUND;
    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)//something wrong
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr+32+slotCount+(recordSize*slotNum);
  memcpy(slotPointer, rec, recordSize);
    // update dirty bit using setDirtyBit()
  int ans=StaticBuffer::setDirtyBit(this->blockNum);
    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */
  if(ans!=SUCCESS)
   exit(1);
  return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head){

  unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  
  if(ret!=SUCCESS) return ret;


    // cast bufferPtr to type HeadInfo*
  struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
  bufferHeader->numSlots = head->numSlots;
  bufferHeader->blockType=head->blockType;
  bufferHeader->lblock=head->lblock;
  bufferHeader->numAttrs=head->numAttrs;
  bufferHeader->numEntries=head->numEntries;
  bufferHeader->pblock=head->pblock;
  bufferHeader->rblock=head->rblock;
    // update dirty bit by calling StaticBuffer::setDirtyBit()
  ret= StaticBuffer::setDirtyBit(this->blockNum);
  if(ret!=SUCCESS)
   exit(1);
    // if setDirtyBit() failed, return the error code

  return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){

  unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if(ret!=SUCCESS) return ret;

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;

  // int32_t *blocktypeptr=(int32_t *)bufferPtr;
  *((int32_t *) bufferPtr) = blockType;
    
    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
  StaticBuffer::blockAllocMap[this->blockNum]=blockType;
    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call
  ret= StaticBuffer::setDirtyBit(this->blockNum);
  if(ret!=SUCCESS)
   exit(1);

  return SUCCESS;

}

int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
  int block=0;  
  for(;block<DISK_BLOCKS;block++){
    if(StaticBuffer::blockAllocMap[block]==UNUSED_BLK){
      break;
    }
  }
    // if no block is free, return E_DISKFULL.
  if(block==DISK_BLOCKS) return E_DISKFULL;
    // set the object's blockNum to the block number of the free block.
  this->blockNum=block;
    // find a free buffer using StaticBuffer::getFreeBuffer() .
  int buffernum=StaticBuffer::getFreeBuffer(this->blockNum);
  if (buffernum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }
    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.
  struct HeadInfo head;
  //head.blockType=blockType; this is wrong
  head.lblock=-1;
  head.numAttrs=0;
  head.numEntries=0;
  head.numSlots=0;
  head.pblock=-1;
  head.rblock=-1;
  setHeader(&head);

  
    // update the block type of the block to the input block type using setBlockType().
  setBlockType(blockType);
    // return block number of the free block.
  return block;  
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
  int ret=loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
  if(ret!=SUCCESS) return ret;
    // get the header of the block using the getHeader() function
  struct  HeadInfo head;
  getHeader(&head);
  int numSlots =head.numSlots /* the number of slots in the block */;
    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
  unsigned char *slotptr;
  slotptr=bufferPtr+HEADER_SIZE;
  memcpy(slotptr,slotMap,numSlots);
    // update dirty bit using 
  ret=StaticBuffer::setDirtyBit(this->blockNum);
  if (ret!=SUCCESS) 
    exit(1);
    // if setDirtyBit failed, return the value returned by the call

  return SUCCESS;
}

int BlockBuffer::getBlockNum(){

  return this->blockNum;
}

