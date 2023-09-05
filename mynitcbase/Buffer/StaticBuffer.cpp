#include "StaticBuffer.h"

// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

// StaticBuffer::StaticBuffer() 
// {

//   // initialise all blocks as free
//   for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++/*bufferIndex = 0 to BUFFER_CAPACITY-1*/) {
//     metainfo[bufferIndex].free = true;
//   }
// }

StaticBuffer::StaticBuffer() {

  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++/*bufferIndex = 0 to BUFFER_CAPACITY-1*/) 
  {
    // set metainfo[bufferindex] with the following values
    metainfo[bufferIndex].free=true;
    //   free = true
    metainfo[bufferIndex].dirty=false;
    //   dirty = false
    metainfo[bufferIndex].timeStamp=-1;
    //   timestamp = -1
    metainfo[bufferIndex].blockNum=-1;
    //   blockNum = -1
  }
}

StaticBuffer::~StaticBuffer() 
{
  /*iterate through all the buffer blocks,
    write back blocks with metainfo as free=false,dirty=true
    using Disk::writeBlock()
    */
  for(int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++)
  {
    if(metainfo[bufferIndex].free==false && metainfo[bufferIndex].dirty==true)
    {
      Disk::writeBlock(blocks[bufferIndex],metainfo[bufferIndex].blockNum);
    }
  } 
}

// write back all modified blocks on system exit

/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
//StaticBuffer::~StaticBuffer() {}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer;

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  for(allocatedBuffer=0;allocatedBuffer<BUFFER_CAPACITY;allocatedBuffer++)
  {
    if(metainfo[allocatedBuffer].free)
    break;
  }

  //if no block is free then
  if(allocatedBuffer==BUFFER_CAPACITY)
  {
    int timespamp=-1;
    int bufferNum=-1;
    for(int bufferindex=0;bufferindex<BUFFER_CAPACITY;bufferindex++)
    {
      //setting with largest timespam 
      if(metainfo[bufferindex].timeStamp>timespamp)
      {
        timespamp=metainfo[bufferindex].timeStamp;
        bufferNum=bufferindex;
      }
    }   
      allocatedBuffer=bufferNum;
      if(metainfo[allocatedBuffer].dirty==true)
      {
        Disk::writeBlock(StaticBuffer::blocks[allocatedBuffer],metainfo[allocatedBuffer].blockNum);
      }
  }

  //if a block is free then
  //now allocatedBuffer countains the index of the buffernum free
  metainfo[allocatedBuffer].free=false;
  metainfo[allocatedBuffer].dirty=false;
  metainfo[allocatedBuffer].blockNum=blockNum;
  metainfo[allocatedBuffer].timeStamp=0;

  return allocatedBuffer;

}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) 
{
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
    if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
  for(int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++)
  {
    if(metainfo[bufferIndex].free == false && metainfo[bufferIndex].blockNum==blockNum)
    return bufferIndex;
  }
  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
    // find the buffer index corresponding to the block using getBufferNum().
     int bufferIndex=getBufferNum(blockNum);

    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(bufferIndex==E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
    if(bufferIndex==E_OUTOFBOUND)
    return E_OUTOFBOUND;

    // else
    //     (the bufferNum is valid)
    metainfo[bufferIndex].dirty=true;
    //     set the dirty bit of that buffer to true in metainfo

     return SUCCESS;
}


