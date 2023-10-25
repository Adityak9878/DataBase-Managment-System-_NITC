#include "StaticBuffer.h"
// the declarations for this class can be found at "StaticBuffer.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

void printBuffer (int x, unsigned char block[]){
  for (int i = 0; i < BLOCK_SIZE; i++){
    if (i % 32 == 0) printf ("\n");
    printf("%u ", block[i]);
  }
  printf("\n");
}

StaticBuffer::StaticBuffer() {
  unsigned char *it;
  it=blockAllocMap;
  for(int block=0;block<4;block++){
    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer,block);
    memcpy(it,buffer,BLOCK_SIZE);
    it=it+BLOCK_SIZE;
  }

  for (int bufferindex=0;bufferindex<32;bufferindex++) {
    // set metainfo[bufferindex] with the following values
    metainfo[bufferindex].free = true;
    metainfo[bufferindex].dirty = false;
    metainfo[bufferindex].timeStamp = -1;
    metainfo[bufferindex].blockNum = -1;
  }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int index=0;
  for(;index<BUFFER_CAPACITY;index++){
    if(!metainfo[index].free){
       metainfo[index].timeStamp++;
    }
  }
  int BufferNum;
  index=0;
  for(;index<BUFFER_CAPACITY;index++){
    if(metainfo[index].free){
        BufferNum=index;
        break;
    }
  }
  if (index==BUFFER_CAPACITY) {
    index=0;
    int max=-1;
    for(;index<BUFFER_CAPACITY;index++){
      if(max<metainfo[index].timeStamp){
          BufferNum=index;
          max=metainfo[index].timeStamp;
      }
    }
    if(metainfo[BufferNum].dirty){
     // unsigned char *buffer;
     // buffer = blocks[BufferNum];
      Disk::writeBlock(blocks[BufferNum],metainfo[BufferNum].blockNum);
    }
  }


  metainfo[BufferNum].free = false;
  metainfo[BufferNum].blockNum = blockNum;
  metainfo[index].timeStamp=0;
  metainfo[BufferNum].dirty = false;
  return BufferNum;
}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
  for(int i=0;i<BUFFER_CAPACITY;i++){
    if(metainfo[i].blockNum==blockNum){
        return i;
    }
  }
  //if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
    int index=StaticBuffer::getBufferNum(blockNum);
  
    if(index== E_BLOCKNOTINBUFFER)
      return E_BLOCKNOTINBUFFER;

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    else if(index==E_OUTOFBOUND)
    return E_OUTOFBOUND;

    else{
      metainfo[index].dirty = true;
      return SUCCESS;
    }
}

int StaticBuffer::getStaticBlockType(int blockNum){

  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
  return (int)blockAllocMap[blockNum]; 
}

// write back all modified blocks on system exit ==
StaticBuffer::~StaticBuffer() {
  unsigned char *it; 
  it=blockAllocMap;
  for(int block=0;block<4;block++){
    unsigned char buffer[BLOCK_SIZE];
    memcpy(buffer,it,BLOCK_SIZE);
    Disk::writeBlock(buffer,block);
    it=it+2048;
  }

  for (int bufferindex=0;bufferindex<32;bufferindex++){
    //unsigned char *buffer;
    //buffer = blocks[bufferindex];
    int t=metainfo[bufferindex].dirty==true&&metainfo[bufferindex].free==false;
    if(t)
      Disk::writeBlock(blocks[bufferindex],metainfo[bufferindex].blockNum);
  }
}

