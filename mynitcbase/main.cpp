#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include<iostream>


int main(int argc, char *argv[]) 
{
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  
 // for i = 0 and i = 1 (i.e RELCAT_RELID and ATTRCAT_RELID)
  for(int i=0;i<=2;i++)
  {
    //i=0  ->  RELCAT_ID
    //i=1  ->  ATTRCAT_ID
     
     RelCatEntry RelCatBuffer;
     RelCacheTable::getRelCatEntry(i,&RelCatBuffer);
     printf("Relation : %s\n ", RelCatBuffer.relName);

     for(int j=0;j<RelCatBuffer.numAttrs;j++)
     
     {
         AttrCatEntry AttrCatBuffer;
         AttrCacheTable::getAttrCatEntry(i,j,&AttrCatBuffer);
         const char *attrType = AttrCatBuffer.attrType == NUMBER ? "NUM" : "STR";
			   printf (" %s: %s\n", AttrCatBuffer.attrName, attrType);

     }

     printf("\n");

  }
      // get the relation catalog entry using RelCacheTable::getRelCatEntry()
      // printf("Relation: %s\n", relname);

      // for j = 0 to numAttrs of the relation - 1
      //     get the attribute catalog entry for (rel-id i, attribute offset j)
      //      in attrCatEntry using AttrCacheTable::getAttrCatEntry()

      //     printf("  %s: %s\n", attrName, attrType);
  

  return 0;
}