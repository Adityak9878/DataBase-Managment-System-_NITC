#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include<iostream>

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  return FrontendInterface::handleFrontend(argc, argv);
}
// #include "Buffer/StaticBuffer.h"
// #include "Cache/OpenRelTable.h"
// #include "Disk_Class/Disk.h"
// #include "FrontendInterface/FrontendInterface.h"
// #include<iostream>
// int main(int argc, char *argv[]) {
//   Disk disk_run;
//   StaticBuffer buffer;
//   OpenRelTable cache;

  
//   //for i = 0 and i = 1 (i.e RELCAT_RELID and ATTRCAT_RELID)
//   for(int i=0;i<=2;i++){
//       //get the relation catalog entry using RelCacheTable::getRelCatEntry()
//       RelCatEntry relCatBuf;
//       RelCacheTable::getRelCatEntry(i,&relCatBuf);
//       //printf("Relation: %s\n", relname);
//       printf("Relation: %s\n", relCatBuf.relName);


//       //for j = 0 to numAttrs of the relation - 1
//       for(int j=0;j<relCatBuf.numAttrs;j++){
//           //get the attribute catalog entry for (rel-id i, attribute offset j)
//           AttrCatEntry attrCatBuf;
//           //in attrCatEntry using AttrCacheTable::getAttrCatEntry()
//           AttrCacheTable::getAttrCatEntry(i,j,&attrCatBuf);
//           //printf("  %s: %s\n", attrName, attrType);

// 			const char *attrType = attrCatBuf.attrType == NUMBER ? "NUM" : "STR";
// 			printf ("    %s: %s\n", attrCatBuf.attrName, attrType);
//           // printf("  %s: %s\n",attrCatBuf.attrName, attrCatBuf.attrType);
//       }  
//       printf("\n");
//   }
//   return 0;
// }