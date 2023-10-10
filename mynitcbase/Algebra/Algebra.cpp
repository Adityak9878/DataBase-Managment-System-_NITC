#include "Algebra.h"

#include <cstring>
#include <cstdlib>
#include <iostream>
/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/

// will return if a string can be parsed as a floating point number
bool isNumber(char *str)
{
	int len;
	float ignore;
	/*
	  sscanf returns the number of elements read, so if there is no float matching
	  the first %f, ret will be 0, else it'll be 1

	  %n gets the number of characters read. this scanf sequence will read the
	  first float ignoring all the whitespace before and after. and the number of
	  characters read that far will be stored in len. if len == strlen(str), then
	  the string only contains a float with/without whitespace. else, there's other
	  characters.
	*/
	int ret = sscanf(str, "%f %n", &ignore, &len);
	return ret == 1 && len == strlen(str);
}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE])
{
	int srcRelId = OpenRelTable::getRelId(srcRel); // we'll implement this later
	if (srcRelId == E_RELNOTOPEN)
	{
		return E_RELNOTOPEN;
	}

	AttrCatEntry attrCatEntry;
	// get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
	int t = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
	//    return E_ATTRNOTEXIST if it returns the error
	if (t == E_ATTRNOTEXIST)
	{
		return E_ATTRNOTEXIST;
	}

	/*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
	int type = attrCatEntry.attrType;
	Attribute attrVal;
	if (type == NUMBER)
	{
		if (isNumber(strVal))
		{ // the isNumber() function is implemented below
			attrVal.nVal = atof(strVal);
		}
		else
		{
			return E_ATTRTYPEMISMATCH;
		}
	}
	else if (type == STRING)
	{
		strcpy(attrVal.sVal, strVal);
	}

	/*** Selecting records from the source relation ***/

	// Before calling the search function, reset the search to start from the first hit
	// using RelCacheTable::resetSearchIndex()
	RelCacheTable::resetSearchIndex(srcRelId);

	RelCatEntry relCatEntry;
	// get relCatEntry using RelCacheTable::getRelCatEntry()
	RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);
	/************************
	The following code prints the contents of a relation directly to the output
	console. Direct console output is not permitted by the actual the NITCbase
	specification and the output can only be inserted into a new relation. We will
	be modifying it in the later stages to match the specification.
	************************/

	printf("|");
	for (int i = 0; i < relCatEntry.numAttrs; ++i)
	{
		AttrCatEntry attrCatEntry;
		// get attrCatEntry at offset i using RelCacheTable::getRelCatEntry()//this is wrong
		// RelCacheTable::getRelCatEntry();
		AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
		printf(" %s |", attrCatEntry.attrName);
	}
	printf("\n");

	while (true)
	{
		RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

		if (searchRes.block != -1 && searchRes.slot != -1)
		{

			// get the record at searchRes using BlockBuffer.getRecord
			RecBuffer recBuffer(searchRes.block);
			HeadInfo head;
			recBuffer.getHeader(&head);
			Attribute currentRecord[head.numAttrs];
			recBuffer.getRecord(currentRecord, searchRes.slot);
			// print the attribute values in the same format as above
			printf("|");
			for (int i = 0; i < relCatEntry.numAttrs /*head.numAttrs*/; i++)
			{
				// const char *attrType = currentRecord.== NUMBER ? "NUM" : "STR";
				// AttrCatEntry attrCatEntry;
				AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
				if (attrCatEntry.attrType == NUMBER)
					printf(" %d |", (int)currentRecord[i].nVal);
				else
					printf(" %s |", currentRecord[i].sVal);
			}

			printf("\n");
		}
		else
		{

			// (all records over)
			break;
		}
	}

	return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
	int t = (strcmp(relName, RELCAT_RELNAME) || strcmp(relName, ATTRCAT_RELNAME));
	if (t == 0)
	{
		return E_NOTPERMITTED;
	}

	// get the relation's rel-id using OpenRelTable::getRelId() method
	int relId = OpenRelTable::getRelId(relName);

	// if relation is not open in open relation table, return E_RELNOTOPEN
	// (check if the value returned from getRelId function call = E_RELNOTOPEN)
	// get the relation catalog entry from relation cache
	// (use RelCacheTable::getRelCatEntry() of Cache Layer)
	if (relId == E_RELNOTOPEN)
		return E_RELNOTOPEN;
	RelCatEntry relcatBuf;
	RelCacheTable::getRelCatEntry(relId, &relcatBuf);

	if (relcatBuf.numAttrs != nAttrs)
		return E_NATTRMISMATCH;
	// let recordValues[numberOfAttributes] be an array of type union Attribute
	Attribute recordvalues[nAttrs];
	/*
		Converting 2D char array of record values to Attribute array recordValues
	 */
	for (int i = 0; i < nAttrs; i++)
	{
		// get the attr-cat entry for the i'th attribute from the attr-cache
		// (use AttrCacheTable::getAttrCatEntry())
		AttrCatEntry attrcatbuf;
		AttrCacheTable::getAttrCatEntry(relId, i, &attrcatbuf);

		int type = attrcatbuf.attrType;

		if (type == NUMBER)
		{
			// if the char array record[i] can be converted to a number
			// (check this using isNumber() function)
			if (isNumber(record[i]))
			{ // the isNumber() function is implemented below
				recordvalues[i].nVal = atof(record[i]);
			}
			else
			{
				return E_ATTRTYPEMISMATCH;
			}
		}
		else
		{
			// copy record[i] to recordValues[i].sVal
			strcpy(recordvalues[i].sVal, record[i]);
		}
	}

	// insert the record by calling BlockAccess::insert() function
	// let retVal denote the return value of insert call

	int retVal = BlockAccess::insert(relId, recordvalues);
	return retVal;
}
