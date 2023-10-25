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

	/*** Creating and opening the target relation ***/
	// Prepare arguments for createRel() in the following way:
	// get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCatEntry srcRelcatBuf;
	RelCacheTable::getRelCatEntry(srcRelId, &srcRelcatBuf);

	int src_nAttrs = srcRelcatBuf.numAttrs;

	char attr_names[src_nAttrs][ATTR_SIZE];

	int attr_types[src_nAttrs];

	/*iterate through 0 to src_nAttrs-1 :
		get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
		fill the attr_names, attr_types arrays that we declared with the entries
		of corresponding attributes
	*/
	for (int index = 0; index < src_nAttrs; index++)
	{
		AttrCatEntry srcattrcatentry;
		AttrCacheTable::getAttrCatEntry(srcRelId, index, &srcattrcatentry);

		strcpy(attr_names[index], srcattrcatentry.attrName);

		attr_types[index] = srcattrcatentry.attrType;
	}

	/* Create the relation for target relation by calling Schema::createRel()
	   by providing appropriate arguments */
	// if the createRel returns an error code, then return that value.
	int retval = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
	if (retval != SUCCESS)
		return retval;

	/* Open the newly created target relation by calling OpenRelTable::openRel()
	   method and store the target relid */
	int trgRelId = OpenRelTable::openRel(targetRel);

	/* If opening fails, delete the target relation by calling Schema::deleteRel()
	   and return the error value returned from openRel() */
	if (trgRelId < 0 || trgRelId >= MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return trgRelId;
	}

	/*** Selecting and inserting records into the target relation ***/
	/* Before calling the search function, reset the search to start from the
	   first using RelCacheTable::resetSearchIndex() */

	Attribute record[src_nAttrs];

	/*
		The BlockAccess::search() function can either do a linearSearch or
		a B+ tree search. Hence, reset the search index of the relation in the
		relation cache using RelCacheTable::resetSearchIndex().
		Also, reset the search index in the attribute cache for the select
		condition attribute with name given by the argument `attr`. Use
		AttrCacheTable::resetSearchIndex().
		Both these calls are necessary to ensure that search begins from the
		first record.
	*/
	RelCacheTable::resetSearchIndex(srcRelId);
	AttrCacheTable::resetSearchIndex(srcRelId,attr);

	// read every record that satisfies the condition by repeatedly calling
	// BlockAccess::search() until there are no more records to be read
	int k=0;
	while(BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS)
	k++;
	while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS)
	{

		retval = BlockAccess::insert(trgRelId, record);

		if (retval != SUCCESS)
		{
			Schema::closeRel(targetRel);
			Schema::deleteRel(targetRel);
			return retval;
		}
	}
	printf("%d\n",k);
	// Close the targetRel by calling closeRel() method of schema layer
	Schema::closeRel(targetRel);

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

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE])
{

	int srcRelId = OpenRelTable::getRelId(srcRel);
	if (srcRelId == E_RELNOTOPEN)
	{
		return E_RELNOTOPEN;
	}

	// get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCatEntry relCatEntry;
	RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

	// get the no. of attributes present in relation from the fetched RelCatEntry.
	int numAttrs = relCatEntry.numAttrs;

	// declare attr_offset[tar_nAttrs] an array of type int.
	// where i-th entry will store the offset in a record of srcRel for the
	// i-th attribute in the target relation.
	int attr_offset[tar_nAttrs];

	// let attr_types[tar_nAttrs] be an array of type int.
	// where i-th entry will store the type of the i-th attribute in the
	// target relation.
	int attr_types[tar_nAttrs];

	/*** Checking if attributes of target are present in the source relation
		 and storing its offsets and types ***/

	/*iterate through 0 to tar_nAttrs-1 :
		- get the attribute catalog entry of the attribute with name tar_attrs[i].
		- if the attribute is not found return E_ATTRNOTEXIST
		- fill the attr_offset, attr_types arrays of target relation from the
		  corresponding attribute catalog entries of source relation
	*/
	for (int index = 0; index < tar_nAttrs; index++)
	{
		AttrCatEntry attrcatentry;
		int retval = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[index], &attrcatentry);
		if (retval != SUCCESS)
			return E_ATTRNOTEXIST;
		attr_offset[index] = attrcatentry.offset;
		attr_types[index] = attrcatentry.attrType;
	}

	/*** Creating and opening the target relation ***/

	// Create a relation for target relation by calling Schema::createRel()
	int retval = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
	if (retval != SUCCESS)
		return retval;

	// Open the newly created target relation by calling OpenRelTable::openRel()
	// and get the target relid
	int trgRelId = OpenRelTable::openRel(targetRel);

	// If opening fails, delete the target relation by calling Schema::deleteRel()
	// and return the error value from openRel()
    if (trgRelId < 0 || trgRelId >= MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return trgRelId;
	}

	/*** Inserting projected records into the target relation ***/

	// Take care to reset the searchIndex before calling the project function
	// using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(srcRelId);

	Attribute record[numAttrs];

	while (BlockAccess::project(srcRelId, record) == SUCCESS )
	{
		// the variable `record` will contain the next record

		Attribute proj_record[tar_nAttrs];

		// iterate through 0 to tar_attrs-1:
		//     proj_record[attr_iter] = record[attr_offset[attr_iter]]
		for(int attr_iter = 0; attr_iter < tar_nAttrs; attr_iter++)
			proj_record[attr_iter] = record[attr_offset[attr_iter]];
		
		retval = BlockAccess::insert(trgRelId, proj_record);

		if (retval != SUCCESS)
		{
			Schema::closeRel(targetRel);
			Schema::deleteRel(targetRel);
			return retval;
		}
	}

	Schema::closeRel(targetRel);

	return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE])
{

	int srcRelId = OpenRelTable::getRelId(srcRel);
	if (srcRelId == E_RELNOTOPEN)
	{
		return E_RELNOTOPEN;
	}

	// get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
	RelCatEntry relCatEntry;
	RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

	// get the no. of attributes present in relation from the fetched RelCatEntry.
	int numAttrs = relCatEntry.numAttrs;

	// attrNames and attrTypes will be used to store the attribute names
	// and types of the source relation respectively
	char attrNames[numAttrs][ATTR_SIZE];
	int attrTypes[numAttrs];

	/*iterate through every attribute of the source relation :
		- get the AttributeCat entry of the attribute with offset.
		  (using AttrCacheTable::getAttrCatEntry())
		- fill the arrays `attrNames` and `attrTypes` that we declared earlier
		  with the data about each attribute
	*/
	for (int index = 0; index < numAttrs; index++)
	{
		AttrCatEntry srcattrcatentry;
		AttrCacheTable::getAttrCatEntry(srcRelId, index, &srcattrcatentry);

		strcpy(attrNames[index], srcattrcatentry.attrName);

		attrTypes[index] = srcattrcatentry.attrType;
	}

	/*** Creating and opening the target relation ***/

	// Create a relation for target relation by calling Schema::createRel()

	// if the createRel returns an error code, then return that value.
	int retval = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);
	if (retval != SUCCESS)
		return retval;

	// Open the newly created target relation by calling OpenRelTable::openRel()
	// and get the target relid
	int trgRelId = OpenRelTable::openRel(targetRel);

	// If opening fails, delete the target relation by calling Schema::deleteRel() of
	// return the error value returned from openRel().
	if (trgRelId < 0 || trgRelId >= MAX_OPEN)
	{
		Schema::deleteRel(targetRel);
		return trgRelId;
	}

	/*** Inserting projected records into the target relation ***/

	// Take care to reset the searchIndex before calling the project function
	// using RelCacheTable::resetSearchIndex()
	RelCacheTable::resetSearchIndex(srcRelId);

	Attribute record[numAttrs];

	while (BlockAccess::project(srcRelId, record) == SUCCESS)
	{
		// record will contain the next record

		retval = BlockAccess::insert(trgRelId, record);

		if (retval != SUCCESS)
		{
			Schema::closeRel(targetRel);
			Schema::deleteRel(targetRel);
			return retval;
		}
	}

	Schema::closeRel(targetRel);

	return SUCCESS;
}
