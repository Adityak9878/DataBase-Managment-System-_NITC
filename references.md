# Basic Terminologies

### Attributes
Attribute is just a property, the types a value can have is called a **domain**.

### Records
Records refer to a collection of **attributes**. This is just a *row* in the database.

### Relation
Relation is a collection of **records**. Thus, it refers to the *entire table* in the database.



# Disk Model

## The Disk Class

### `Disk::readBlock`
- **Args:** `buffer` (denotes the buffer into which data should be brought) and `blockNum` (which disk block should be brought in)

### `Disk::writeBlock`
- **Args:** `buffer` (denotes the data which should be written) and `blockNum` (denotes the block to which data should be written)

## Record Block
1. `REC` : denotes the type of block
2. `PBlock` : the parent block
3. `LBlock` : the left block
4. `RBlock` : the right block
5. `#Entries` : number of entries in the block
6. `#Attrs` : number of attributes of each record
7. `#Slots` : number of slots possible for record entries
8. `Res` : reserved
9. `SlotMap` : which slots are empty and which are occupied

## Relation Catalog
Relation Catalog is used for storing meta-information of the relations in a database. Each entry in the Relation Catalog has a size of 96 bytes and has the following six attributes:
1. `RelName` 
2. `#Attributes`
3. `#Records`
4. `FirstBlock` 
5. `LastBlock`
6. `#Slots` 

## Attribute Catalog
Attribute Catalog stores meta-information regarding all the attributes of each relation. NITCbase allows Attribute Catalog to be stored over multiple blocks arranged as a linked list.

1. `RelName`
2. `AttributeName`
3. `AttributeType`
4. `PrimaryFlag`
5. `RootBlock`
6. `Offset`

# Caches
Both **Relation Cache**  and **Attribute Cache** are arrays of size 12 (ie. `MAX_OPEN`).

## Relation Cache
An entry in the relation cache stores the **relation catalog entry**, the **record id (block# and slot#)** on the disk and some other *run-time data*.

## Attribute Cache
An entry in the attribute cache is a *linked list* where each node contains one of the **attribute catalog entry**, **record id** and some other *run-time meta data*.  