
#include "rm.h"

#include <algorithm>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor())
{
}

RelationManager::~RelationManager()
{
}
RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Create the Tables table file
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    // Create the Columns table file
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;

    // Insert entry for the Tables table
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;

    // Insert entry for the Columns table
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;

    // Insert entries for tables and columns into Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;

    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Delete the Tables table file
    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    // Delete the Columns table file
    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Create the file to store the table
    rc = rbfm->createFile(getFileName(tableName));
    if (rc)
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Check if the table is a system table; if it is, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open the Tables table
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find the entry with the same table ID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete the entry from the Tables table
    rc = rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    if (rc)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Open the Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find and delete all entries from the Columns table associated with this table
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    while ((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Clear out any old values
    attrs.clear();
    RC rc;

    // Get the table ID for the given tableName
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // Prepare the projection for scanning the Columns table
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second) 
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get the record descriptor for the table
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Open the file handle for the table
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let RecordBasedFileManager handle the insertion
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get the record descriptor for the table
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Open the file handle for the table
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let RecordBasedFileManager handle the deletion
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get the record descriptor for the table
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Open the file handle for the table
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let RecordBasedFileManager handle the update
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get the record descriptor for the table
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Open the file handle for the table
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let RecordBasedFileManager read the record
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let RecordBasedFileManager handle the printing
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get the record descriptor for the table
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Open the file handle for the table
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let RecordBasedFileManager read the attribute
    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    // Create attributes for the Tables table
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    // Create attributes for the Columns table
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}
// Creates the Tables table entry for the given id, system flag, and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len; 
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id, position, and attribute
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}
RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Open the Tables table file
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Prepare the data to be inserted into the Tables table
    void *tableData = malloc(TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);

    // Insert the record into the Tables table
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    // Close the file and free memory
    rbfm->closeFile(fileHandle);
    free(tableData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    // Open the Tables table file
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Projection to retrieve only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get the largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    // Initialize variables
    RID rid;
    void *data = malloc(1 + INT_SIZE);
    int32_t max_table_id = 0;

    // Iterate over records to find the maximum table ID
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse out the table ID, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }

    // If we reached the end of file, consider it a success
    if (rc == RM_EOF)
        rc = SUCCESS;

    // Free memory and set the next table ID
    free(data);
    table_id = max_table_id + 1;

    // Close the file and iterator
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    // Open the Tables table file
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Projection to retrieve only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Prepare the value for comparison
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char *)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // Initialize variables
    RID rid;
    void *data = malloc(1 + INT_SIZE);

    // If found, extract the table ID
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    // Free memory and close the file and iterator
    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return rc;
}
// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    // Open the Tables table file
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc(1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    // Free memory and close the file and iterator
    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;   
}

// Convert string to API format
void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*)data + 1, &len, INT_SIZE);
    memcpy((char*)data + 1 + INT_SIZE, str.c_str(), len);
}

// Convert integer to API format
void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*)data + 1, &integer, INT_SIZE);
}

// Convert float to API format
void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*)data + 1, &real, REAL_SIZE);
}

// Convert API format to string
void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*)data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*)data + 5, len);

    str = string(tmp);
}

// Convert API format to integer
void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*)data + 1, INT_SIZE);

    integer = tmp;
}

// Convert API format to float
void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*)data + 1, REAL_SIZE);
    
    real = tmp;
}
// RM_ScanIterator ///////////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

// Extra credit work (10 points)
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr) {
    // Check if the table exists
    int32_t tableID;
    if (getTableID(tableName, tableID) != SUCCESS) {
        return -1;
    }

    // Check if the attribute already exists
    vector<Attribute> existingAttrs;
    if (getAttributes(tableName, existingAttrs) != SUCCESS) {
        return -1;
    }
    for (const auto &existingAttr : existingAttrs) {
        if (existingAttr.name == attr.name) {
            return -1;
        }
    }

    // Add the attribute to the system catalogs
    if (insertColumns(tableID, {attr}) != SUCCESS) {
        return -1;
    }

    return SUCCESS;
}

RC RelationManager::dropAttribute(const string &tableName, const string &attributeName) {
 
    // Open the Columns file
     RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle columnsFileHandle;
    if (rbfm->openFile(COLUMNS_TABLE_NAME + std::string(TABLE_FILE_EXTENSION), columnsFileHandle) != SUCCESS)
        return -1;

    // Get the table ID
    int32_t tableID;
    if (getTableID(tableName, tableID) != SUCCESS) {
        rbfm->closeFile(columnsFileHandle);
        return -1;
    }

    // Scan the Columns table to find the attribute to be dropped
    RM_ScanIterator rmScanIterator;
    vector<string> attributeNames;
    attributeNames.push_back(COLUMNS_COL_COLUMN_NAME);
    if (scan(COLUMNS_TABLE_NAME, COLUMNS_COL_TABLE_ID, EQ_OP, &tableID, attributeNames, rmScanIterator) != SUCCESS) {
        rbfm->closeFile(columnsFileHandle);
        return -1;
    }

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);
    bool found = false;
    while (rmScanIterator.getNextTuple(rid, data) != RM_EOF) {
        string columnName;
        fromAPI(columnName, data);
        if (columnName == attributeName) {
            found = true;
            break;
        }
    }

    if (!found) {
        free(data);
        rbfm->closeFile(columnsFileHandle);
        return -1;
    }

    // Delete the attribute record
    if (rbfm->deleteRecord(columnsFileHandle, columnDescriptor, rid) != SUCCESS) {
        free(data);
        rbfm->closeFile(columnsFileHandle);
        return -1;
    }

    // Close the files
    free(data);
    rbfm->closeFile(columnsFileHandle);

    return SUCCESS;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	return -1;
}