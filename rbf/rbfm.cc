#include "rbfm.h"
#include "pfm.h"
#include <stdio.h>
#include <string.h>
#include<cmath>
#include<iostream>
using namespace std;
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* _pf_manager = PagedFileManager::instance();


RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	if(_pf_manager->createFile(fileName)!=0){
		perror("Create file failure!");
		return -1;
	}
	pagedirectory_num = 0;
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	_pf_manager->destroyFile(fileName);
    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	if(_pf_manager->openFile(fileName,fileHandle)!=0){
		perror("Open file failure!");
		return -1;
	}
	if(pagedirectory_num==0){
		PageDirectory pd;
		pd.encodePageDirectory();
		fileHandle.appendPage(pd.buffer);
		pagedirectory_num = 1;
		current_pagedirectory = 0;
	}
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	_pf_manager->closeFile(fileHandle);
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	int page_num = fileHandle.getNumberOfPages();
	Record record(recordDescriptor,data);

	char* pd_tmp = (char*)malloc(PAGE_SIZE);
	fileHandle.readPage(current_pagedirectory,pd_tmp);
	PageDirectory pd(pd_tmp);

	if(pd.num_page == (PAGE_SIZE-2*sizeof(int))/sizeof(PageDirectorySlot)){
		pd.next_page = page_num;
		pd.encodePageDirectory();

		fileHandle.writePage(current_pagedirectory,pd.buffer);
		PageDirectory pd1;
		pd = pd1;
		pd.num_page = 0;
		pd.encodePageDirectory();

		fileHandle.appendPage(pd.buffer);
		pagedirectory_num += 1;
		current_pagedirectory = page_num;
		page_num ++;

	}
	if(page_num==pagedirectory_num){
		Page page;
		Slot slot;
		slot.record_offset = 0;
		slot.record_lenght = record.total_size;
		rid.pageNum = page_num;
		rid.slotNum = page.slot_directory.size();
		page.slot_directory.push_back(slot);
		record.rid = rid;
		page.records.push_back(record);
		page.free_ptr += record.total_size;
		page.encodePage();

		fileHandle.appendPage(page.buffer);
		PageDirectorySlot pagedirectoryslot;
		pagedirectoryslot.free_byte = PAGE_SIZE-record.total_size-2*sizeof(int)-sizeof(Slot);
		pagedirectoryslot.page_id = 1;
		pd.num_page = 1;
		pd.pds.push_back(pagedirectoryslot);
		pd.encodePageDirectory();
		fileHandle.writePage(current_pagedirectory,pd.buffer);

	}
	else{

		int find = 0;
		char* tmp = (char*)malloc(PAGE_SIZE);
		fileHandle.readPage(0,tmp);
		PageDirectory tmp_pd(tmp);
		int next_page = 0;
		while(1){
			int i=0;

			for(i=0; i< tmp_pd.pds.size(); i++){
				if(record.total_size+sizeof(Slot) <= tmp_pd.pds[i].free_byte){
					char* page_buffer = (char*)malloc(PAGE_SIZE);
					fileHandle.readPage(tmp_pd.pds[i].page_id,page_buffer);
					Page page(page_buffer,recordDescriptor);
					Slot slot;
					slot.record_offset = page.free_ptr;
					slot.record_lenght = record.total_size;
					rid.pageNum = tmp_pd.pds[i].page_id;
					rid.slotNum = page.slot_directory.size();
					page.slot_directory.push_back(slot);
					record.rid = rid;
					page.records.push_back(record);
					page.free_ptr += record.total_size;
					page.encodePage();

					fileHandle.writePage(tmp_pd.pds[i].page_id,page.buffer);
					tmp_pd.pds[i].free_byte -= record.total_size+sizeof(Slot);
					tmp_pd.encodePageDirectory();
					fileHandle.writePage(next_page,tmp_pd.buffer);
					find = 1;
					break;
				}
			}

			if(find ==1)
				break;
			if(tmp_pd.next_page==-1)
				break;
			else{
				next_page = tmp_pd.next_page;
				fileHandle.readPage(next_page,tmp);

				PageDirectory tmp_pd1(tmp);
				tmp_pd = tmp_pd1;

			}
		}

		if(find == 0)
		{

			Page page;
			Slot slot;
			slot.record_offset = 0;
			slot.record_lenght = record.total_size;
			rid.pageNum = page_num;
			rid.slotNum = page.slot_directory.size();
			page.slot_directory.push_back(slot);
			record.rid = rid;
			page.records.push_back(record);
			page.free_ptr += record.total_size;
			page.encodePage();

			fileHandle.appendPage(page.buffer);
			PageDirectorySlot pagedirectoryslot;
			pagedirectoryslot.free_byte = PAGE_SIZE-record.total_size-2*sizeof(int)-sizeof(Slot);
			pagedirectoryslot.page_id = page_num;
			pd.num_page += 1;
			pd.pds.push_back(pagedirectoryslot);
			pd.encodePageDirectory();
			fileHandle.writePage(current_pagedirectory,pd.buffer);
		}
	}

	free(pd_tmp);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	char* page_buffer = (char*)malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum,page_buffer);

	Page page(page_buffer,recordDescriptor);
	Slot slot;
	memcpy(&slot,&(page.slot_directory[rid.slotNum]),sizeof(Slot));

	Record record(recordDescriptor,page.buffer,slot);
	record.decodeRecord(data);

	free(page_buffer);

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int offset = 0;
	int null_bit = ceil(1.0*recordDescriptor.size()/8);
	unsigned char* null_indicator = (unsigned char*)malloc(null_bit);
	memcpy(null_indicator,data,null_bit);
	if(null_indicator==NULL){
		perror("Read null bit error!");
		return -1;
	}

	offset = null_bit;
	int null_byte_num;
	int null_bit_num;
	for(int i=0;i<recordDescriptor.size();i++){
		null_byte_num = i/8;
		null_bit_num = i%8;
		if(null_indicator[null_byte_num] & (1 << (7-null_bit_num))){
			cout << recordDescriptor[i].name << ":" << NULL << endl;
		}
		else{
			switch(recordDescriptor[i].type){
			case TypeInt:
			{
				int int_attr;
				memcpy(&int_attr,(char*)data+offset,recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				cout << recordDescriptor[i].name << ":" << int_attr << endl;
				break;
			}
			case TypeReal:
			{
				float real_attr;
				memcpy(&real_attr,(char*)data+offset,recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				cout << recordDescriptor[i].name << ":" << real_attr << endl;
				break;
			}
			case TypeVarChar:
			{
				int attr_len;
				memcpy(&attr_len,(char*)data+offset,4*sizeof(char));
				offset += 4*sizeof(char);
				char* char_attr = (char*)malloc(attr_len);
				memcpy(char_attr,(char*)data+offset,attr_len);
				offset += attr_len;
				cout << recordDescriptor[i].name << ":" << char_attr << endl;
				free(char_attr);
				break;
			}
			}
		}
	}

	free(null_indicator);
    return 0;
}


Record::Record(){
}

Record::~Record(){
}

Record::Record(const vector<Attribute> &recordDescriptor, const void *data):field_size(0),total_size(0){
	record_Descriptor = recordDescriptor;
	int offset = 0;
	null_bit = ceil(1.0*recordDescriptor.size()/8);
	null_indicator = (unsigned char*)malloc(null_bit);
	memcpy(null_indicator,data,null_bit);
	if(null_indicator==NULL){
		perror("Read null bit error!");
	}

	offset = null_bit;
	int null_byte_num;
	int null_bit_num;
	for(int i=0;i<recordDescriptor.size();i++){
		null_byte_num = i/8;
		null_bit_num = i%8;
		if(null_indicator[null_byte_num] & (1 << (7-null_bit_num))){
			field_size += recordDescriptor[i].length;
		}
		else{
			switch(recordDescriptor[i].type){
			case TypeInt:
			{
				IntField int_field;
				int_field.attr = recordDescriptor[i];
				memcpy(&int_field.int_value,(char*)data+offset,recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				field_size += recordDescriptor[i].length;
				fields.push_back(int_field);
				break;
			}
			case TypeReal:
			{
				RealField real_field;
				real_field.attr = recordDescriptor[i];
				memcpy(&real_field.real_value,(char*)data+offset,recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				field_size += recordDescriptor[i].length;
				fields.push_back(real_field);
				break;
			}
			case TypeVarChar:
			{
				VarCharField varchar_field;
				varchar_field.attr = recordDescriptor[i];
				int attr_len;
				memcpy(&attr_len,(char*)data+offset,4*sizeof(char));
				varchar_field.attr.length = attr_len;
				offset += 4*sizeof(char);
				varchar_field.varchar_value = (char*)malloc(attr_len);
				memcpy(varchar_field.varchar_value,(char*)data+offset,attr_len);
				offset += attr_len;
				field_size += attr_len;
				fields.push_back(varchar_field);
				break;
			}
			}
		}
		total_size = null_bit + field_size + 4*record_Descriptor.size();
	}

}
Record::Record(const vector<Attribute> &recordDescriptor,char* page_buffer, Slot slot):field_size(0),total_size(0){
	record_Descriptor = recordDescriptor;
	buffer = (char*)malloc(slot.record_lenght);

	memcpy(buffer,page_buffer+slot.record_offset,slot.record_lenght);

	int offset = 0;
	null_bit = ceil(1.0*recordDescriptor.size()/8);
	null_indicator = (unsigned char*)malloc(null_bit);
	memcpy(null_indicator,buffer,null_bit);
	if(null_indicator==NULL){
		perror("Read null bit error!");
	}

	offset = null_bit;
	offset += recordDescriptor.size()*sizeof(int);
	int null_byte_num;
	int null_bit_num;
	for(int i=0;i<recordDescriptor.size();i++){

		null_byte_num = i/8;
		null_bit_num = i%8;
		if(null_indicator[null_byte_num] & (1 << (7-null_bit_num))){
			offset += recordDescriptor[i].length;
			field_size += recordDescriptor[i].length;
		}
		else{
			switch(recordDescriptor[i].type){
			case TypeInt:
			{
				IntField int_field;
				int_field.attr = recordDescriptor[i];
				memcpy(&int_field.int_value,buffer+*(int*)(buffer+null_bit+4*i),recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				field_size += recordDescriptor[i].length;
				fields.push_back(int_field);
				break;
			}
			case TypeReal:
			{

				RealField real_field;
				real_field.attr = recordDescriptor[i];
				memcpy(&real_field.real_value,buffer+*(int*)(buffer+null_bit+4*i),recordDescriptor[i].length);
				offset += recordDescriptor[i].length;
				field_size += recordDescriptor[i].length;
				fields.push_back(real_field);
				break;
			}
			case TypeVarChar:
			{

				VarCharField varchar_field;
				varchar_field.attr = recordDescriptor[i];
				int attr_len;
				if(i==recordDescriptor.size()-1)
					attr_len = slot.record_lenght - offset;
				else
					attr_len = *(int*)(buffer+null_bit+4*i+4) - offset;
				varchar_field.attr.length = attr_len;
				varchar_field.varchar_value = (char*)malloc(attr_len);
				memcpy(varchar_field.varchar_value,buffer+*(int*)(buffer+null_bit+4*i),attr_len);
				offset += attr_len;
				field_size += attr_len;

				fields.push_back(varchar_field);
				break;
			}
			}

		}

		total_size = null_bit + field_size + 4*record_Descriptor.size();
	}
}



RC Record::encodeRecord(){
	int offset = 0;

	if(sizeof(*buffer)!=0)
		buffer = (char*)malloc(total_size);

	memcpy(buffer+offset,null_indicator,null_bit);

	offset += null_bit;

	offset += 4*record_Descriptor.size();
	int null_byte_num;
	int null_bit_num;
	int null_num = 0;

	for(int i=0;i<record_Descriptor.size();i++){
		null_byte_num = i/8;
		null_bit_num = i%8;
		if(null_indicator[null_byte_num] & (1 << (7-null_bit_num))){

			null_num++;
			offset += record_Descriptor[i].length;

		}
		else{


		switch(fields[i-null_num].attr.type){
		case TypeInt:
		{
			memcpy(buffer+offset,&(fields[i-null_num].int_value),fields[i-null_num].attr.length);

			memcpy(buffer+null_bit+4*i,&offset,sizeof(int));
			offset += fields[i-null_num].attr.length;

			break;
		}
		case TypeReal:
		{
			memcpy(buffer+offset,&(fields[i-null_num].real_value),fields[i-null_num].attr.length);
			memcpy(buffer+null_bit+4*i,&offset,sizeof(int));
			offset += fields[i-null_num].attr.length;
			break;
		}
		case TypeVarChar:
		{
			memcpy(buffer+offset,fields[i-null_num].varchar_value,fields[i-null_num].attr.length);
			memcpy(buffer+null_bit+4*i,&offset,sizeof(int));
			offset += fields[i-null_num].attr.length;
			break;
		}
		}
		}
	}
	return 0;
}

RC Record::decodeRecord(void* return_data){
	int offset = 0;
	memcpy((char*)return_data+offset,null_indicator,null_bit);
	offset += null_bit;
	for(int i=0; i<fields.size(); i++){
		switch(fields[i].attr.type){
		case TypeInt:
		{
			memcpy((char*)return_data+offset,&fields[i].int_value,sizeof(int));
			offset += sizeof(int);
			break;
		}
		case TypeReal:
		{
			memcpy((char*)return_data+offset,&fields[i].real_value,sizeof(float));
			offset += sizeof(float);
			break;
		}
		case TypeVarChar:
		{
			memcpy((char*)return_data+offset,&fields[i].attr.length,sizeof(int));
			offset += sizeof(int);
			memcpy((char*)return_data+offset,fields[i].varchar_value,fields[i].attr.length);
			offset += fields[i].attr.length;
			break;
		}
	}
	}
	return 0;
}

Page::Page(){
	buffer = (char*)malloc(PAGE_SIZE);
	free_ptr = 0;

};
Page::~Page(){
}

Page::Page(char* tmp,vector<Attribute> record_Descriptor){
	buffer = (char*)malloc(PAGE_SIZE);
	memcpy(buffer,tmp,PAGE_SIZE);
	int offset = PAGE_SIZE-sizeof(int);
	memcpy(&free_ptr, buffer+offset,sizeof(int));
	offset -= sizeof(int);
	int rec_num;
	memcpy(&rec_num, buffer+offset,sizeof(int));
	for(int i=0;i<rec_num;i++){
		Slot slot;
		offset -= sizeof(Slot);

		memcpy(&slot,buffer+offset,sizeof(Slot));
		slot_directory.push_back(slot);
		Record record(record_Descriptor,buffer,slot);
		records.push_back(record);

	}

}

RC Page::encodePage(){
	int offset = PAGE_SIZE-sizeof(int);
	memcpy(buffer+offset,&free_ptr,sizeof(int));
	offset -= sizeof(int);
	int rec_num = records.size();
	memcpy(buffer+offset,&rec_num,sizeof(int));
	for(int i=0; i<slot_directory.size();i++){
		records[i].encodeRecord();
		memcpy(buffer+slot_directory[i].record_offset,records[i].buffer,records[i].total_size);
		if(slot_directory[i].record_offset+records[i].total_size > free_ptr)
			free_ptr = slot_directory[i].record_offset+records[i].total_size;

		offset -= sizeof(Slot);
		memcpy(buffer+offset,&slot_directory[i],sizeof(Slot));
	}

	return 0;
}

PageDirectory::PageDirectory(char* tmp){
	buffer = (char*)malloc(PAGE_SIZE);
	memcpy(buffer,tmp,PAGE_SIZE);
	int offset = 0;
	memcpy(&next_page,buffer+offset,sizeof(int));
	offset += sizeof(int);
	memcpy(&num_page,buffer+offset,sizeof(int));
	offset += sizeof(int);
	for(int i=0;i< num_page;i++){
		PageDirectorySlot pagedirectoryslot;
		memcpy(&pagedirectoryslot,buffer+offset,sizeof(PageDirectorySlot));
		offset += sizeof(PageDirectorySlot);
		pds.push_back(pagedirectoryslot);
	}
}

RC PageDirectory::encodePageDirectory(){
	int offset = 0;
	memcpy(buffer+offset,&next_page,sizeof(int));
	offset += sizeof(int);
	memcpy(buffer+offset,&num_page,sizeof(int));
	offset += sizeof(int);
	for(int i=0; i<pds.size(); i++){
		memcpy(buffer+offset,&pds[i],sizeof(PageDirectorySlot));
		offset += sizeof(PageDirectorySlot);
	}
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    // Get page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData) != 0)
        return RBFM_READ_FAILED;

    // Get page header
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if (slotHeader.recordEntriesNumber <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Get slot record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    SlotStatus status = getSlotStatus(recordEntry);
    // Cannot delete a deleted page
    if (status == DEAD)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }
    // Recursively delete moved pages
    else if (status == MOVED)
    {
        RID newRid;
        newRid.pageNum = recordEntry.length;
        newRid.slotNum = -recordEntry.offset;
        RC rc = deleteRecord(fileHandle, recordDescriptor, newRid);
        if (rc != 0)
        {
            free(pageData);
            return rc;
        }
        markSlotDeleted(pageData, rid.slotNum);
    }
    else if (status == VALID)
    {
        markSlotDeleted(pageData, rid.slotNum);
        reorganizePage(pageData);
    }
    
    // Once we've deleted the page(s), write changes to disk
    RC rc = fileHandle.writePage(rid.pageNum, pageData);
    free(pageData);
    return rc;
}

void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

SlotStatus RecordBasedFileManager::getSlotStatus(SlotDirectoryRecordEntry slot)
{
    if (slot.length == 0 && slot.offset == 0)
        return DEAD;
    if (slot.offset <= 0)
        return MOVED;
    return VALID;
}
// Mark slot header as dead (all 0s)
void RecordBasedFileManager::markSlotDeleted(void *page, unsigned i)
{
    memset  (
            ((char*) page + sizeof(SlotDirectoryHeader) + i * sizeof(SlotDirectoryRecordEntry)),
            0,
            sizeof(SlotDirectoryRecordEntry)
            );
}

void RecordBasedFileManager::reorganizePage(void *page)
{
    SlotDirectoryHeader header = getSlotDirectoryHeader(page);

    // Add all live records to vector, keeping track of slot numbers
    vector<IndexedRecordEntry> liveRecords;
    for (unsigned i = 0; i < header.recordEntriesNumber; i++)
    {
        IndexedRecordEntry entry;
        entry.slotNum = i;
        entry.recordEntry = getSlotDirectoryRecordEntry(page, i);
        if (getSlotStatus(entry.recordEntry) == VALID)
            liveRecords.push_back(entry);
    }
    // Sort records by offset, descending
    auto comp = [](IndexedRecordEntry first, IndexedRecordEntry second) 
        {return first.recordEntry.offset > second.recordEntry.offset;};
    sort(liveRecords.begin(), liveRecords.end(), comp);

    // Move each record back filling in any gap preceding the record
    uint16_t pageOffset = PAGE_SIZE;
    SlotDirectoryRecordEntry current;
    for (unsigned i = 0; i < liveRecords.size(); i++)
    {
        current = liveRecords[i].recordEntry;
        pageOffset -= current.length;

        // Use memmove rather than memcpy because locations may overlap
        memmove((char*)page + pageOffset, (char*)page + current.offset, current.length);
        current.offset = pageOffset;
        setSlotDirectoryRecordEntry(page, liveRecords[i].slotNum, current);
    }
    header.freeSpaceOffset = pageOffset;
    setSlotDirectoryHeader(page, header);
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    // Retrieve the specific page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
    {
        free(pageData);
        return RBFM_READ_FAILED;
    }

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber <= rid.slotNum)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    SlotStatus status = getSlotStatus(recordEntry);
    switch (status)
    {
        // Error to update a deleted record
        case DEAD:
            free(pageData);
            return RBFM_READ_AFTER_DEL;
        // Get the forwarding address from the record entry and recurse
        case MOVED:
            free(pageData);
            RID newRid;
            newRid.pageNum = recordEntry.length;
            newRid.slotNum = -recordEntry.offset;
            return updateRecord(fileHandle, recordDescriptor, data, newRid);
        default:
        break;
    }
    // Do actual work
    // Gets the size of the updated record
    unsigned recordSize = getRecordSize(recordDescriptor, data);
    if (recordSize  == recordEntry.length)
    {
        setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
        RC rc = fileHandle.writePage(rid.pageNum, pageData);
        free(pageData);
        return rc;
    }
    else if (recordSize < recordEntry.length)
    {
        setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
        recordEntry.length = recordSize;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        reorganizePage(pageData);
        RC rc = fileHandle.writePage(rid.pageNum, pageData);
        free(pageData);
        return rc;
    }
    else if (recordSize > recordEntry.length)
    {
        unsigned space = getPageFreeSpaceSize(pageData) + recordEntry.length;
        if (recordSize > space)
        {
            // Need to insert then set forward address then reorganize
            RID newRid;
            RC rc = insertRecord(fileHandle, recordDescriptor, data, newRid);
            if (rc != 0)
            {
                free(pageData);
                return rc;
            }
            recordEntry.length = newRid.pageNum;
            recordEntry.offset = -newRid.slotNum;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            reorganizePage(pageData);
        }
        else
        {
            // Need to set header to DEAD and reorganize to consolidate free space
            recordEntry.length = 0;
            recordEntry.offset = 0;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            reorganizePage(pageData);

            // Get updated slotHeader with new free space pointer
            slotHeader = getSlotDirectoryHeader(pageData);
            // Update record length and offset
            recordEntry.length = recordSize;
            recordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

            // Update header with new free space pointer
            slotHeader.freeSpaceOffset = recordEntry.offset;
            setSlotDirectoryHeader(pageData, slotHeader);

            // Add new record data
            setRecordAtOffset (pageData, recordEntry.offset, recordDescriptor, data);
        }
    }
    RC rc = fileHandle.writePage(rid.pageNum, pageData);
    free(pageData);
    return rc;
}

void RecordBasedFileManager::getRecordAtOffset(void *page, int32_t offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, (char*)page + offset, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), nullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    char *pageData = (char*)malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(rid.pageNum, pageData) != 0)
    {
        free(pageData);
        return RBFM_READ_FAILED;
    }
    // Get record header, recurse if forwarded
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    SlotStatus status = getSlotStatus(recordEntry);
    switch (status)
    {
        // Error to get attribute of a deleted record
        case DEAD:
            free(pageData);
            return RBFM_READ_AFTER_DEL;
        // Get the forwarding address from the record entry and recurse
        case MOVED:
            free(pageData);
            RID newRid;
            newRid.pageNum = recordEntry.length;
            newRid.slotNum = -recordEntry.offset;
            return readAttribute(fileHandle, recordDescriptor, newRid, attributeName, data);
        default:
        break;
    }

    // Get offset to record
    unsigned offset = recordEntry.offset;
    // Get index and type of attribute
    auto pred = [&](Attribute a) {return a.name == attributeName;};
    auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
    unsigned index = distance(recordDescriptor.begin(), iterPos);
    if (index == recordDescriptor.size())
        return RBFM_NO_SUCH_ATTR;
    AttrType type = recordDescriptor[index].type;
    // Write attribute to data
    getAttributeFromRecord(pageData, offset, index, type, data);
    free(pageData);
    return 0;
}

int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

void RecordBasedFileManager::getAttributeFromRecord(void *page, unsigned offset, unsigned attrIndex, AttrType type, void *data)
{
    char *start = (char*)page + offset;
    unsigned data_offset = 0;

    // Get number of columns
    RecordLength n;
    memcpy (&n, start, sizeof(RecordLength));

    // Get null indicator
    int recordNullIndicatorSize = getNullIndicatorSize(n);
    char recordNullIndicator[recordNullIndicatorSize];
    memcpy (recordNullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // Set null indicator for result
    char resultNullIndicator = 0;
    if (fieldIsNull(recordNullIndicator, attrIndex))
        resultNullIndicator |= (1 << 7);
    memcpy(data, &resultNullIndicator, 1);
    data_offset += 1;
    if (resultNullIndicator) return;

    // Now we know the result isn't null, so we grab it
    unsigned header_offset = sizeof(RecordLength) + recordNullIndicatorSize;
    // attrEnd points to end of attribute, attrStart points to the beginning
    // Our directory at the beginning of each record contains pointers to the ends of each attribute,
    // so we can pull attrEnd from that
    ColumnOffset attrEnd, attrStart;
    memcpy(&attrEnd, start + header_offset + attrIndex * sizeof(ColumnOffset), sizeof(ColumnOffset));
    // The start is either the end of the previous attribute, or the start of the data section of the
    // record if we are after the 0th attribute
    if (attrIndex > 0)
        memcpy(&attrStart, start + header_offset + (attrIndex - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
    else
        attrStart = header_offset + n * sizeof(ColumnOffset);
    // The length of any attribute is just the difference between its start and end
    uint32_t len = attrEnd - attrStart;
    if (type == TypeVarChar)
    {
        // For varchars we have to return this length in the result
        memcpy((char*)data + data_offset, &len, sizeof(VARCHAR_LENGTH_SIZE));
        data_offset += VARCHAR_LENGTH_SIZE;
    }
    // For all types, we then copy the data into the result
    memcpy((char*)data + data_offset, start + attrStart, len);
}

RBFM_ScanIterator::RBFM_ScanIterator()
: currPage(0), currSlot(0), totalPage(0), totalSlot(0)
{
    rbfm = RecordBasedFileManager::instance();
}

RC RBFM_ScanIterator::close()
{
    free(pageData);
    return 0;
}

 RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
    return rbfm_ScanIterator.scanInit(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

RC RBFM_ScanIterator::scanInit(FileHandle &fh,
        const vector<Attribute> rd,
        const string &ca, 
        const CompOp co, 
        const void *v, 
        const vector<string> &an)
{
    // Start at page 0 slot 0
    currPage = 0;
    currSlot = 0;
    totalPage = 0;
    totalSlot = 0;
    // Keep a buffer to hold the current page
    pageData = malloc(PAGE_SIZE);

    // Store the variables passed in to
    fileHandle = fh;
    conditionAttribute = ca;
    recordDescriptor = rd;
    compOp = co;
    value = v;
    attributeNames = an;

    skipList.clear();

    // Get total number of pages
    totalPage = fh.getNumberOfPages();
    if (totalPage > 0)
    {
        if (fh.readPage(0, pageData))
            return RBFM_READ_FAILED;
    }
    else
        return 0;

    // Get number of slots on first page
    SlotDirectoryHeader header = rbfm->getSlotDirectoryHeader(pageData);
    totalSlot = header.recordEntriesNumber;

    // If we don't need to do any comparisons, we can ignore the condition attribute
    if (co == NO_OP)
        return 0;

    // Else, we need to find the condition attribute's index in the record descriptor
    auto pred = [&](Attribute a) {return a.name == conditionAttribute;};
    auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
    attrIndex = distance(recordDescriptor.begin(), iterPos);
    if (attrIndex == recordDescriptor.size())
        return RBFM_NO_SUCH_ATTR;

    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    RC rc = getNextSlot();
    if (rc)
        return rc;

    // If we are not returning any results, we can just set the RID and return
    if (attributeNames.size() == 0)
    {
        rid.pageNum = currPage;
        rid.slotNum = currSlot++;
        return 0;
    }

    // Prepare null indicator
    unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(attributeNames.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currSlot);

    // Unsure how large each attribute will be, set to size of page to be safe
    void *buffer = malloc(PAGE_SIZE);
    if (buffer == NULL)
        return RBFM_MALLOC_FAILED;

    // Keep track of offset into data
    unsigned dataOffset = nullIndicatorSize;

    for (unsigned i = 0; i < attributeNames.size(); i++)
    {
        // Get index and type of attribute in record
        auto pred = [&](Attribute a) {return a.name == attributeNames[i];};
        auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
        unsigned index = distance(recordDescriptor.begin(), iterPos);
        if (index == recordDescriptor.size())
            return RBFM_NO_SUCH_ATTR;
        AttrType type = recordDescriptor[index].type;

        // Read attribute into buffer
        rbfm->getAttributeFromRecord(pageData, recordEntry.offset, index, type, buffer);
        // Determine if null
        char null;
        memcpy (&null, buffer, 1);
        if (null)
        {
            int indicatorIndex = i / CHAR_BIT;
            char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
            nullIndicator[indicatorIndex] |= indicatorMask;
        }
        // Read from buffer into data
        else if (type == TypeInt)
        {
            memcpy ((char*)data + dataOffset, (char*)buffer + 1, INT_SIZE);
            dataOffset += INT_SIZE;
        }
        else if (type == TypeReal)
        {
            memcpy ((char*)data + dataOffset, (char*)buffer + 1, REAL_SIZE);
            dataOffset += REAL_SIZE;
        }
        else if (type == TypeVarChar)
        {
            uint32_t varcharSize;
            memcpy(&varcharSize, (char*)buffer + 1, VARCHAR_LENGTH_SIZE);
            memcpy((char*)data + dataOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
            dataOffset += VARCHAR_LENGTH_SIZE;
            memcpy((char*)data + dataOffset, (char*)buffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
            dataOffset += varcharSize;
        }
    }
    // Finally set null indicator of data, clean up and return
    memcpy((char*)data, nullIndicator, nullIndicatorSize);

    free (buffer);
    rid.pageNum = currPage;
    rid.slotNum = currSlot++;
    return 0;
}

// Private helper methods ///////////////////////////////////////////////////////////////////

RC RBFM_ScanIterator::getNextSlot()
{
    // If we're done with the current page, or we've read the last page
    if (currSlot >= totalSlot || currPage >= totalPage)
    {
        // Reinitialize the current slot and increment page number
        currSlot = 0;
        currPage++;
        // If we're done with last page, return EOF
        if (currPage >= totalPage)
            return RBFM_EOF;
        // Otherwise get next page ready
        RC rc = getNextPage();
        if (rc)
            return rc;
    }

    // Get slot header, check to see if valid and meets scan condition
    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currSlot);

    if (rbfm->getSlotStatus(recordEntry) != VALID || !checkScanCondition())
    {
        // If not, try next slot
        currSlot++;
        return getNextSlot();
    }
    return 0;
}

RC RBFM_ScanIterator::getNextPage()
{
    // Read in page
    if (fileHandle.readPage(currPage, pageData))
        return RBFM_READ_FAILED;

    // Update slot total
    SlotDirectoryHeader header = rbfm->getSlotDirectoryHeader(pageData);
    totalSlot = header.recordEntriesNumber;
    return 0;
}

bool RBFM_ScanIterator::checkScanCondition()
{
    if (compOp == NO_OP) return true;
    if (value == NULL) return false;
    Attribute attr = recordDescriptor[attrIndex];
    // Allocate enough memory to hold attribute and 1 byte null indicator
    void *data = malloc(1 + attr.length);
    // Get record entry to get offset
    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currSlot);
    // Grab the given attribute and store it in data
    rbfm->getAttributeFromRecord(pageData, recordEntry.offset, attrIndex, attr.type, data);

    char null;
    memcpy(&null, data, 1);

    bool result = false;
    if (null)
    {
        result = false;
    }
    // Checkscan condition on record data and scan value
    else if (attr.type == TypeInt)
    {
        int32_t recordInt;
        memcpy(&recordInt, (char*)data + 1, INT_SIZE);
        result = checkScanCondition(recordInt, compOp, value);
    }
    else if (attr.type == TypeReal)
    {
        float recordReal;
        memcpy(&recordReal, (char*)data + 1, REAL_SIZE);
        result = checkScanCondition(recordReal, compOp, value);
    }
    else if (attr.type == TypeVarChar)
    {
        uint32_t varcharSize;
        memcpy(&varcharSize, (char*)data + 1, VARCHAR_LENGTH_SIZE);
        char recordString[varcharSize + 1];
        memcpy(recordString, (char*)data + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
        recordString[varcharSize] = '\0';

        result = checkScanCondition(recordString, compOp, value);
    }
    free (data);
    return result;
}

bool RBFM_ScanIterator::checkScanCondition(int recordInt, CompOp compOp, const void *value)
{
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool RBFM_ScanIterator::checkScanCondition(float recordReal, CompOp compOp, const void *value)
{
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool RBFM_ScanIterator::checkScanCondition(char *recordString, CompOp compOp, const void *value)
{
    if (compOp == NO_OP)
        return true;

    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int cmp = strcmp(recordString, valueStr);
    switch (compOp)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        // Should never happen
        default: return false;
    }
}
