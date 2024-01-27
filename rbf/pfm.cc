#include "pfm.h"

#include <iostream>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	FILE * file;
	if(exists(fileName)){	
		return -1;
	}
	file = fopen (fileName.c_str(),"wb");	
	if(file==NULL){
		perror("Create file failure!");
		return -1;
	}
	if (fclose(file) != 0){
		perror("Close file failure!");
		return -1;	
	}

	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if(remove(fileName.c_str()) != 0){ 		
	    perror( "Error deleting file" );
	    return -1;
	}
	return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(!exists(fileName)) return -1;		
	FILE * file = fopen(fileName.c_str(),"rb+wb");
	if(file==NULL) return -1;				
	if(fileHandle.getFilePtr() != NULL){	
		perror("The fileHandle is being used");
		return -1;
	}
	if(fileHandle.getFileName() != ""){	
		perror("The fileHandle has already been connected with other files");
		return -1;
	}
	fileHandle.setFileName(fileName);
	fileHandle.setFilePtr(file);		

	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE* fp = fileHandle.getFilePtr();	
    if (fclose(fp) != 0)	return -1;
    fileHandle.setFilePtr(NULL);		
	return 0;
}


FileHandle::FileHandle(): fileName(""),filePtr(NULL)
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;

}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum >= getNumberOfPages()){
		perror("Read page not exist");
		return -1;
	}
	FILE* fp = getFilePtr();
	if(fseek(fp,pageNum*PAGE_SIZE,SEEK_SET)!=0){
		perror("Seek page error");
		return -1;
	}
	fread(data,sizeof(char),PAGE_SIZE,fp);
	if(ferror(fp)){
		perror("readPage Failure!");
		return -1;
	}
	readPageCounter = readPageCounter + 1;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageNum >= getNumberOfPages()){
		perror("Write page not exist");
		return -1;
	}
	FILE* fp = getFilePtr();
	if(fseek(fp,pageNum*PAGE_SIZE,SEEK_SET)!=0){
		perror("Seek page error");
		return -1;
	}
	fwrite(data,sizeof(char),PAGE_SIZE,fp);
	if(ferror(fp)){
		perror("writePage Failure!");
		return -1;
	}
	writePageCounter = writePageCounter + 1;
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
    FILE* fp = getFilePtr();
    fseek(fp,0,SEEK_END);
	fwrite(data,sizeof(char),PAGE_SIZE,fp);
    if(ferror(fp)){
    	perror("appendPage Failure!");
    	return -1;
    }
	appendPageCounter = appendPageCounter + 1;
	return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	FILE* fp = getFilePtr();
	if(fp == NULL){
		perror("File has not been open!");
		return -1;
	}
	long fileSize;

	fseek (fp, 0, SEEK_END);  
	fileSize = ftell (fp);
	rewind(fp);
	unsigned pageNum = fileSize / PAGE_SIZE;

    return pageNum;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;

	return 0;
}