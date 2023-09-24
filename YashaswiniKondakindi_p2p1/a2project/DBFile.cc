#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <cstring>
#include <iostream>

//Constructor to initialize File and Page objects
DBFile::DBFile () {
  page = new Page ();
  file = new File ();
}

// New file is created at the given file path. If success 1 is returned else 0 is returned.
int DBFile::Create (char *f_path, fType f_type, void *startup) {
  fileType = f_type;
	fileExists = true;
	strcpy(filePath, f_path);
	pageIndex = 0;
	page->EmptyItOut ();
	file->Open (0, filePath);
  if(file->GetLength ()>=0) {
	  return 1;
  }
  return 0;
}

// File at the given path is opened. 1 is returned if success and 0 if failure
int DBFile::Open (char *f_path) {
	fileType = heap;
	strcpy(filePath, f_path);
	pageIndex = 0;
	page->EmptyItOut ();
	file->Open (1, filePath);
  if(file->GetLength ()>=0) {
	  return 1;
  }
  return 0;
}

// Data is loaded from the path given in the input and added to the file
void DBFile::Load (Schema &f_schema, char *loadpath) {
  FILE *openedFile = fopen(loadpath, "r");
	Record record;
	if (openedFile == NULL) {
		cout << "File cannot be opened. There is no File with " << loadpath;
    cout << " \n";
		exit(0);
	}
	pageIndex = 0;
	page->EmptyItOut ();
	while (record.SuckNextRecord (&f_schema, openedFile) == 1)
		Add (record);
  file->AddPage (page, pageIndex++);
	page->EmptyItOut ();
	fclose(openedFile);
}

// File is closed if exists. 1 is returned if success else 0
int DBFile::Close () {
  if (page->GetRecsCount () > 0 && fileExists) {
		file->AddPage (page, pageIndex);
		page->EmptyItOut ();
		fileExists = false;
	}
	if(file->Close ()>=0) {
	  return 1;
  }
  return 0;
}

// The DBFile instance is pointed to the first record in the file
void DBFile::MoveFirst () {
  page->EmptyItOut ();
	pageIndex = 0;
	file->GetPage (page, pageIndex);
}

// Record is appended at the end of the file
void DBFile::Add (Record &rec) {
  if (!(page->Append (&rec))) {
  file->AddPage (page, pageIndex++);
  page->EmptyItOut ();
  page->Append (&rec);
}
}

// next record is fetched which satisfies the selection predicate conjuctive norm form. 1 is returned if founf else 0
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine compareEngine;
	while (GetNext (fetchme)) {
		if (compareEngine.Compare (&fetchme, &literal, &cnf)){
			return 1;
		}
	}
	return 0;
}

// check if there is a record in the existing page else get the record from the next page.
//If no record found 0 is returned
int DBFile::GetNext (Record &fetchme) {
  if (page->GetFirst (&fetchme)) {
		return 1;
	} else {
		if (++pageIndex < file->GetLength () - 1) {
			file->GetPage (page, pageIndex);
			page->GetFirst (&fetchme);
			return 1;
		} else {
			   return 0;
		}
	}
}

//Destructor to delete page and file
DBFile::~DBFile () {
	delete page;
  delete file;
}
