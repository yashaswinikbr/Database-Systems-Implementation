#include "BigQ.h"
#include <stdlib.h>
using namespace std;


OrderMaker sortingOrder;

class SortRunRecords{

OrderMaker *sortingOrder;

public:
	SortRunRecords(OrderMaker *order)
	{
		sortingOrder = order;
	}

bool operator()(Record* record_1,Record* record_2) {

	ComparisonEngine compEngine;

	if( compEngine.Compare(record_1,record_2,sortingOrder) < 0)
	{
		return true;
	}
	else
	{
		return false;
	}
}
};

class MergeSortedRuns{

	OrderMaker *sortingOrder;

public:
	MergeSortedRuns(OrderMaker *order)
	{
		sortingOrder = order;
	}

bool operator()(Record* record_1,Record* record_2) {

	ComparisonEngine compEngine;

	if(compEngine.Compare(record_1,record_2,sortingOrder) < 0)
	{
		return false;
	}
	else
	{
		return true;
	}
}
};

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	if(runlen<=0)
	{
		out.ShutDown();
		return;
	}
	OrderMaker* sortingOrder=&sortorder;
	map<int,Page*> check_OverFlow;
	int count=0;
	int totalPages=0;
	Record record;
	Record* temp;
	File f;
	Page* p=new (std::nothrow) Page();
	Page* filePage;
	vector<Record*> record_Vector;
	vector<Page*> page_Vector;
	char file[100];
	sprintf(file,"temp%d.bin",rand());
	f.Open(0,file);
	//Tempfile for maintaining sorted run
	while(true) {

		if(in.Remove(&record) && count<runlen) {

			if(!p->Append(&record)) {
				page_Vector.push_back(p);
				p=new (std::nothrow) Page();
				p->Append(&record);
				count++;
			}
		}
		//Got runlen pages, now sort them
		else {

			//Incase of last run, push the large page for sorting
			if(count<runlen && p->GetRecsCount()) {
				page_Vector.push_back(p);
			}

			//Extract records of each run from pages, do an in-memory sort
			for(int j=0;j<page_Vector.size();j++) {
				temp=new (std::nothrow) Record();
				while(page_Vector[j]->GetFirst(temp)) {
					record_Vector.push_back(temp);
					temp=new (std::nothrow) Record();
				}
				delete temp;
				delete  page_Vector[j];
				page_Vector[j]=NULL;
				temp=NULL;
			}

			//Using in-built algorithm for sort
			std::sort(record_Vector.begin(),record_Vector.end(),SortRunRecords(&sortorder));

			//Writing each run into temp file
			int total_Pages=0;
			filePage=new (std::nothrow)Page();
			for(int i=0;i<record_Vector.size();i++) {
				if(!filePage->Append(record_Vector[i])) {
					total_Pages++;
					f.AddPage(filePage,totalPages++);
					filePage->EmptyItOut();
					filePage->Append(record_Vector[i]);
				}
				delete record_Vector[i];
				record_Vector[i]=NULL;
			}

			if(total_Pages<runlen) {
				if(filePage->GetRecsCount())
					f.AddPage(filePage,totalPages++);
			}
			else {
				//Check for overflow page and map the run number to it
				if(filePage->GetRecsCount()) {

					check_OverFlow[totalPages-1]=filePage;
				}
			}
			filePage=NULL;

			record_Vector.clear();
			page_Vector.clear();

			//Appending the record of next-run into buffer page, and setting run-page count to 0
			if (count>=runlen) {
				p->Append(&record);
				count=0;
				continue;
			}
			else {
				break;
			}
		}
	}

	//Parameters for merge step
	off_t fileLength=f.GetLength();
	off_t totalRuns;
	if(fileLength!=0)
		 totalRuns=((ceil)((float)(fileLength-1)/runlen));
	else totalRuns=0;
	off_t previousRunPages=(fileLength-1)-(totalRuns-1)*runlen;

	priority_queue<Record*,vector<Record*>,MergeSortedRuns> priorityQueue(&sortorder);
	//Maintains the pointer to rec_head

	//Maps Record to run (for k-way merge)
	map<Record*,int> records;

	//Maintains page numbers of each run
	int* pageIndex=new (std::nothrow) int[totalRuns];

	//Maintains current buffer page in each run
	Page** pagesArray=new (std::nothrow) Page*[totalRuns];

	//Load first pages into the memory buffer and first record into priority queue
	int pageNumber=0;
	for(int i=0;i<totalRuns;i++) {
		pagesArray[i]=new Page();
		f.GetPage(pagesArray[i],pageNumber);
		pageIndex[i]=1;
		Record* r=new (std::nothrow) Record;
		pagesArray[i]->GetFirst(r);
		priorityQueue.push(r);
		records[r]=i;
		r=NULL;
		pageNumber+=runlen;
	}

	//Extract from priority queue and place on the output pipe
	while(!priorityQueue.empty()) {

		Record* rec=priorityQueue.top();
		//Extract the min priority record
		priorityQueue.pop();

		//run number of record being pushed, -1 sentinel
		int nextRunRecord=-1;

		nextRunRecord=records[rec];
		records.erase(rec);

		if(nextRunRecord == -1) {
			cout<<"Weird! Since element which was mapped before isn't present"<<endl;
			return;
		}

		Record* nextRecord=new (std::nothrow) Record;
		bool foundRecord=true;
		if(!pagesArray[nextRunRecord]->GetFirst(nextRecord)) {
			//Check if not the end of run
			if((!(nextRunRecord==(totalRuns-1)) && pageIndex[nextRunRecord]<runlen)
					|| ((nextRunRecord==(totalRuns-1) && pageIndex[nextRunRecord]<previousRunPages))){

				f.GetPage(pagesArray[nextRunRecord],pageIndex[nextRunRecord]+nextRunRecord*runlen);
				pagesArray[nextRunRecord]->GetFirst(nextRecord);
				pageIndex[nextRunRecord]++;
			}
			else {
				//Check for overflow page incase of last run
				if(pageIndex[nextRunRecord]==runlen)
				 {
					if(check_OverFlow[(nextRunRecord+1)*runlen-1]) {
						delete pagesArray[nextRunRecord];
						pagesArray[nextRunRecord]=NULL;
						pagesArray[nextRunRecord]=(check_OverFlow[(nextRunRecord+1)*runlen-1]);
						check_OverFlow[(nextRunRecord+1)*runlen-1]=NULL;
						pagesArray[nextRunRecord]->GetFirst(nextRecord);
					}
					else foundRecord=false;
				}
				else foundRecord=false;
			}
		}

		//Push the nextRecordrecord into the priority queue if found
		if(foundRecord) {
			priorityQueue.push(nextRecord);
		}
		records[nextRecord] = nextRunRecord;

		out.Insert(rec);
		//Put the min priority record onto the pipe
	}

	f.Close();
	//Tempfile close

	remove(file);
	//Deleting the temp file

	out.ShutDown ();

}

BigQ::~BigQ() {}
