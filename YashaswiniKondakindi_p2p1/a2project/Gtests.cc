#include "DBFile.h"
#include "BigQ.h"
#include <pthread.h>
#include "gtest/gtest.h"
#include "test.h"

relation *reln;
int runlen;
int val = 0;
int inputLength = 0;
int outputLength = 0;

FILE *file;
DBFile newFile;

void *producer (void *arg) {

	Pipe *pipe = (Pipe *) arg;

	Record tempRecord;

	DBFile dbfile;
	dbfile.Open (reln->path ());
	dbfile.MoveFirst ();

	while (dbfile.GetNext (tempRecord) == 1) {
		inputLength += 1;
		pipe->Insert (&tempRecord);
	}

	dbfile.Close ();
	pipe->ShutDown ();
	return nullptr;

}

void *consumer (void *arg) {

	testutil *test = (testutil *) arg;

	ComparisonEngine compEngine;

	DBFile dbfile;

	Record record[2];
	Record *lastRecord = NULL, *prevRecord = NULL;

	while (test->pipe->Remove (&record[outputLength%2]))
	{
		prevRecord = lastRecord;
		lastRecord = &record[outputLength%2];

		if (prevRecord && lastRecord)
		{
			if (compEngine.Compare (prevRecord, lastRecord, test->order) == 1)
			{
				val++;
			}
		}
		outputLength++;
	}
return nullptr;
}

TEST (BigQClassTest, TestRunLength)
{
	OrderMaker sortOrder;
	int bufferSize = 100; // pipe cache size
	Pipe inputPipe (bufferSize);
	Pipe outputPipe (bufferSize);

	//Checking whether the output contains zero records when the run length is less than 0
	BigQ bq (inputPipe, outputPipe, sortOrder, -20);
	EXPECT_EQ(outputLength,0);
		//Output would have zero records because the file would be closed if the runlen less than or equal to 0
}

/*TEST (BigQClassTest, CheckSorting) {
	// sort order for records
	OrderMaker sortOrder;
	reln->get_sort_order (sortOrder);
	int bufferSize = 100; // pipe cache size
	Pipe inputPipe (bufferSize);
	Pipe outputPipe (bufferSize);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&inputPipe);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tesUtil = {&outputPipe, &sortOrder, false, false};

  pthread_create (&thread2, NULL, consumer, (void *)&tesUtil);

  BigQ bq (inputPipe, outputPipe, sortOrder, runlen);

  pthread_join (thread1, NULL);
  pthread_join (thread2, NULL);

	EXPECT_EQ(0,val);
}*/

TEST (BigQClassTest, CheckNumberOfRecords)
{
	EXPECT_EQ(inputLength,outputLength);
}

TEST(BigQClassTest, InsertAndRemoveFromPipe)
{
    Pipe input (100);
    int hasRecords;
    Record temp, getRecord;
    int recordCounter = 0;
    DBFile db;
		char *dbfile_dir = "bin/nation.bin";
    db.Open (dbfile_dir);
    db.MoveFirst ();
    while (db.GetNext (temp) == 1) {
        recordCounter += 1;
        if (recordCounter%100000 == 0) {
             cerr << " producer: " << recordCounter << endl;
        }
        input.Insert(&temp);
    }

    hasRecords = input.Remove(&getRecord);
    db.Close ();
    input.ShutDown ();
    EXPECT_TRUE(hasRecords == 1);
}

TEST( BigQClassTest, CheckIfBinFileEmpty)
{
    int isNotEmpty;
    Record temp;
    int counter = 0;

    DBFile dbfile;
		char *dbfile_dir = "bin/nation.bin";
    dbfile.Open (dbfile_dir);
    dbfile.MoveFirst ();
    isNotEmpty = dbfile.GetNext (temp);
    EXPECT_TRUE(isNotEmpty == 1);
}

// In this gtest, creation of new file is tested and returns 1 if successfully created using create method
TEST (gtest, testDBFileCreate) {
	EXPECT_EQ(1,newFile.Create (reln->path(), heap, NULL));
	newFile.Close ();
}

// In this gtest, opening a created file returns 1 if success and 0 if failure
TEST (gtest, testDBFileOpen) {
	newFile.Create (reln->path(), heap, NULL);
	EXPECT_EQ(1,newFile.Open (reln->path()));
	newFile.Close ();
}

// In this gtest, closing an opened file is tested. returns 1 if success and 0 on failure
TEST (gtest, testDBFileClose) {
	newFile.Create (reln->path(), heap, NULL);
	newFile.Open (reln->path());
	EXPECT_EQ(1,newFile.Close ());
}

int main(int argc, char **argv) {

    testing::InitGoogleTest(&argc, argv);
  	int table = 0;
    setup();
    relation *table_p[] = {n, r, c, p, ps, o, li};

    while (table <= 0 || table >= 8) {
      cout << "\n SELECT table Name:"<<endl;
      cout << "1)Nation"<<endl;
      cout << "2)Region"<<endl;
      cout << "3)Customer"<<endl;
      cout << "4)Part"<<endl;
      cout << "5)Partsupp"<<endl;
      cout << "6)Orders"<<endl;
      cout << "7)Lineitem"<<endl;
      cin >> table;
    }

    table--;
    reln = table_p [table];

    cout << "\t\n Enter the Run Length:\n\t ";
    cin >> runlen;

    return RUN_ALL_TESTS();
}
