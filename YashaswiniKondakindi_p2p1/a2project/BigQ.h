#ifndef BIGQ_H
#define BIGQ_H
#include <math.h>
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include <vector>
#include <map>
#include <queue>
#include "Pipe.h"
#include <algorithm>
#include <iostream>


using namespace std;
class ComparisonEngine;

typedef struct {
	Pipe *in;
	Pipe *out;
	OrderMaker* sortingOrder;
	int run_len;
} bigq_util;


class BigQ  {
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};


#endif
