#include <iostream>
#include "datagenerator.h"

void getRandomString(int *length, char **rands) {
	int r = rand() + 1;
	int offset = r % rslength;

	*length = rslength - offset + 1;
	*rands = (char *)malloc(*length);

	memcpy(*rands, rstring + offset, *length);
}

void getRandomRecord(int *x, int *y, int *rsize, void **record) {
	*x = rand();
	*y = rand();

	getRandomString(rsize, (char **)record);
}
