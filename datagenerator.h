#ifndef DATAGENERATOR_HPP
#define DATAGENERATOR_HPP

#include <stdlib.h>
#include <string.h>

using namespace std;

static const char rstring[] = "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

static int rslength = sizeof(rstring) -1;

void getRandomString(int *length, char **rands);
void getRandomRecord(int *x, int *y, int *rsize, void **record);

#endif
