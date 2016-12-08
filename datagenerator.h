#ifndef DATAGENERATOR_HPP
#define DATAGENERATOR_HPP

#include <stdlib.h>
#include <string.h>

using namespace std;

static const char rstring[] =
    "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

static int64_t rslength = sizeof(rstring) - 1;

void getRandomString(int64_t * length, void **rands);
void getRandomRecord(int64_t * x, int64_t * y, int64_t * rsize, void **record);

#endif
