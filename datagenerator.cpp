#include "datagenerator.h"

/* Generates random length substring of rstring
   Parameters:
   length: Length of random string is stored
   rands: Random string is stored
*/
void getRandomString(int64_t * length, void **rands)
{
	int64_t r = rand() + 1;
	int64_t offset = r % rslength;

	*length = rslength - offset + 1;
	*rands = (void *)malloc(*length);

	memcpy(*rands, rstring + offset, *length);
}

/* Generates random record
   Parameters:
   x: Random coordinate (x) of record is stored
   y: Random coordinate (y) of record is stored
   rsize: Random size of record is stored
   record: Random data of record is stored
*/
void getRandomRecord(int64_t * x, int64_t * y, int64_t * rsize, void **record)
{
	*x = rand();
	*y = rand();

	getRandomString(rsize, record);
}
