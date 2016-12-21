#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include "gridfile.h"
#include "datagenerator.h"

#define SIZE 1000
#define PSIZE 4096
#define NAME "db"
#define NRECORDS 8000000
#define X1 0
#define X2 INT_MAX
#define Y1 0
#define Y2 INT_MAX

int main()
{
	int error = 0;
	int iter = 0;
	int64_t x = 0;
	int64_t y = 0;
	int64_t rsize = 0;
	void *record = NULL;
	struct gridconfig vconfig;
	struct gridfile vgrid;
	int64_t ds = 0;
	int64_t nr = 0;
	time_t start;
	time_t end;
	double elapsed = 0;

	vconfig.size = SIZE;
	vconfig.psize = PSIZE;
	vconfig.name = NAME;

	error = vgrid.createGrid(&vconfig);
	if (error < 0) {
		goto clean;
	}

	error = vgrid.loadGrid();
	if (error < 0) {
		goto clean;
	}

	start = time(NULL);

	for (iter = 0; iter < NRECORDS; iter++) {
		getRandomRecord(&x, &y, &rsize, &record);

		error = vgrid.insertRecord(x, y, record, rsize);
		if (error < 0) {
			goto pclean;
		}

		free(record);
	}

	end = time(NULL);

	elapsed = (double)(end - start);
	printf("Elapsed time: %.2f.\n", elapsed);

	start = time(NULL);

	error = vgrid.findRangeRecords(X1, Y1, X2, Y2, &ds, &record);
	if (error < 0) {
		goto clean;
	}

	end = time(NULL);

	elapsed = (double)(end - start);
	printf("Elapsed time: %.2f.\n", elapsed);
	printf("Records found: %d\n", ((int64_t*)record)[0]);

	free(record);

 pclean:
	vgrid.unloadGrid();

 clean:
	printf("Error: %d\n", error);
	return error;
}
