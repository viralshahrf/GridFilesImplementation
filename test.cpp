#include <stdio.h>
#include <errno.h>
#include "gridfile.h"
#include "datagenerator.h"

#define SIZE 1000
#define PSIZE 4096
#define NAME "db"
#define NRECORDS 8000000
#define FRECORD 666
#define X1 1123456789
#define X2 1234567891
#define Y1 1345678912
#define Y2 1456789123

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
	int64_t fx = 0;
	int64_t fy = 0;
	int64_t ds = 0;
	int64_t nr = 0;
	int64_t xiter = 0;
	int64_t yiter = 0;

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

	for (iter = 0; iter < NRECORDS; iter++) {
		getRandomRecord(&x, &y, &rsize, &record);

		if (iter == FRECORD) {
			fx = x;
			fy = y;
		}

		if (x >= X1 && x <= X2 && y >= Y1 && y <= Y2) {
			ds += (24 + rsize);
			nr += 1;
		}

		error = vgrid.insertRecord(x, y, record, rsize);
		if (error < 0) {
			goto pclean;
		}

		free(record);
	}

	printf("Size: %d | Records: %d\n", ds, nr);

	ds = 0;
	nr = 0;
	error = vgrid.findRangeRecords(X1, Y1, X2, Y2, &ds, &record);
	if (error < 0) {
		goto clean;
	}

	nr = ((int64_t *) record)[0];
	free(record);

	printf("Size: %d | Records: %d\n", ds, nr);

	error = vgrid.findRecord(fx, fy, &record);
	if (error < 0) {
		goto clean;
	}

	error = vgrid.deleteRecord(fx, fy);
	if (error < 0) {
		goto clean;
	}

	error = vgrid.findRecord(fx, fy, &record);
	if (error == -EINVAL) {
		error = 0;
	} else if (error == 0) {
		error = -EINVAL;
	}

 pclean:
	vgrid.unloadGrid();

 clean:
	printf("Error: %d\n", error);
	return error;
}
