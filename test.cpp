#include <stdio.h>
#include "gridfile.h"
#include "datagenerator.h"

#define SIZE 1000
#define PSIZE 4096
#define NAME "db"
#define NRECORDS 8000000

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

		error = vgrid.insertRecord(x, y, record, rsize);
		if (error < 0) {
			goto pclean;
		}

		free(record);
	}

 pclean:
	vgrid.unloadGrid();

 clean:
	printf("Error: %d\n", error);
	return error;
}
