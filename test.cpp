#include "gridfile.h"
#include "datagenerator.h"

#define NRECORDS 300

int main() {
	int error = 0;
	int x = 0;
	int y = 0;
	int rsize = 0;
	int iter = 0;
	void *record = NULL;

	struct gridfile vgrid;
	struct gridconfig vconfig;
	vconfig.size = 10;
	vconfig.psize = 4096;
	vconfig.name = "test";

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
			cout<<"l1\n";
			free(record);
			goto pclean;
		}

		free(record);
	}

	error = vgrid.findRecord(x, y, &record);
	if (error < 0) {
		cout<<"l2\n";
		goto clean;
	}

	printf("Record: %s\n", (char *)record);

pclean:
	vgrid.unloadGrid();

clean:
	cout<<error<<"\n";
	return error;
}
