#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <iostream>
#include <string>

using namespace std;

int createFile(int64_t size, string fname, const char *mode)
{
	int error = 0;
	int w = 0;
	int wzero = size;
	FILE *f = NULL;

	f = fopen(fname.c_str(), mode);
	if (f == NULL) {
		error = -errno;
		goto clean;
	}

	error = fseek(f, size - 1, SEEK_SET);
	if (error) {
		error = -errno;
		fclose(f);
		goto clean;
	}

	fputc('\0', f);

	error = fseek(f, 0, SEEK_SET);
	if (error) {
		error = -errno;
		fclose(f);
		goto clean;
	}

	fclose(f);

 clean:
	return error;
}

int createGrid(int64_t size, string fname)
{
	int error = 0;
	FILE *f = NULL;
	int ssize = (2 * size + 1) * 8;
	int dsize = (size * size) * 4 * 8;
	int bsize = (size * size) * 4096;
	string sname = fname + "scale";
	string dname = fname + "directory";
	string bname = fname + "buckets";
	string rname = fname + "records";
	int sfd = -1;
	int64_t *saddr = NULL;

	error = createFile(ssize, sname, "w");
	if (error < 0) {
		goto clean;
	}

	sfd = open(sname.c_str(), O_RDWR | O_CREAT);
	if (sfd == -1) {
		error = -errno;
		goto clean;
	}

	saddr =
	    (int64_t *) mmap(NULL, ssize, PROT_READ | PROT_WRITE, MAP_SHARED,
			     sfd, 0);
	if (*saddr == -1) {
		error = -errno;
		fclose(f);
		goto clean;
	}

	saddr[0] = size;
	munmap(saddr, ssize);

	error = createFile(dsize, dname, "w");
	if (error < 0) {
		goto clean;
	}

	error = createFile(bsize, bname, "w");
	if (error < 0) {
		goto clean;
	}

	f = fopen(rname.c_str(), "w");
	if (f == NULL) {
		error = -errno;
		goto clean;
	}

	fclose(f);

 clean:
	return error;
}

int main()
{
	return createGrid(5, "grid");
}
