#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
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
	int64_t ssize = (2 * size + 1) * 8;
	int64_t dsize = (size * size) * 4 * 8;
	int64_t bsize = (size * size) * 4096;
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

	sfd = open(sname.c_str(), O_RDWR);
	if (sfd == -1) {
		error = -errno;
		goto clean;
	}

	saddr =
	    (int64_t *) mmap(NULL, ssize, PROT_READ | PROT_WRITE, MAP_SHARED,
			     sfd, 0);
	if (*saddr == -1) {
		error = -errno;
		close(sfd);
		goto clean;
	}

	saddr[0] = size;
	munmap(saddr, ssize);
	close(sfd);

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

int getGridLocation(int64_t * lon, int64_t * lat, double x, double y,
		    string fname, int64_t size)
{
	int error = 0;
	int sfd = -1;
	double *saddr = NULL;
	int64_t ssize = (2 * size + 1) * 8;
	string sname = fname + "scale";
	int64_t xint = 0;
	int64_t yint = 0;
	double *xpart = NULL;
	double *ypart = NULL;
	int64_t iter = 0;

	sfd = open(sname.c_str(), O_RDWR);
	if (sfd == -1) {
		error = -errno;
		goto clean;
	}

	saddr =
	    (double *)mmap(NULL, ssize, PROT_READ | PROT_WRITE, MAP_SHARED, sfd,
			   0);
	if (*saddr == -1) {
		error = -errno;
		close(sfd);
		goto clean;
	}

	xint = saddr[1];
	yint = saddr[size + 1];

	xpart = saddr + 2;
	ypart = saddr + 1 + size + 1;

	*lon = 0;
	while (iter < xint && x > xpart[iter]) {
		*lon = ++iter;
	}

	iter = 0;
	*lat = 0;
	while (iter < yint && y > ypart[iter]) {
		*lat = ++iter;
	}

	munmap(saddr, ssize);
	close(sfd);

 clean:
	return error;
}

int insertGridPartition(int lon, int partition, string fname, int64_t size)
{
	int error = 0;
	int sfd = -1;
	double *saddr = NULL;
	int64_t ssize = (2 * size + 1) * 8;
	string sname = fname + "scale";
	int64_t ints = 0;
	double *inta = NULL;
	double *part = NULL;
	int64_t iter = 0;
	int64_t ipart = 0;

	sfd = open(sname.c_str(), O_RDWR);
	if (sfd == -1) {
		error = -errno;
		goto clean;
	}

	saddr =
	    (double *)mmap(NULL, ssize, PROT_READ | PROT_WRITE, MAP_SHARED, sfd,
			   0);
	if (*saddr == -1) {
		error = -errno;
		close(sfd);
		goto clean;
	}

	if (lon) {
		ints = saddr[1];
		inta = saddr + 1;
		part = saddr + 2;
	} else {
		ints = saddr[size + 1];
		inta = saddr + size + 1;
		part = saddr + 1 + size + 1;
	}

	if (ints >= size - 1) {
		error = -ENOMEM;
		munmap(saddr, ssize);
		close(sfd);
		goto clean;
	}

	while (iter < ints && partition > part[iter]) {
		iter++;
	}

	ipart = iter;

	for (iter = ints; iter > ipart; iter--) {
		part[iter] = part[iter - 1];
	}

	part[ipart] = partition;
	*inta += 1;

	munmap(saddr, ssize);
	close(sfd);

 clean:
	return error;
}

int getGridEntry(int64_t lon, int64_t lat, double **gentry, string fname,
		 int64_t size)
{
	int error = 0;
	string dname = fname + "directory";
	int64_t gesize = 32;
	int dfd = -1;
	int64_t offset = 0;

	dfd = open(dname.c_str(), O_RDWR);
	if (dfd == -1) {
		error = -errno;
		goto clean;
	}

	offset = lon * size * gesize + lat * gesize;

	*gentry =
	    (double *)mmap(NULL, gesize, PROT_READ | PROT_WRITE, MAP_SHARED,
			   dfd, offset);
	if (**gentry == -1) {
		error = -errno;
		*gentry = NULL;
		close(dfd);
		goto clean;
	}

	close(dfd);

 clean:
	return error;
}

int releaseGridEntry(double *gentry, string fname)
{
	int error = 0;
	string dname = fname + "directory";
	int64_t gesize = 32;
	int dfd = -1;

	dfd = open(dname.c_str(), O_RDWR);
	if (dfd == -1) {
		error = -errno;
		goto clean;
	}

	munmap(gentry, gesize);
	close(dfd);

 clean:
	return error;
}

int main()
{
	return 0;
}
