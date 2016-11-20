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
	int64_t dsize = (size * size) * 4 * 8 + 8;
	int64_t bsize = (size * size) * 4096;
	string sname = fname + "scale";
	string dname = fname + "directory";
	string bname = fname + "buckets";
	string rname = fname + "records";
	int sfd = -1;
	int dfd = -1;
	double *saddr = NULL;
	double *daddr = NULL;

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
	    (double *)mmap(NULL, 8, PROT_READ | PROT_WRITE, MAP_SHARED, sfd, 0);
	if (*saddr == -1) {
		error = -errno;
		close(sfd);
		goto clean;
	}

	*saddr = size;
	munmap(saddr, 8);
	close(sfd);

	error = createFile(dsize, dname, "w");
	if (error < 0) {
		goto clean;
	}

	dfd = open(dname.c_str(), O_RDWR);
	if (dfd == -1) {
		error = -errno;
		goto clean;
	}

	daddr =
	    (double *)mmap(NULL, 8, PROT_READ | PROT_WRITE, MAP_SHARED, dfd, 0);
	if (*daddr == -1) {
		error = -errno;
		close(dfd);
		goto clean;
	}

	*daddr += 1;
	munmap(daddr, 8);
	close(dfd);

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
	double xint = 0;
	double yint = 0;
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
	double ints = 0;
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

int mapGridScale(double **gscale, string fname, int64_t size)
{
	int error = 0;
	int64_t ssize = (2 * size + 1) * 8;
	string sname = fname + "scale";
	int sfd = -1;

	sfd = open(sname.c_str(), O_RDWR);
	if (sfd == -1) {
		error = -errno;
		goto clean;
	}

	*gscale =
	    (double *)mmap(NULL, ssize, PROT_READ | PROT_WRITE, MAP_SHARED, sfd,
			   0);
	if (**gscale == -1) {
		error = -errno;
		close(sfd);
		goto clean;
	}

	close(sfd);

 clean:
	return error;
}

int unmapGridScale(double *gscale, int64_t size)
{
	int error = 0;
	int64_t ssize = (2 * size + 1) * 8;

	if (gscale == NULL) {
		error = -EINVAL;
		goto clean;
	}

	munmap(gscale, ssize);
	gscale = NULL;

 clean:
	return error;
}

int mapGridDirectory(double **gdirectory, string fname, int64_t size)
{
	int error = 0;
	int64_t dsize = (size * size) * 4 * 8 + 8;
	string dname = fname + "directory";
	int dfd = -1;

	dfd = open(dname.c_str(), O_RDWR);
	if (dfd == -1) {
		error = -errno;
		goto clean;
	}

	*gdirectory =
	    (double *)mmap(NULL, dsize, PROT_READ | PROT_WRITE, MAP_SHARED, dfd,
			   0);
	if (**gdirectory == -1) {
		error = -errno;
		close(dfd);
		goto clean;
	}

	close(dfd);

 clean:
	return error;
}

int unmapGridDirectory(double *gdirectory, int64_t size)
{
	int error = 0;
	int64_t dsize = (size * size) * 4 * 8 + 8;

	if (gdirectory == NULL) {
		error = -EINVAL;
		goto clean;
	}

	munmap(gdirectory, dsize);
	gdirectory = NULL;

 clean:
	return error;
}

int getGridEntry(int64_t lon, int64_t lat, double **gentry, double *gdirectory,
		 int64_t size)
{
	int error = 0;
	int64_t offset = 1;

	if (gdirectory == NULL) {
		error = -EINVAL;
		goto clean;
	}

	offset += lon * size * 4 + lat * 4;

	*gentry = gdirectory + offset;

 clean:
	return error;
}

int main()
{
	return 0;
}
