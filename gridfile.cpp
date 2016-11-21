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

int insertGridPartition(int lon, double partition, string fname, int64_t size)
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

int splitGrid(double *gdirectory, double *gscale, string fname, int64_t size,
	      int vertical, int64_t lon, int64_t lat, double x, double y)
{
	int error = 0;
	double *ge = NULL;
	double xint = 0;
	double yint = 0;
	double average = 0;
	double nrecords = 0;
	int64_t xiter = 0;
	int64_t yiter = 0;
	double *cge = NULL;
	double *pge = NULL;

	xint = gscale[1];
	yint = gscale[size + 1];

	if (vertical && xint == size - 1) {
		error = -ENOMEM;
		goto clean;
	}

	if (!vertical && yint == size - 1) {
		error = -ENOMEM;
		goto clean;
	}

	error = getGridEntry(lon, lat, &ge, gdirectory, size);
	if (error < 0) {
		goto clean;
	}

	nrecords = ge[0];

	if (!vertical) {
		average = ge[2];
		average = (average * nrecords + x) / (nrecords + 1);

		for (xiter = 0; xiter <= xint; xiter++) {
			for (yiter = yint + 1; yiter > lat; yiter--) {
				error =
				    getGridEntry(xiter, yiter, &cge, gdirectory,
						 size);
				if (error < 0) {
					goto clean;
				}

				error =
				    getGridEntry(xiter, yiter - 1, &pge,
						 gdirectory, size);
				if (error < 0) {
					goto clean;
				}

				memcpy(cge, pge, 32);
			}
		}
	} else {
		average = ge[1];
		average = (average * nrecords + y) / (nrecords + 1);

		for (yiter = 0; yiter <= yint; yiter++) {
			for (xiter = xint + 1; xiter > lon; xiter--) {
				error =
				    getGridEntry(xiter, yiter, &cge, gdirectory,
						 size);
				if (error < 0) {
					goto clean;
				}

				error =
				    getGridEntry(xiter - 1, yiter, &pge,
						 gdirectory, size);
				if (error < 0) {
					goto clean;
				}

				memcpy(cge, pge, 32);
			}
		}
	}

	error = insertGridPartition(vertical, average, fname, size);
	if (error < 0) {
		goto clean;
	}

 clean:
	return error;
}

int mapGridBucket(double *gentry, double **gbucket, string fname)
{
	int error = 0;
	string bname = fname + "buckets";
	int bfd = -1;
	double baddr = gentry[3];
	double boffset = baddr * 4096;

	if (gentry == NULL) {
		error = -EINVAL;
		goto clean;
	}

	bfd = open(bname.c_str(), O_RDWR);
	if (bfd == -1) {
		error = -errno;
		goto clean;
	}

	*gbucket =
	    (double *)mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, bfd,
			   boffset);
	if (**gbucket == -1) {
		error = -errno;
		close(bfd);
		goto clean;
	}

	close(bfd);

 clean:
	return error;
}

int unmapGridBucket(double *gbucket)
{
	int error = 0;

	if (gbucket == NULL) {
		error = -EINVAL;
		goto clean;
	}

	munmap(gbucket, 4096);
	gbucket = NULL;

 clean:
	return error;
}

int appendRecord(double *aoffset, void *record, int64_t rsize, string fname)
{
	int error = 0;
	string rname = fname + "records";
	int fd = -1;
	int64_t wbytes = -1;

	fd = open(rname.c_str(), O_RDWR);
	if (fd == -1) {
		error = -errno;
		goto clean;
	}

	*aoffset = lseek(fd, 0, SEEK_END);
	cout << *aoffset << "\n";

	while (wbytes < 0) {
		wbytes = write(fd, record, rsize);

		if (wbytes < 0 && !(errno == EAGAIN || errno == EWOULDBLOCK)) {
			error = -errno;
			close(fd);
			goto clean;
		}
	}

	if (wbytes < rsize) {
		error = -ENOMEM;
		close(fd);
		goto clean;
	}

	close(fd);

 clean:
	return error;
}

int appendBucketEntry(double *gbucket, double x, double y, double aoffset)
{
	int error = 0;
	double nrecords = 0;
	int64_t boffset = 0;

	if (gbucket == NULL) {
		error = -EINVAL;
		goto clean;
	}

	nrecords = gbucket[0];
	boffset = 1 + nrecords * 3;

	gbucket[boffset] = x;
	gbucket[boffset + 1] = y;
	gbucket[boffset + 2] = aoffset;

	gbucket[0] += 1;

 clean:
	return error;
}

int insertGridRecord(double *gentry, double x, double y, void *record,
		     int64_t rsize, string fname)
{
	int error = 0;
	int capacity = 170;
	double nrecords = gentry[0];
	double avgx = gentry[1];
	double avgy = gentry[2];
	double *gbucket = NULL;
	double aoffset = 0;
	int64_t wbytes = -1;

	if (nrecords >= capacity) {
		error = -ENOMEM;
		goto clean;
	}

	error = mapGridBucket(gentry, &gbucket, fname);
	if (error < 0) {
		goto clean;
	}

	error = appendRecord(&aoffset, record, rsize, fname);
	if (error < 0) {
		unmapGridBucket(gbucket);
		goto clean;
	}

	error = appendBucketEntry(gbucket, x, y, aoffset);
	if (error < 0) {
		unmapGridBucket(gbucket);
		goto clean;
	}

	gentry[1] = (avgx * nrecords + x) / (nrecords + 1);
	gentry[2] = (avgy * nrecords + y) / (nrecords + 1);
	gentry[0] += 1;

 clean:
	return error;
}

int main()
{
	return 0;
}
