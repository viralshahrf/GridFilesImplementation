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

string fname = "grid";

int createFile(int64_t size, string fname, const char *mode)
{
	int error = 0;
	FILE *f = NULL;

	f = fopen(fname.c_str(), mode);
	if (f == NULL) {
		error = -errno;
		goto clean;
	}

	error = fseek(f, size - 1, SEEK_SET);
	if (error) {
		error = -errno;
		goto pclean;
	}

	fputc('\0', f);

	error = fseek(f, 0, SEEK_SET);
	if (error) {
		error = -errno;
		goto pclean;
	}

 pclean:
	fclose(f);

 clean:
	return error;
}

int createGrid(int64_t size, string fname)
{
	int error = 0;
	int64_t ssize = (2 * size + 1) * 8;
	int64_t dsize = (size * size) * 5 * 8 + 8;
	int64_t bsize = (size * size) * 4096;
	string sname = fname + "scale";
	string dname = fname + "directory";
	string bname = fname + "buckets";
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

	*saddr = (double)size;
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

 clean:
	return error;
}

void getGridLocation(double *gs, int64_t * lon, int64_t * lat, double x,
		     double y)
{
	int64_t size = (int64_t) gs[0];
	double xint = 0;
	double yint = 0;
	double *xpart = NULL;
	double *ypart = NULL;
	int64_t iter = 0;

	xint = gs[1];
	yint = gs[size + 1];

	xpart = gs + 2;
	ypart = gs + 1 + size + 1;

	*lon = 0;
	while (iter < xint && x > xpart[iter]) {
		*lon = ++iter;
	}

	iter = 0;
	*lat = 0;
	while (iter < yint && y > ypart[iter]) {
		*lat = ++iter;
	}
}

int insertGridPartition(double *gs, int lon, double partition)
{
	int error = 0;
	int64_t size = (int64_t) gs[0];
	double ints = 0;
	double *inta = NULL;
	double *part = NULL;
	int64_t iter = 0;
	int64_t ipart = 0;

	if (lon) {
		ints = gs[1];
		inta = gs + 1;
		part = gs + 2;
	} else {
		ints = gs[1 + size];
		inta = gs + 1 + size;
		part = gs + 1 + size + 1;
	}

	if (ints >= size - 1) {
		error = -ENOMEM;
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
	}

	close(sfd);

 clean:
	return error;
}

void unmapGridScale(double *gscale, int64_t size)
{
	int64_t ssize = (2 * size + 1) * 8;

	munmap(gscale, ssize);
	gscale = NULL;
}

int mapGridDirectory(double **gdirectory, string fname, int64_t size)
{
	int error = 0;
	int64_t dsize = (size * size) * 5 * 8 + 8;
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
	}

	close(dfd);

 clean:
	return error;
}

void unmapGridDirectory(double *gdirectory, int64_t size)
{
	int64_t dsize = (size * size) * 5 * 8 + 8;

	munmap(gdirectory, dsize);
	gdirectory = NULL;
}

int getGridEntry(int64_t lon, int64_t lat, double **gentry, double *gdirectory,
		 int64_t size)
{
	int error = 0;
	int64_t offset = 1;

	if (lon >= size || lat >= size) {
		error = -EINVAL;
		goto clean;
	}

	offset += lon * size * 5 + lat * 5;

	*gentry = gdirectory + offset;

 clean:
	return error;
}

int splitGrid(double *gdirectory, double *gscale, int vertical, int64_t lon,
	      int64_t lat, double x, double y)
{
	int error = 0;
	int64_t size = (int64_t) gscale[0];
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

	nrecords = ge[1];

	if (!vertical) {
		average = ge[3];
		average = (average * nrecords + y) / (nrecords + 1);

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

				memcpy(cge, pge, 40);
			}
		}
	} else {
		average = ge[2];
		average = (average * nrecords + x) / (nrecords + 1);

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

				memcpy(cge, pge, 40);
			}
		}
	}

	error = insertGridPartition(gscale, vertical, average);

 clean:
	return error;
}

int mapGridBucket(double *gentry, double **gbucket)
{
	int error = 0;
	string bname = fname + "buckets";
	int bfd = -1;
	double baddr = gentry[4];
	double boffset = baddr * 4096;

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
	}

	close(bfd);

 clean:
	return error;
}

void unmapGridBucket(double *gbucket)
{
	munmap(gbucket, 4096);
	gbucket = NULL;
}

void appendBucketEntry(double *gbucket, double x, double y, int64_t rsize,
		       void *record)
{
	double nbytes = 0;
	int64_t boffset = 0;
	double *bentry = NULL;

	nbytes = gbucket[0];
	boffset = 16 + nbytes;
	bentry = (double *)((char *)gbucket + boffset);

	bentry[0] = x;
	bentry[1] = y;
	bentry[2] = (double)rsize;
	memcpy(bentry + 3, record, rsize);

	gbucket[0] += double (24 + rsize);
	gbucket[1] += 1;
}

int insertGridRecord(double *gentry, double x, double y, void *record,
		     int64_t rsize)
{
	int error = 0;
	double nbytes = gentry[0];
	double nrecords = gentry[1];
	double avgx = gentry[2];
	double avgy = gentry[3];
	double esize = 24 + rsize;
	double capacity = 4096 - 16 - nbytes;
	double *gbucket = NULL;

	if (esize > capacity) {
		error = -ENOMEM;
		goto clean;
	}

	error = mapGridBucket(gentry, &gbucket);
	if (error < 0) {
		goto clean;
	}

	appendBucketEntry(gbucket, x, y, rsize, record);

	gentry[2] = (avgx * nrecords + x) / (nrecords + 1);
	gentry[3] = (avgy * nrecords + y) / (nrecords + 1);
	gentry[1] += 1;
	gentry[0] += double (24 + rsize);

	unmapGridBucket(gbucket);

 clean:
	return error;
}

int getBucketEntry(double **bentry, double *gbucket, int64_t entry)
{
	int error = 0;
	double nrecords = gbucket[1];
	char *be = (char *)gbucket + 16;
	double *cbe = NULL;
	double rsize = 0;
	int64_t iter = 0;

	if (entry >= nrecords) {
		error = -EINVAL;
		goto clean;
	}

	for (iter = 0; iter < entry; iter++) {
		cbe = (double *)be;
		rsize = cbe[2];
		be += int64_t(24 + rsize);
	}

	*bentry = (double *)be;

 clean:
	return error;
}

int deleteBucketEntry(double *gbucket, int64_t entry)
{
	int error = 0;
	double nbytes = gbucket[0];
	double nrecords = gbucket[1];
	double cbytes = nbytes;
	double *cbe = NULL;
	double *nbe = NULL;
	int64_t rsize = 0;
	int64_t iter = 0;

	if (entry >= nrecords) {
		error = -EINVAL;
		goto clean;
	}

	for (iter = 0; iter < entry; iter++) {
		error = getBucketEntry(&cbe, gbucket, iter);
		if (error < 0) {
			goto clean;
		}

		cbytes -= (24 + cbe[2]);
	}

	error = getBucketEntry(&cbe, gbucket, entry);
	if (error < 0) {
		goto clean;
	}

	rsize = (int64_t) cbe[2];
	cbytes -= (24 + rsize);
	nbe = (double *)((char *)cbe + 24 + rsize);

	memmove(cbe, nbe, cbytes);

	gbucket[0] -= (double)(24 + rsize);
	gbucket[1] -= 1;

 clean:
	return error;
}

int getGridPartitions(double *gscale, double *x, double *y, int64_t lon,
		      int64_t lat)
{
	int error = 0;
	int64_t size = (int64_t) gscale[0];
	double xint = gscale[1];
	double yint = gscale[1 + size];
	int64_t xp = 0;
	int64_t yp = 0;

	if (lon > xint || lat > yint) {
		error = -EINVAL;
		goto clean;
	}

	xp = lon - 1 < 0 ? 0 : lon - 1;
	yp = lat - 1 < 0 ? 0 : lat - 1;

	*x = gscale[2 + xp];
	*y = gscale[2 + size + yp];

 clean:
	return error;
}

int splitBucket(double *gscale, double *gdirectory, int vertical, int64_t slon,
		int64_t slat, int64_t dlon, int64_t dlat)
{
	int error = 0;
	double *sge = NULL;
	double *dge = NULL;
	double *sb = NULL;
	double *db = NULL;
	int64_t size = (int64_t) gscale[0];
	double xint = gscale[1];
	double yint = gscale[1 + size];
	double avgx = 0;
	double avgy = 0;
	int64_t iter = 0;
	double *cbe = NULL;
	double ssx = 0;
	double ssy = 0;
	double dsx = 0;
	double dsy = 0;
	double sn = 0;
	double dn = 0;
	double sbytes = 0;
	double dbytes = 0;

	if (slon > xint || slat > yint || dlon > xint || dlat > yint) {
		error = -EINVAL;
		goto clean;
	}

	error = getGridEntry(slon, slat, &sge, gdirectory, size);
	if (error < 0) {
		goto clean;
	}

	error == getGridEntry(dlon, dlat, &dge, gdirectory, size);
	if (error < 0) {
		goto clean;
	}

	error = getGridPartitions(gscale, &avgx, &avgy, dlon, dlat);
	if (error < 0) {
		goto clean;
	}

	error = mapGridBucket(sge, &sb);
	if (error < 0) {
		goto clean;
	}

	error = mapGridBucket(dge, &db);
	if (error < 0) {
		unmapGridBucket(sge);
		goto clean;
	}

	sbytes = sge[0];
	sn = sge[1];
	ssx = sge[2] * sn;
	ssy = sge[3] * sn;

	dbytes = dge[0];
	dn = dge[1];
	dsx = dge[2] * dn;
	dsy = dge[3] * dn;

	if (vertical) {
		while (iter < sn) {
			error = getBucketEntry(&cbe, sb, iter);
			if (error < 0) {
				goto pclean;
			}

			if (cbe[0] > avgx) {
				appendBucketEntry(db, cbe[0], cbe[1], cbe[2],
						  cbe + 3);

				sbytes -= (24 + cbe[2]);
				dbytes += (24 + cbe[2]);
				sn--;
				dn++;
				ssx -= cbe[0];
				ssy -= cbe[1];
				dsx += cbe[0];
				dsy += cbe[1];

				error = deleteBucketEntry(sb, iter);
				if (error < 0) {
					goto pclean;
				}
			} else {
				iter++;
			}
		}
	} else {
		while (iter < sn) {
			error = getBucketEntry(&cbe, sb, iter);
			if (error < 0) {
				goto pclean;
			}

			if (cbe[1] > avgy) {
				appendBucketEntry(db, cbe[0], cbe[1], cbe[2],
						  cbe + 3);

				sbytes -= (24 + cbe[2]);
				dbytes += (24 + cbe[2]);
				sn--;
				dn++;
				ssx -= cbe[0];
				ssy -= cbe[1];
				dsx += cbe[0];
				dsy += cbe[1];

				error = deleteBucketEntry(sb, iter);
				if (error < 0) {
					goto pclean;
				}
			} else {
				iter++;
			}
		}
	}

	sge[0] = sbytes;
	sge[1] = sn;
	sge[2] = ssx / sn;
	sge[3] = ssy / sn;

	dge[0] = dbytes;
	dge[1] = dn;
	dge[2] = dsx / dn;
	dge[3] = dsy / dn;

 pclean:
	unmapGridBucket(sb);
	unmapGridBucket(db);

 clean:
	return error;
}

int hasPairedBucket(int *isPaired, int *vertical, double *ge, double *gd,
		    int64_t lon, int64_t lat, int64_t size)
{
	int error = 0;
	*isPaired = 0;
	*vertical = -1;
	double *xge = NULL;
	double *yge = NULL;

	if (lon > 0) {
		error = getGridEntry(lon - 1, lat, &xge, gd, size);
		if (error < 0) {
			goto clean;
		}

		if (ge[4] == xge[4]) {
			*isPaired = 1;
			*vertical = 1;
		}
	}

	if (lat > 0) {
		error = getGridEntry(lon, lat - 1, &yge, gd, size);
		if (error < 0) {
			goto clean;
		}

		if (ge[4] == yge[4]) {
			*isPaired = 1;
			*vertical = 0;
		}
	}

 clean:
	return error;
}

int insertRecord(double *gs, double *gd, double x, double y, void *record,
		 int64_t rsize)
{
	int error = 0;
	int64_t size = (int64_t) gs[0];
	int64_t lon = 0;
	int64_t lat = 0;
	double *ge = NULL;
	double nbytes = 0;
	double nrecords = 0;
	double capacity = 0;
	double esize = 24 + rsize;
	int isPaired = -1;
	int vertical = -1;
	int split;
	double xint = gs[1];
	double yint = gs[1 + size];
	split = xint == yint ? 1 : 0;

	getGridLocation(gs, &lon, &lat, x, y);

	error = getGridEntry(lon, lat, &ge, gd, size);
	if (error < 0) {
		goto clean;
	}

	nbytes = ge[0];
	nrecords = ge[1];
	capacity = 4096 - 16 - nbytes;

	if (esize <= capacity) {
		error = insertGridRecord(ge, x, y, record, rsize);
		if (error < 0) {
			goto clean;
		}
	} else {
		error =
		    hasPairedBucket(&isPaired, &vertical, ge, gd, lon, lat,
				    size);
		if (error < 0) {
			goto clean;
		}

		if (isPaired) {
			ge[0] = 0;
			ge[1] = 0;
			ge[2] = 0;
			ge[3] = 0;
			ge[4] = gd[0];
			gd[0] += 1;

			if (vertical) {
				error =
				    splitBucket(gs, gd, vertical, lon - 1, lat,
						lon, lat);
				if (error < 0) {
					goto clean;
				}
			} else {
				error =
				    splitBucket(gs, gd, vertical, lon, lat - 1,
						lon, lat);
				if (error < 0) {
					goto clean;
				}
			}
		} else {
			error = splitGrid(gd, gs, split, lon, lat, x, y);
			if (error < 0) {
				goto clean;
			}
		}

		error = insertRecord(gs, gd, x, y, record, rsize);
	}

 clean:
	return error;
}

int findRecord(double *gs, double *gd, double x, double y, void *record)
{
	int error = 0;
	int found = 0;
	int64_t size = (int64_t) gs[0];
	int64_t lon = 0;
	int64_t lat = 0;
	double *ge = NULL;
	double *gb = NULL;
	double nrecords = 0;
	int64_t iter = 0;
	double *be = NULL;
	double bex = 0;
	double bey = 0;
	double rsize = 0;
	double raddr = 0;

	getGridLocation(gs, &lon, &lat, x, y);

	error = getGridEntry(lon, lat, &ge, gd, size);
	if (error < 0) {
		goto clean;
	}

	error = mapGridBucket(ge, &gb);
	if (error < 0) {
		goto clean;
	}

	nrecords = gb[1];

	for (iter = 0; iter < nrecords; iter++) {
		error = getBucketEntry(&be, gb, iter);
		if (error < 0) {
			goto pclean;
		}

		bex = be[0];
		bey = be[1];
		rsize = (int64_t) be[2];

		if (bex == x && bey == y) {
			found = 1;
			memcpy(record, be + 3, rsize);
			break;
		}
	}

 pclean:
	unmapGridBucket(gb);

 clean:
	if (!found) {
		error = -EINVAL;
	}

	return error;
}

int main()
{
	int error = 0;
	double *gs = NULL;
	double *gd = NULL;
	double *ge = NULL;
	double *sge = NULL;
	char data[] = "data";

	error = createGrid(5, fname);
	if (error < 0) {
		goto clean;
	}

	error = mapGridScale(&gs, fname, 5);
	if (error < 0) {
		goto clean;
	}

	error = mapGridDirectory(&gd, fname, 5);
	if (error < 0) {
		goto clean;
	}

	error = insertGridPartition(gs, 1, 2.5);
	if (error < 0) {
		goto clean;
	}

	error = getGridEntry(0, 0, &ge, gd, 5);
	if (error < 0) {
		goto clean;
	}

	error = getGridEntry(1, 0, &sge, gd, 5);
	if (error < 0) {
		goto clean;
	}

	sge[0] = 0;
	sge[1] = 0;
	sge[2] = 0;
	sge[3] = gd[0];

	error = insertGridRecord(ge, 1, 2, data, sizeof(data));
	if (error < 0) {
		goto clean;
	}

	error = insertGridRecord(ge, 2, 3, data, sizeof(data));
	if (error < 0) {
		goto clean;
	}

	error = insertGridRecord(ge, 3, 4, data, sizeof(data));
	if (error < 0) {
		goto clean;
	}

	error = insertGridRecord(ge, 4, 5, data, sizeof(data));
	if (error < 0) {
		goto clean;
	}

	error = insertGridRecord(ge, 5, 6, data, sizeof(data));
	if (error < 0) {
		goto clean;
	}

	error = splitBucket(gs, gd, 1, 0, 0, 1, 0);
	if (error < 0) {
		goto clean;
	}

	unmapGridDirectory(gd, 5);
	unmapGridScale(gs, 5);

 clean:
	cout << "error: " << error << "\n";
	return error;
}
