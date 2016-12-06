#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include "gridfile.h"

using namespace std;

int gridfile::createFile(int64_t size, string fname, const char *mode)
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
	}

 pclean:
	fclose(f);

 clean:
	return error;
}

int gridfile::createGrid(int64_t size, int64_t psize, string name)
{
	int error = 0;
	int sfd = -1;
	int dfd = -1;
	double *saddr = NULL;
	double *daddr = NULL;

	gridSize = size;
	pageSize = psize;
	scaleSize = (2 * gridSize + 1) * 8;
	directorySize = (gridSize * gridSize) * 5 * 8 + 8;
	bucketSize = (gridSize * gridSize) * pageSize;
	gridName = name;
	scaleName = name + "scale";
	directoryName = name + "directory";
	bucketName = name + "buckets";
	gridScale = NULL;
	gridDirectory = NULL;

	error = createFile(scaleSize, scaleName, "w");
	if (error < 0) {
		goto clean;
	}

	sfd = open(scaleName.c_str(), O_RDWR);
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

	error = createFile(directorySize, directoryName, "w");
	if (error < 0) {
		goto clean;
	}

	dfd = open(directoryName.c_str(), O_RDWR);
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

	error = createFile(bucketSize, bucketName, "w");
	if (error < 0) {
		goto clean;
	}

 clean:
	return error;
}

void gridfile::getGridLocation(int64_t * lon, int64_t * lat, double x, double y)
{
	int64_t xint = 0;
	int64_t yint = 0;
	double *xpart = NULL;
	double *ypart = NULL;
	int64_t iter = 0;

	xint = (int64_t) gridScale[1];
	yint = (int64_t) gridScale[gridSize + 1];

	xpart = gridScale + 2;
	ypart = gridScale + 1 + gridSize + 1;

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

int gridfile::insertGridPartition(int lon, double partition)
{
	int error = 0;
	int64_t ints = 0;
	double *inta = NULL;
	double *part = NULL;
	int64_t iter = 0;
	int64_t ipart = 0;

	if (lon) {
		ints = (int64_t) gridScale[1];
		inta = gridScale + 1;
		part = gridScale + 2;
	} else {
		ints = (int64_t) gridScale[1 + gridSize];
		inta = gridScale + 1 + gridSize;
		part = gridScale + 1 + gridSize + 1;
	}

	if (ints >= gridSize - 1) {
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

int gridfile::mapGridScale()
{
	int error = 0;
	int sfd = -1;

	sfd = open(scaleName.c_str(), O_RDWR);
	if (sfd == -1) {
		error = -errno;
		goto clean;
	}

	gridScale =
	    (double *)mmap(NULL, scaleSize, PROT_READ | PROT_WRITE, MAP_SHARED,
			   sfd, 0);
	if (*gridScale == -1) {
		error = -errno;
	}

	close(sfd);

 clean:
	return error;
}

void gridfile::unmapGridScale()
{
	munmap(gridScale, scaleSize);
	gridScale = NULL;
}

int gridfile::mapGridDirectory()
{
	int error = 0;
	int dfd = -1;

	dfd = open(directoryName.c_str(), O_RDWR);
	if (dfd == -1) {
		error = -errno;
		goto clean;
	}

	gridDirectory =
	    (double *)mmap(NULL, directorySize, PROT_READ | PROT_WRITE,
			   MAP_SHARED, dfd, 0);
	if (*gridDirectory == -1) {
		error = -errno;
	}

	close(dfd);

 clean:
	return error;
}

void gridfile::unmapGridDirectory()
{
	munmap(gridDirectory, directorySize);
	gridDirectory = NULL;
}

int gridfile::getGridEntry(int64_t lon, int64_t lat, double **gentry)
{
	int error = 0;
	int64_t offset = 1;

	if (lon >= gridSize || lat >= gridSize) {
		error = -EINVAL;
		goto clean;
	}

	offset += lon * gridSize * 5 + lat * 5;

	*gentry = gridDirectory + offset;

 clean:
	return error;
}

int gridfile::splitGrid(int vertical, int64_t lon, int64_t lat, double x,
			double y)
{
	int error = 0;
	double *ge = NULL;
	int64_t xint = 0;
	int64_t yint = 0;
	double average = 0;
	int64_t nrecords = 0;
	int64_t xiter = 0;
	int64_t yiter = 0;
	double *cge = NULL;
	double *pge = NULL;

	xint = (int64_t) gridScale[1];
	yint = (int64_t) gridScale[gridSize + 1];

	if (vertical && xint == gridSize - 1) {
		error = -ENOMEM;
		goto clean;
	}

	if (!vertical && yint == gridSize - 1) {
		error = -ENOMEM;
		goto clean;
	}

	error = getGridEntry(lon, lat, &ge);
	if (error < 0) {
		goto clean;
	}

	nrecords = (int64_t) ge[1];

	if (!vertical) {
		average = ge[3];
		average = (average * nrecords + y) / (nrecords + 1);

		for (xiter = 0; xiter <= xint; xiter++) {
			for (yiter = yint + 1; yiter > lat; yiter--) {
				error = getGridEntry(xiter, yiter, &cge);
				if (error < 0) {
					goto clean;
				}

				error = getGridEntry(xiter, yiter - 1, &pge);
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
				error = getGridEntry(xiter, yiter, &cge);
				if (error < 0) {
					goto clean;
				}

				error = getGridEntry(xiter - 1, yiter, &pge);
				if (error < 0) {
					goto clean;
				}

				memcpy(cge, pge, 40);
			}
		}
	}

	error = insertGridPartition(vertical, average);

 clean:
	return error;
}

int gridfile::mapGridBucket(double *gentry, double **gbucket)
{
	int error = 0;
	int bfd = -1;
	int64_t baddr = (int64_t) gentry[4];
	int64_t boffset = baddr * pageSize;

	bfd = open(bucketName.c_str(), O_RDWR);
	if (bfd == -1) {
		error = -errno;
		goto clean;
	}

	*gbucket =
	    (double *)mmap(NULL, pageSize, PROT_READ | PROT_WRITE, MAP_SHARED,
			   bfd, boffset);
	if (**gbucket == -1) {
		error = -errno;
	}

	close(bfd);

 clean:
	return error;
}

void gridfile::unmapGridBucket(double *gbucket)
{
	munmap(gbucket, pageSize);
	gbucket = NULL;
}

void gridfile::appendBucketEntry(double *gbucket, double x, double y,
				 int64_t rsize, void *record)
{
	int64_t nbytes = (int64_t) gbucket[0];
	int64_t boffset = (int64_t) (16 + nbytes);
	double *bentry = (double *)((char *)gbucket + boffset);

	bentry[0] = x;
	bentry[1] = y;
	bentry[2] = (double)rsize;
	memcpy(bentry + 3, record, rsize);

	gbucket[0] += double (24 + rsize);
	gbucket[1] += 1;
}

int gridfile::insertGridRecord(double *gentry, double x, double y, void *record,
			       int64_t rsize)
{
	int error = 0;
	int64_t nbytes = (int64_t) gentry[0];
	int64_t nrecords = (int64_t) gentry[1];
	double avgx = gentry[2];
	double avgy = gentry[3];
	int64_t esize = 24 + rsize;
	int64_t capacity = pageSize - 16 - nbytes;
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

int gridfile::getBucketEntry(double **bentry, double *gbucket, int64_t entry)
{
	int error = 0;
	int64_t nrecords = (int64_t) gbucket[1];
	char *be = (char *)gbucket + 16;
	double *cbe = NULL;
	int64_t rsize = 0;
	int64_t iter = 0;

	if (entry >= nrecords) {
		error = -EINVAL;
		goto clean;
	}

	for (iter = 0; iter < entry; iter++) {
		cbe = (double *)be;
		rsize = (int64_t) cbe[2];
		be += (24 + rsize);
	}

	*bentry = (double *)be;

 clean:
	return error;
}

int gridfile::deleteBucketEntry(double *gbucket, int64_t entry)
{
	int error = 0;
	int64_t nbytes = (int64_t) gbucket[0];
	int64_t nrecords = (int64_t) gbucket[1];
	int64_t cbytes = nbytes;
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

		cbytes -= (24 + (int64_t) cbe[2]);
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

int gridfile::getGridPartitions(double *x, double *y, int64_t lon, int64_t lat)
{
	int error = 0;
	int64_t xint = (int64_t) gridScale[1];
	int64_t yint = (int64_t) gridScale[1 + gridSize];
	int64_t xp = 0;
	int64_t yp = 0;

	if (lon > xint || lat > yint) {
		error = -EINVAL;
		goto clean;
	}

	xp = lon - 1 < 0 ? 0 : lon - 1;
	yp = lat - 1 < 0 ? 0 : lat - 1;

	*x = gridScale[2 + xp];
	*y = gridScale[2 + gridSize + yp];

 clean:
	return error;
}

int gridfile::updatePairedBuckets(int64_t lon, int64_t lat, int64_t baddr)
{
	int error = 0;
	int64_t xint = (int64_t) gridScale[1];
	int64_t yint = (int64_t) gridScale[1 + gridSize];
	int64_t xiter = 0;
	int64_t yiter = 0;
	double *ge = NULL;
	double *pge = NULL;

	error = getGridEntry(lon, lat, &ge);
	if (error < 0) {
		goto clean;
	}

	for (xiter = 0; xiter < xint; xiter++) {
		error = getGridEntry(xiter, lat, &pge);
		if (error < 0) {
			goto clean;
		}

		if (baddr == (int64_t) pge[4] && xiter != lon) {
			memcpy(pge, ge, 40);
		}
	}

	for (yiter = 0; yiter < yint; yiter++) {
		error = getGridEntry(lon, yiter, &pge);
		if (error < 0) {
			goto clean;
		}

		if (baddr == (int64_t) pge[4] && yiter != lat) {
			memcpy(pge, ge, 40);
		}
	}

 clean:
	return error;
}

int gridfile::splitBucket(int vertical, int64_t slon, int64_t slat,
			  int64_t dlon, int64_t dlat)
{
	int error = 0;
	double *sge = NULL;
	double *dge = NULL;
	double *sb = NULL;
	double *db = NULL;
	int64_t xint = (int64_t) gridScale[1];
	int64_t yint = (int64_t) gridScale[1 + gridSize];
	double avgx = 0;
	double avgy = 0;
	int64_t iter = 0;
	double *cbe = NULL;
	double ssx = 0;
	double ssy = 0;
	double dsx = 0;
	double dsy = 0;
	int64_t sn = 0;
	int64_t dn = 0;
	int64_t sbytes = 0;
	int64_t dbytes = 0;
	int64_t dbaddr = 0;

	if (slon > xint || slat > yint || dlon > xint || dlat > yint) {
		error = -EINVAL;
		goto clean;
	}

	error = getGridEntry(slon, slat, &sge);
	if (error < 0) {
		goto clean;
	}

	error == getGridEntry(dlon, dlat, &dge);
	if (error < 0) {
		goto clean;
	}

	dge[0] = 0;
	dge[1] = 0;
	dge[2] = 0;
	dge[3] = 0;
	dbaddr = (int64_t) dge[4];
	dge[4] = gridDirectory[0];
	gridDirectory[0] += 1;

	error = getGridPartitions(&avgx, &avgy, dlon, dlat);
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

	sbytes = (int64_t) sge[0];
	sn = (int64_t) sge[1];
	ssx = sge[2] * sn;
	ssy = sge[3] * sn;

	dbytes = (int64_t) dge[0];
	dn = (int64_t) dge[1];
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

				sbytes -= (24 + (int64_t) cbe[2]);
				dbytes += (24 + (int64_t) cbe[2]);
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

				sbytes -= (24 + (int64_t) cbe[2]);
				dbytes += (24 + (int64_t) cbe[2]);
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

	sge[0] = (double)sbytes;
	sge[1] = (double)sn;
	sge[2] = ssx / sn;
	sge[3] = ssy / sn;

	dge[0] = (double)dbytes;
	dge[1] = (double)dn;
	dge[2] = dsx / dn;
	dge[3] = dsy / dn;

	error = updatePairedBuckets(slon, slat, (int64_t) sge[4]);
	if (error < 0) {
		goto pclean;
	}

	error = updatePairedBuckets(dlon, dlat, dbaddr);

 pclean:
	unmapGridBucket(sb);
	unmapGridBucket(db);

 clean:
	return error;
}

int gridfile::hasPairedBucket(int *isPaired, int *vertical, double *ge,
			      int64_t lon, int64_t lat)
{
	int error = 0;
	*isPaired = 0;
	*vertical = -1;
	double *xge = NULL;
	double *yge = NULL;

	if (lon > 0) {
		error = getGridEntry(lon - 1, lat, &xge);
		if (error < 0) {
			goto clean;
		}

		if (ge[4] == xge[4]) {
			*isPaired = 1;
			*vertical = 1;
		}
	}

	if (lat > 0) {
		error = getGridEntry(lon, lat - 1, &yge);
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

int gridfile::insertRecord(double x, double y, void *record, int64_t rsize)
{
	int error = 0;
	int64_t lon = 0;
	int64_t lat = 0;
	double *ge = NULL;
	int64_t nbytes = 0;
	int64_t nrecords = 0;
	int64_t capacity = 0;
	int64_t esize = 24 + rsize;
	int isPaired = -1;
	int vertical = -1;
	int split;
	int64_t xint = (int64_t) gridScale[1];
	int64_t yint = (int64_t) gridScale[1 + gridSize];
	split = xint == yint ? 1 : 0;

	getGridLocation(&lon, &lat, x, y);

	error = getGridEntry(lon, lat, &ge);
	if (error < 0) {
		goto clean;
	}

	nbytes = (int64_t) ge[0];
	nrecords = (int64_t) ge[1];
	capacity = pageSize - 16 - nbytes;

	if (esize <= capacity) {
		error = insertGridRecord(ge, x, y, record, rsize);
		if (error < 0) {
			goto clean;
		}

		error = updatePairedBuckets(lon, lat, (int64_t) ge[4]);
		if (error < 0) {
			goto clean;
		}
	} else {
		error = hasPairedBucket(&isPaired, &vertical, ge, lon, lat);
		if (error < 0) {
			goto clean;
		}

		if (isPaired) {
			if (vertical) {
				error =
				    splitBucket(vertical, lon - 1, lat, lon,
						lat);
				if (error < 0) {
					goto clean;
				}
			} else {
				error =
				    splitBucket(vertical, lon, lat - 1, lon,
						lat);
				if (error < 0) {
					goto clean;
				}
			}
		} else {
			error = splitGrid(split, lon, lat, x, y);
			if (error < 0) {
				goto clean;
			}
		}

		error = insertRecord(x, y, record, rsize);
	}

 clean:
	return error;
}

int gridfile::findRecord(double x, double y, void **record)
{
	int error = 0;
	int found = 0;
	int64_t lon = 0;
	int64_t lat = 0;
	double *ge = NULL;
	double *gb = NULL;
	int64_t nrecords = 0;
	int64_t iter = 0;
	double *be = NULL;
	double bex = 0;
	double bey = 0;
	double rsize = 0;

	*record = (void *)malloc(pageSize);
	if (*record == NULL) {
		error = -ENOMEM;
		goto clean;
	}

	getGridLocation(&lon, &lat, x, y);

	error = getGridEntry(lon, lat, &ge);
	if (error < 0) {
		goto clean;
	}

	error = mapGridBucket(ge, &gb);
	if (error < 0) {
		goto clean;
	}

	nrecords = (int64_t) gb[1];

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
			memcpy(*record, be + 3, rsize);
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

int gridfile::deleteRecord(double x, double y)
{
	int error = 0;
	int found = 0;
	int64_t lon = 0;
	int64_t lat = 0;
	double *ge = NULL;
	double *gb = NULL;
	int64_t nrecords = 0;
	int64_t iter = 0;
	double *be = NULL;
	double bex = 0;
	double bey = 0;

	getGridLocation(&lon, &lat, x, y);

	error = getGridEntry(lon, lat, &ge);
	if (error < 0) {
		goto clean;
	}

	error = mapGridBucket(ge, &gb);
	if (error < 0) {
		goto clean;
	}

	nrecords = (int64_t) gb[1];

	for (iter = 0; iter < nrecords; iter++) {
		error = getBucketEntry(&be, gb, iter);
		if (error < 0) {
			goto pclean;
		}

		bex = be[0];
		bey = be[1];

		if (bex == x && bey == y) {
			found = 1;
			error = deleteBucketEntry(gb, iter);
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

int gridfile::findRangeRecords(double x1, double y1, double x2, double y2,
			       int64_t * dsize, void **records)
{
	int error = 0;
	int64_t lon1 = 0;
	int64_t lat1 = 0;
	int64_t lon2 = 0;
	int64_t lat2 = 0;
	int64_t iter = 0;
	int64_t xiter = 0;
	int64_t yiter = 0;
	double *ge = NULL;
	double *gb = NULL;
	double *be = NULL;
	int64_t nrecords = 0;
	int64_t nr = 0;
	double bx = 0;
	double by = 0;
	int64_t bs = 0;
	int64_t rsize = 0;
	char *rrecords = (char *)records + 8;

	getGridLocation(&lon1, &lat1, x1, y1);
	getGridLocation(&lon2, &lat2, x2, y2);

	rsize = (lon2 - lon1 + 1) * (lat2 - lat1 + 1) * pageSize;
	*records = (void *)malloc(rsize);
	if (*records == NULL) {
		error = -ENOMEM;
		goto clean;
	}

	rrecords = (char *)(*records) + 8;

	for (xiter = lon1; xiter <= lon2; xiter++) {
		for (yiter = lat1; yiter <= lat2; yiter++) {
			error = getGridEntry(xiter, yiter, &ge);
			if (error < 0) {
				goto clean;
			}

			error = mapGridBucket(ge, &gb);
			if (error < 0) {
				goto clean;
			}

			nrecords = (int64_t) gb[1];

			for (iter = 0; iter < nrecords; iter++) {
				error = getBucketEntry(&be, gb, iter);
				if (error < 0) {
					unmapGridBucket(gb);
					goto clean;
				}

				bx = be[0];
				by = be[1];
				bs = (int64_t) be[2];

				if (bx >= x1 && bx <= x2 && by >= y1
				    && by <= y2) {
					nr += 1;
					memcpy(rrecords, be, 24 + bs);
					rrecords += (int64_t) (24 + bs);
					*dsize += (24 + bs);
				}
			}

			unmapGridBucket(gb);
		}
	}

	((double *)records)[0] = (double)nr;

 clean:
	return error;
}

int gridfile::loadGrid()
{
	int error = 0;

	error = mapGridScale();
	if (error < 0) {
		goto clean;
	}

	error = mapGridDirectory();
	if (error < 0) {
		unmapGridScale();
	}

 clean:
	return error;
}

void gridfile::unloadGrid()
{
	unmapGridScale();
	unmapGridDirectory();
}
