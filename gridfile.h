#ifndef GRIDFILE_HPP
#define GRIDFILE_HPP

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <string>

using namespace std;

struct gridconfig {
	int64_t size;
	int64_t psize;
	string name;
};

struct gridfile {
 private:
	int64_t gridSize;
	int64_t pageSize;
	int64_t scaleSize;
	int64_t directorySize;
	int64_t bucketSize;
	string gridName;
	string scaleName;
	string directoryName;
	string bucketName;
	int64_t *gridScale;
	int64_t *gridDirectory;

	int createFile(int64_t size, string fname, const char *mode);
	int mapGridScale();
	void unmapGridScale();
	int mapGridDirectory();
	void unmapGridDirectory();
	void getGridLocation(int64_t * lon, int64_t * lat, int64_t x,
			     int64_t y);
	int insertGridPartition(int lon, int64_t partition);
	int getGridPartitions(int64_t * x, int64_t * y, int64_t lon,
			      int64_t lat);
	int getGridEntry(int64_t lon, int64_t lat, int64_t ** gentry);
	int mapGridBucket(int64_t * gentry, int64_t ** gbucket);
	void unmapGridBucket(int64_t * gbucket);
	void appendBucketEntry(int64_t * gbucket, int64_t x, int64_t y,
			       int64_t rsize, void *record);
	int getBucketEntry(int64_t ** bentry, int64_t * gbucket, int64_t entry);
	int deleteBucketEntry(int64_t * gbucket, int64_t entry);
	int insertGridRecord(int64_t * gentry, int64_t x, int64_t y,
			     void *record, int64_t rsize);
	int splitGrid(int vertical, int64_t lon, int64_t lat, int64_t x,
		      int64_t y);
	int updateBucket(int direction, int64_t slon, int64_t slat,
			 int64_t dlon, int64_t dlat, int64_t baddr);
	int updatePairedBuckets(int direction, int64_t lon, int64_t lat,
				int64_t baddr);
	int splitBucket(int vertical, int64_t slon, int64_t slat, int64_t dlon,
			int64_t dlat);
	int checkPairedBucket(int *isPaired, int64_t slon, int64_t slat,
			      int64_t dlon, int64_t dlat);
	int hasPairedBucket(int direction, int *isPaired, int *vertical,
			    int *forward, int64_t lon, int64_t lat);

 public:
	int createGrid(struct gridconfig *configuration);
	int loadGrid();
	void unloadGrid();
	int insertRecord(int64_t x, int64_t y, void *record, int64_t rsize);
	int testFunctionality();
};

#endif
