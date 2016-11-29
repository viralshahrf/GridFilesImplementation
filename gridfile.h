#ifndef UTILITIES_HPP
#define UTILITIES_HPP

using namespace std;

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
	double *gridScale;
	double *gridDirectory;

	int createFile(int64_t size, string fname, const char *mode);
	void getGridLocation(int64_t * lon, int64_t * lat, double x, double y);
	int insertGridPartition(int lon, double partition);
	int mapGridScale();
	void unmapGridScale();
	int mapGridDirectory();
	void unmapGridDirectory();
	int getGridEntry(int64_t lon, int64_t lat, double **gentry);
	int splitGrid(int vertical, int64_t lon, int64_t lat, double x,
		      double y);
	int mapGridBucket(double *gentry, double **gbucket);
	void unmapGridBucket(double *gbucket);
	void appendBucketEntry(double *gbucket, double x, double y,
			       int64_t rsize, void *record);
	int insertGridRecord(double *gentry, double x, double y, void *record,
			     int64_t rsize);
	int getBucketEntry(double **bentry, double *gbucket, int64_t entry);
	int deleteBucketEntry(double *gbucket, int64_t entry);
	int getGridPartitions(double *x, double *y, int64_t lon, int64_t lat);
	int splitBucket(int vertical, int64_t slon, int64_t slat, int64_t dlon,
			int64_t dlat);
	int hasPairedBucket(int *isPaired, int *vertical, double *ge,
			    int64_t lon, int64_t lat);
 public:
	int createGrid(int64_t size, int64_t psize, string name);
	int insertRecord(double x, double y, void *record, int64_t rsize);
	int findRecord(double x, double y, void *record);
	int deleteRecord(double x, double y);
	int findRangeRecords(double x1, double y1, double x2, double y2,
			     int64_t dsize, void *records);
};

#endif
