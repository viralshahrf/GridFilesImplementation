.PHONY : make
make :
	g++ gridfile.cpp -o gridfile
.PHONY : clean
clean :
	rm -f build \
	*.o \
	*.out \
	*.a
	rm -rf *.swp
	rm -rf gridfile
	rm -rf gridscale
	rm -rf griddirectory
	rm -rf gridbuckets
	rm -rf gridrecords
