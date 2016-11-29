.PHONY : make
make :
	g++ -c gridfile.cpp -o gridfile.o
.PHONY : clean
clean :
	rm -f build \
	*.o \
	*.out \
	*.cpp~ \
	*.h~ \
	*.a
	rm -rf *.swp
	rm -rf gridfile
	rm -rf gridscale
	rm -rf griddirectory
	rm -rf gridbuckets
	rm -rf gridrecords
