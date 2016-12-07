.PHONY : make
make :
	g++ -c gridfile.cpp -o gridfile.o
	g++ -c test.cpp -o test.o
	g++ -c datagenerator.cpp -o datagenerator.o
	g++ gridfile.o test.o datagenerator.o -o test
.PHONY : clean
clean :
	rm -f build \
	*.o \
	*.out \
	*.cpp~ \
	*.h~ \
	*.a
	rm -rf test
	rm -rf *.swp
	rm -rf *scale
	rm -rf *directory
	rm -rf *buckets
