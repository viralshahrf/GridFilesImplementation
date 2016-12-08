.PHONY : make
make :
	g++ -c gridfile.cpp -o gridfile.o
	g++ -c datagenerator.cpp -o datagenerator.o
	g++ -c test.cpp -o test.o
	g++ gridfile.o datagenerator.o test.o -o test
.PHONY : clean
clean :
	rm -f build \
	*.o \
	*.out \
	*.cpp~ \
	*.h~ \
	*.a
	rm -rf *.swp
	rm -rf test
	rm -rf db*
