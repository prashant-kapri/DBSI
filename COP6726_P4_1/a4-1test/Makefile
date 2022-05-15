CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Tree.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Tree.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o -ll -lpthread

a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o DBFileGeneric.o Heap.o Sorted.o Tree.o Pipe.o y.tab.o lex.yy.o a2test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o DBFileGeneric.o Heap.o Sorted.o Tree.o Pipe.o y.tab.o lex.yy.o a2test.o -lpthread #-lfl

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Tree.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o gtest.o	
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Tree.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o gtest.o -ll -lpthread -lgtest	

test.o: test.cc
	$(CC) -g -c test.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h