
CC = g++ -std=c++11 -Wno-deprecated

tag = -i
gtest_tag = -std=c++11 -lgtest -lgtest_main

ifdef linux
tag = -n
endif

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o DBFileGeneric.o Heap.o Sorted.o Tree.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o DBFileGeneric.o Heap.o Sorted.o Tree.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lpthread
	
a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lpthread
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o DBFileGeneric.o Heap.o Sorted.o Tree.o Pipe.o y.tab.o lex.yy.o a2-test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o DBFileGeneric.o Heap.o Sorted.o Tree.o Pipe.o y.tab.o lex.yy.o a2-test.o -lpthread
	
a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Tree.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest.o
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o Heap.o Sorted.o DBFile.o Tree.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o gtest.o -ll -lpthread -lgtest

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc $(gtest_tag)

test.o: test.cc
	$(CC) -g -c test.cc
	
a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFileGeneric.o: DBFileGeneric.cc
	$(CC) -g -c DBFileGeneric.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

Tree.o: Tree.cc
	$(CC) -g -c Tree.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
