# Makefile for procmon
# Author: Doug Jacobsen <dmjacobsen@lbl.gov>
# 2013/02/17
# 

OPT_EXTRA=-O3 #-ggdb #-Wall
EXTRA=$(OPT_EXTRA)

CFLAGS=$(EXTRA)

procmon: procmon.o
	$(CC) -o $@ $^
procmon.o: procmon.c
	$(CC) -c $(CFLAGS) -o $@ -c $^

clean:
	$(RM) *.o procmon
