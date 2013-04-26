# Makefile for procmon
# Author: Doug Jacobsen <dmjacobsen@lbl.gov>
# 2013/02/17
# 

OPT_EXTRA=-ggdb #-Wall
EXTRA=-std=c++11 $(OPT_EXTRA)

CFLAGS=$(EXTRA) -I$(HDF5_DIR)/include -I$(RABBITMQ_C_DIR)/include

all: driver procmon
driver: driver.o ProcIO.o
	$(CXX) -o $@ $^ -lhdf5 -L$(HDF5_DIR)/lib #-static
procmon: procmon.o ProcIO.o
	$(CXX) -o $@ $^ -lhdf5 -L$(HDF5_DIR)/lib #-static
procmon.o: procmon.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^
driver.o: driver.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^
ProcIO.o: ProcIO.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^

clean:
	$(RM) *.o procmon driver
