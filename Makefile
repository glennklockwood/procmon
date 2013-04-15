# Makefile for procmon
# Author: Doug Jacobsen <dmjacobsen@lbl.gov>
# 2013/02/17
# 

OPT_EXTRA=-O3 #-Wall
EXTRA=$(OPT_EXTRA)

CFLAGS=$(EXTRA) -I$(HDF5_DIR)/include

driver: driver.o procfmt.o
	$(CXX) -o $@ $^ -lhdf5 -L$(HDF5_DIR)/bin #-static
procmon: procmon.o procfmt.o
	$(CXX) -o $@ $^ -lhdf5 -L$(HDF5_DIR)/bin #-static
procmon.o: procmon.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^
driver.o: driver.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^
procfmt.o: procfmt.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^

clean:
	$(RM) *.o procmon
