# Makefile for procmon
# Author: Doug Jacobsen <dmjacobsen@lbl.gov>
# 2013/02/17
# 


OPT_EXTRA=-O2
EXTRA=-std=c++11 $(OPT_EXTRA)
HDF5_INCLUDE=-I$(HDF5_DIR)/include -DUSE_HDF5=1
AMQP_INCLUDE=-I$(RABBITMQ_C_DIR)/include -DUSE_AMQP=1
BOOST_INCLUDE=-I$(BOOST_ROOT)/include

HDF5_LDFLAGS=-L$(HDF5_DIR)/lib -lhdf5 -lz -ldl
AMQP_LDFLAGS=$(RABBITMQ_C_DIR)/lib/librabbitmq.a -lrt
BOOST_LDFLAGS=-L$(BOOST_ROOT)/lib -lboost_program_options
CONFIG="echo > config.h"

CFLAGS=$(EXTRA) 
LDFLAGS=-static-libstdc++ -static-libgcc
PROCMON_CFLAGS=$(CFLAGS)
PROCMON_LDFLAGS=$(LDFLAGS)
REDUCER_CFLAGS=$(CFLAGS) $(BOOST_INCLUDE)
REDUCER_LDFLAGS=-static -ldl -lz $(LDFLAGS) $(BOOST_LDFLAGS) -ldl -lz

ifdef GENEPOOL
	USE_PROCMON_AMQP=1
	USE_REDUCER_AMQP=1
	USE_REDUCER_HDF5=1
	CONFIG="cp config_genepool.h config.h"
	CFLAGS += "-DUSE_CONFIG_H=1"
endif

ifdef TEST
	USE_AMQP=1
	USE_PROCMON_AMQP=1
	USE_REDUCER_AMQP=1
	USE_REDUCER_HDF5=1
	CONFIG="cp config_testvm.h config.h"
	CFLAGS += "-DUSE_CONFIG_H=1"
endif

ifdef SECURED
    PROCMON_CFLAGS += "-DSECURED=1"
    PROCMON_LDFLAGS +=-pthread -lcap
endif

ifdef USE_PROCMON_HDF5
	PROCMON_CFLAGS += $(HDF5_INCLUDE)
	PROCMON_LDFLAGS += $(HDF5_LDFLAGS)
endif
ifdef USE_REDUCER_HDF5
	REDUCER_CFLAGS += $(HDF5_INCLUDE)
	REDUCER_LDFLAGS += $(HDF5_LDFLAGS)
endif

ifdef USE_PROCMON_AMQP
	PROCMON_CFLAGS += $(AMQP_INCLUDE)
	PROCMON_LDFLAGS += $(AMQP_LDFLAGS)
endif
ifdef USE_REDUCER_AMQP
	REDUCER_CFLAGS += $(AMQP_INCLUDE)
	REDUCER_LDFLAGS += $(AMQP_LDFLAGS)
endif

all: config.h procmon ProcReducer ProcMuxer PostReducer CheckH5
config.h:
	/bin/sh -c $(CONFIG)
procmon: procmon.o ProcIO.procmon.o
	$(CXX) -o $@ $^ $(PROCMON_LDFLAGS)
ProcReducer: ProcReducer.o ProcIO.reducer.o ProcReducerData.o
	$(CXX) -o $@ $^ $(REDUCER_LDFLAGS)
ProcMuxer: ProcMuxer.o ProcIO.reducer.o
	$(CXX) -o $@ $^ $(REDUCER_LDFLAGS) -pthread
PostReducer: PostReducer.o ProcIO.reducer.o ProcReducerData.o
	$(CXX) -o $@ $^ $(REDUCER_LDFLAGS)
CheckH5: CheckH5.o ProcIO.reducer.o
	$(CXX) -o $@ $^ $(REDUCER_LDFLAGS)
procmon.o: procmon.cpp procmon.hh config.h
	$(CXX) -c $(PROCMON_CFLAGS) -o $@ -c procmon.cpp
ProcReducer.o: ProcReducer.cpp
	$(CXX) -c $(REDUCER_CFLAGS) -o $@ -c $^
ProcMuxer.o: ProcMuxer.cpp
	$(CXX) -c $(REDUCER_CFLAGS) -o $@ -c $^
PostReducer.o: PostReducer.cpp
	$(CXX) -c $(REDUCER_CFLAGS) -o $@ -c $^
CheckH5.o: CheckH5.cpp
	$(CXX) -c $(REDUCER_CFLAGS) -o $@ -c $^
ProcReducerData.o: ProcReducerData.cc ProcReducerData.hh config.h
	$(CXX) -c $(REDUCER_CFLAGS) -o $@ -c ProcReducerData.cc
ProcIO.reducer.o: ProcIO.cpp config.h ProcIO.hh
	$(CXX) -c $(REDUCER_CFLAGS) -o $@ -c ProcIO.cpp
ProcIO.procmon.o: ProcIO.cpp config.h ProcIO.hh
	$(CXX) -c $(PROCMON_CFLAGS) -o $@ -c ProcIO.cpp

clean:
	$(RM) *.o procmon ProcReducer config.h
