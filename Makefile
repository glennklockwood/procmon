# Makefile for procmon
# Author: Doug Jacobsen <dmjacobsen@lbl.gov>
# 2013/02/17
# 


OPT_EXTRA=-O3
EXTRA=-std=c++11 $(OPT_EXTRA)
HDF5_INCLUDE=-I$(HDF5_DIR)/include -D__USE_HDF5=1
AMQP_INCLUDE=-I$(RABBITMQ_C_DIR)/include -D__USE_AMQP=1
BOOST_INCLUDE=-I$(BOOST_ROOT)/include

HDF5_LDFLAGS=-L$(HDF5_DIR)/lib -lhdf5
AMQP_LDFLAGS=-L$(RABBITMQ_C_DIR)/lib -lrabbitmq -lpthread -lssl -lcrypto -lz -ldl
BOOST_LDFLAGS=-L$(BOOST_ROOT)/lib -lboost_program_options -lboost_system -lpthread
CONFIG="echo > config.h"

CFLAGS=$(EXTRA) 
LDFLAGS=-static

ifdef GENEPOOL
	USE_AMQP=1
	USE_BOOST=1
	CONFIG="cp config_genepool.h config.h"
	CFLAGS += "-DUSE_CONFIG_H=1"
endif

ifdef USE_HDF5
	CFLAGS += $(HDF5_INCLUDE)
	LDFLAGS += $(HDF5_LDFLAGS)
endif

ifdef USE_AMQP
	CFLAGS += $(AMQP_INCLUDE)
	LDFLAGS += $(AMQP_LDFLAGS)
endif

ifdef USE_BOOST
	CFLAGS += $(BOOST_INCLUDE)
	LDFLAGS += $(BOOST_LDFLAGS)
endif

all: config.h driver procmon
config.h:
	/bin/sh -c $(CONFIG)
driver: driver.o ProcIO.o
	$(CXX) -o $@ $^ $(LDFLAGS)
procmon: procmon.o ProcIO.o
	$(CXX) -o $@ $^ $(LDFLAGS)
procmon.o: procmon.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^
driver.o: driver.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^
ProcIO.o: ProcIO.cpp
	$(CXX) -c $(CFLAGS) -o $@ -c $^

clean:
	$(RM) *.o procmon driver config.h
