ifndef CXX
CXX = g++
endif

ifndef PS_PATH
PS_PATH = $(shell pwd)/../../3rdparty/ps-lite
endif

ifndef DEPS_PATH
DEPS_PATH = $(PS_PATH)/deps
endif

INCPATH = -I$(PS_PATH)/src -I$(PS_PATH)/include -I$(DEPS_PATH)/include
CFLAGS = -std=c++11 -msse2 -fPIC -O3 -ggdb -Wall -finline-functions $(INCPATH) $(ADD_CFLAGS)
PS_LDFLAGS_SO = -L$(DEPS_PATH)/lib -lprotobuf-lite -lzmq
LDFLAGS = -Wl,-rpath,$(DEPS_PATH)/lib $(PS_LDFLAGS_SO) -pthread

all: 
	$(CXX) -std=c++0x $(CFLAGS) -MM -MT test_ssp test_ssp.cc >test_ssp.cc.d
	$(CXX) -std=c++0x $(CFLAGS) -o test_ssp test_ssp.cc $(PS_PATH)/build/libps.a $(LDFLAGS)

clean:
	rm -rf *.d test_ssp




