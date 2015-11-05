QUIET	?= @
INCPATH = ../../include
LIBPATH = ../../lib

CXXFLAGS ?=-g3 -O2 -W -Wall -Werror -I$(INCPATH) -DUNDEBUG
#PROFILER  =-L../../../gperftools/lib -lprofiler 
#TCMALLOC  =-L../../../gperftools/lib -ltcmalloc
LDFLAGS   =-L$(LIBPATH) $(PROFILER) $(TCMALLOC) -lulib -lpthread -lrt

APP = $(patsubst %.cpp, %.app, $(wildcard *.cpp))

%.app: %.cpp
	$(QUIET)echo "GEN "$@
	$(QUIET)$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

all: $(APP)

clean:
	$(QUIET)rm -rf $(APP) *~

.PHONY: all clean
