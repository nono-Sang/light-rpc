CXX = g++
CXXFLAGS = -g -Wall -Werror -std=c++17

LIBFLAGS = -lprotobuf -lmimalloc -lrdmacm -libverbs -lpthread

# $@: target file
# $<：first dependent file
# $^：all dependent files

all: build/client build/server

src = $(wildcard proto/*.cc src/*.cc test/*cc)
obj = $(patsubst %.cc, %.o, $(src))

PBOBJ = proto/light_impl.pb.o test/echo.pb.o src/light_api.o src/light_common.o

build/server: $(PBOBJ) src/light_server.o test/server.o
	$(CXX) $^ -o $@ $(LIBFLAGS)

build/client: $(PBOBJ) src/light_channel.o test/client.o
	$(CXX) $^ -o $@ $(LIBFLAGS)

# compile and generate dependency info
%.o: %.cc
	$(CXX) -c -fPIC $(CXXFLAGS) $*.cc -o $*.o $(LIBFLAGS)


dynamic: liblightrpc.so
member = $(wildcard proto/*.o src/*.o)
liblightrpc.so: $(member)
	$(CXX) -shared $^ -o $@


# remove compilation products
clean:
	rm -f $(obj)