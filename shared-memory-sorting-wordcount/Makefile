
all: count sort

count: wordCount_shm.cpp
	g++ -std=c++11 -o count wordCount_shm.cpp -pthread

sort: sort_shm.cpp
	g++ -std=c++11 -o sort sort_shm.cpp -pthread
clean:
	rm count

