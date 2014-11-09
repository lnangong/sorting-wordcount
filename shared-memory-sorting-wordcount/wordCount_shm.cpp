/**
 * Multithreaded word-count program
 * 
 * Author: Long Nangong 
 *
 * Email: lnangong@hawk.iit.edu
 * 
 * Created on: 10/9/2014 
 */

#include <iostream>
#include <thread>
#include <cctype>
#include <fstream>
#include <string>
#include <unordered_map>
#include <boost/bimap.hpp>
#include <boost/bimap/set_of.hpp>
#include <boost/bimap/multiset_of.hpp>
#include <queue>
#include <chrono>
#include <mutex>
#include <condition_variable>

using namespace std;
namespace bimaps = boost::bimaps;

typedef unordered_map<string,int> u_map;
typedef boost::bimap<bimaps::set_of<string>,bimaps::multiset_of<int>> bimap_t;
typedef bimap_t::value_type value_type;

const size_t NUM_THREADS = 3; //# of worker threads
queue<string> jobQ;
const size_t JOBQ_SIZE = 10 * 1024 * 1024; //The maximum size of job queue 
mutex mtx;    // Mutex for critical section
condition_variable master; //Condition value of Master thread



void wordCount(u_map& word_map, string buffer){
	string word;

	for(int i = 0; i < buffer.length(); ++i){

		if(isalpha(buffer[i])){
			word.append(1,buffer[i]); //Append alphabet to word string
		}
		else if(buffer[i] == '\''){
			if(isalpha(buffer[i+1]))
				word.append(1,buffer[i]); //Append alphabet to word string
			else if(!word.empty()){

				if(word_map.find(word)==word_map.end()){ //If word string is not in the map
					word_map[word] = 1;
				}
				else{    
					word_map[word]++; //Word found in the map
				}
				
				word.clear();
			}
		}
		else{
			if(!word.empty()){

				if(word_map.find(word)==word_map.end()){ //If word string is not in the map
					word_map[word] = 1;
				}
				else{    
					word_map[word]++; //Word found in the map
				}
				
				word.clear();
			}
		}
	}
/*
	word = strtok(buffer," \t\n\v\f\r");
 	while (word != NULL){
		if(word_map.find(word)==word_map.end())
			word_map[word] = 1;
		else	
			word_map[word]++;

    		word = strtok (NULL, " ,.-");
				
	}
*/
}


void worker(u_map& word_map, bool& stop){
	string buffer;

	while(!stop || !jobQ.empty()){ //Keep working untill job queue is empty and have stop set

		while(!jobQ.empty()){ //Check available job in the job queue

			mtx.lock(); //Critical section to get job from job queue

			if(jobQ.empty()){ //Check job queue
				mtx.unlock();
				break;
			}

			buffer = jobQ.front();	
			jobQ.pop(); //Remove front job from queue
			master.notify_one();
			mtx.unlock();
		
			wordCount(word_map, buffer); //Count the words in the buffer

		}
	}

}


int main(int argc, char *argv[]){
	thread thread[NUM_THREADS]; //Create # of worker threads
	u_map word_map[NUM_THREADS]; //Create # of hash maps for worder threads
	u_map single_word_map;
	bimap_t ordered_map;
	bool stop = false;
	
	if(argc!=2) { //Check input argument
		cout << "Usage: count <sourceFile>\n";
		return -1;
	}
	
	cout << "Shared memory word count is started...\n\n";

	/* Start Clock */
	auto begin = chrono::high_resolution_clock::now(); 
	
	//Launch a group of worker threads	
	for(int i=0; i<NUM_THREADS; i++){
		thread[i] = std::thread(worker,ref(word_map[i]),ref(stop));	
	}

	ifstream inFile(argv[1]);  //Input file stream
	
	if(inFile.fail()) {
		cout << "Cannot open input file.\n";
		return -1;
	}
	
	string buffer;
	while(getline(inFile,buffer)){
		if(NUM_THREADS != 0){ //Multithreaded word count
			unique_lock<mutex> lck(mtx);

			while(!(jobQ.size() < JOBQ_SIZE)) master.wait(lck); //Check job queue size

			jobQ.push(buffer);//Push job to job queue

		}
		else{
			//Single thread word count
			wordCount(single_word_map,buffer); 

		}
	}
	
	stop = true;
	inFile.close();

	//Join the worker threads with main thread
	for(int i=0; i<NUM_THREADS; i++){
		thread[i].join();	
	}

	//Reorder the map for single thread word count
	auto &key = ordered_map.left;
	if(NUM_THREADS == 0){
		for (u_map::iterator iter = single_word_map.begin(); iter != single_word_map.end(); iter++){

			if(key.find(iter->first) == key.end()){
				ordered_map.insert(value_type(iter->first, iter->second));
			}
			else{
				int data = key.find(iter->first)->second;
				key.replace_data(key.find(iter->first), data += iter->second);
			}
		}	
	}

	//Combine and reorder the word_maps for multithreaded word count	
	for(int i=0; i<NUM_THREADS; i++){

		for (u_map::iterator iter = word_map[i].begin(); iter != word_map[i].end(); iter++){

			if(key.find(iter->first) == key.end()){
				ordered_map.insert(value_type(iter->first, iter->second));
			}
			else{
				int data = key.find(iter->first)->second;
				key.replace_data(key.find(iter->first), data += iter->second);
			}
		}	
	}
	
	//Open output stream
	ofstream outFile("wordCount_c++.txt");
	if(outFile.fail()) {
		cout << "Cannot open output file.\n";
		return -1;
	}	

	//Produce result output file
    	for (bimap_t::right_reverse_iterator iter = ordered_map.right.rbegin(); iter != ordered_map.right.rend(); iter++)
        	outFile << iter->second << ": " << iter->first << endl;
	

	outFile.close();

	/* Stop Clock */
	auto end = chrono::high_resolution_clock::now();    
	auto elap = end - begin;
	auto excTime = chrono::duration_cast<std::chrono::milliseconds>(elap).count();
	/* Display timing results */
	cout << "Elapsed time = " << excTime << " ms." << endl;

	cout << "\nDone!\n";

	return 0;

}
