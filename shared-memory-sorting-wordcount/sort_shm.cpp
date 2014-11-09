/**
 * Multithreaded sorting program
 * 
 * Author: Long Nangong 
 *
 * Email: lnangong@hawk.iit.edu
 * 
 * Created on: 10/14/2014 
 */

#include <iostream>
#include <thread>
#include <cctype>
#include <fstream>
#include <string>
#include <cstring>
#include <map>
#include <unordered_map>
#include <queue>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <vector>

using namespace std;

typedef unordered_map<string,int> u_map;
typedef map<string,int> o_map;

const size_t NUM_THREADS = 3; //# of worker threads
queue<string> jobQ;
//vector<queue<string>> chunkBuf;
const size_t JOBQ_SIZE = 10 * 1024 * 1024; //The maximum # of records in job queue = 10M
const size_t VECTOR_SIZE = 1024 * 1024; //# of records for each chunk = 1M
//const size_t buf_size = 10240; // Chunk read buffer size = 10K
mutex mtx;    // Mutex for critical section
condition_variable master; //Condition value of Master thread
int chunk_index = 0;



void splitter(char *token_meta, size_t token_size) {
	string infile = "chunk_0.txt";
	ifstream inchunk(infile); //open chunk file

	int lines = 0;
	int meta_index = 0;
	string record;
	char token='\0';
	
	while(getline(inchunk,record)){
		lines++;
		if(lines <= token_size || record[0] == token){
			//token_meta[meta_index] = record[0];	//store spliter char to split meta space
			token = record[0];
		}else{
			token_meta[meta_index] = record[0];	//store record to send buffer
			meta_index++; //increase meta index
			lines = 0; //reset read line
			//cout << token_meta[meta_index] << endl;
		}
		
	}
//	token_meta[meta_index] = "\0";
	
	inchunk.close(); //close chunk file
	
}


void do_split(string rec_buf[], char *token_meta, int index){
	string infile = "chunk_"+to_string(index)+".txt";
        ifstream inchunk(infile);

	string record;
	int buf_index = 0;
	bool next = false;
	while(getline(inchunk,record)){
		if(record[0] != token_meta[buf_index] ){
			rec_buf[buf_index].append(record,0,record.size()-1);	//store record to send buffera

		}else{
			rec_buf[++buf_index].append(record,0,record.size()-1);	//store record to send buffer
			//cout << rec_buf[buf_index] << endl;//debuging
		}
		
	}

	//cout << rec_buf[0].size() <<endl;	//debuging
	inchunk.close();
	
}


void msg_parsing(string &rev_msg, size_t record_size, int chunk_index){
	string outfile = "new_chunk_"+to_string(chunk_index)+".txt";

	ofstream outchunk;
	outchunk.open(outfile, std::ofstream::out | std::ofstream::app);

	string record;
	int index = 1;
	
	for(string::iterator iter=rev_msg.begin(); iter!=rev_msg.end(); iter++){
		record.append(1,*iter);
		if(index < (record_size-1)){
			index++;
		}else{
			index = 1;
			outchunk << record << endl;
			record.clear();
		}
		
	}

	outchunk.close();
}


void reorder(int PID){
	ifstream inchunk;
	ofstream outchunk;
	vector<string> record_v;
	string record;

	int nthreads;
	if(NUM_THREADS == 0) nthreads = 1;
	else{
		nthreads = NUM_THREADS;
	}

	for(int index=PID; index < chunk_index; index+=nthreads){ //chunk index
		string infile = "new_chunk_"+to_string(index)+".txt";
		inchunk.open(infile);
		while(getline(inchunk,record)){
			record_v.push_back(record);
	
		}
		inchunk.close();

		sort(record_v.begin(),record_v.end()); //sort record vector
		
		string outfile = "sorted_chunk_"+to_string(index)+".txt";
		outchunk.open(outfile);

		for(vector<string>::iterator iter=record_v.begin(); iter!= record_v.end(); iter++){
			outchunk << *iter << endl;

		}

		record_v.clear();
		outchunk.close();
	}


}


void distribution() {	
	int num_worker = NUM_THREADS;
	size_t token_size;
	char token_meta[chunk_index]; //split standard metadata

	string filename = "chunk_0.txt";
	ifstream inchunk(filename);
	if(inchunk.fail()) {
		cout << "Cannot open chunk file.\n";
		return ;
	}

	string record;
	size_t lines = 0;
	size_t record_size;
	bool getsize = true;
	while(getline(inchunk,record)){
		lines++;
		if(getsize){
			record_size = record.size();
			getsize = false;
		}
	
	}  //get lines in the chunk

	inchunk.close();

	token_size = lines / chunk_index ;  //get split size

	splitter(token_meta, token_size); //get split standard metadata
	
	int buf_size = chunk_index;


	for(int index=0; index < chunk_index; index++){ //chunk index
			
		string rec_buf[buf_size]; //sending record buffer

		do_split(rec_buf, token_meta, index); //token the records to the sending buffer
	
		for(int buf_index=0; buf_index < buf_size; buf_index++){
			msg_parsing(rec_buf[buf_index],record_size,index);
		
		}
		
	}

}


//final message gather
void msg_gather() {

	string outfile = "sort1MB-sharedmemory.txt";
	ofstream outchunk;
	outchunk.open(outfile, std::ofstream::out | std::ofstream::app);

	ifstream inchunk;//input chunk

	string record;
	int index = 1;
//	static int size = 0;
//	int max_size = 10000;
//	if(size < max_size){
		for(int index=0; index < chunk_index; index++){ //chunk index
			string infile = "sorted_chunk_"+to_string(index)+".txt";
			inchunk.open(infile);
			while(getline(inchunk,record)){
				outchunk << record << endl;
			}
			inchunk.close();	
		}
//	}
	outchunk.close();

}


void recordSort(vector<string>& rec_vector){
	
	
	sort(rec_vector.begin(),rec_vector.end());

	mtx.lock();
	string filename = "chunk_"+to_string(chunk_index)+".txt";
	chunk_index++;	
	mtx.unlock();

	ofstream partition(filename);
	if(partition.fail()) {
		cout << "Cannot open partition file.\n";
		return;
	}
	
	for(vector<string>::iterator iter=rec_vector.begin(); iter!=rec_vector.end(); ++iter)
		partition << *iter << endl;
	
	
	rec_vector.clear();	

	
}


void worker(bool& stop){
	string record;
	vector<string> rec_vector;

	while(!stop || !jobQ.empty()){ //Keep working untill job queue is empty and have stop set

		while(!jobQ.empty()){ //Check available job in the job queue

			mtx.lock(); //Critical section to get job from job queue

			if(jobQ.empty()){ //Check job queue
				mtx.unlock();
				break;
			}

			record = jobQ.front();	
			jobQ.pop(); //Remove front job from queue
			master.notify_one();
			mtx.unlock();
		
			if(rec_vector.size() < VECTOR_SIZE){
				rec_vector.push_back(record);
			}
			else{
				recordSort(rec_vector); //Count the words in the buffer
			}

		}
	}

	if(!rec_vector.empty())	
		recordSort(rec_vector); //Count the words in the buffer

}


int main(int argc, char *argv[]){
	std::thread thread[NUM_THREADS]; //Create # of worker threads
	multimap<string,int> minHeap;
	bool stop = false;
	
	if(argc!=2) { //Check input argument
		cout << "Usage: sort <sourceFile>\n";
		return -1;
	}
	
	cout << "Shared memory sorting is started...\n";

	/* Start Clock */
	auto begin = chrono::high_resolution_clock::now(); 
	
	//Launch a group of worker threads	
	for(int i=0; i<NUM_THREADS; i++){
		thread[i] = std::thread(worker,ref(stop));	
	}

	ifstream inFile(argv[1]);  //Input file stream
	
	if(inFile.fail()) {
		cout << "Cannot open input file.\n";
		return -1;
	}
	
	string record;
	vector<string> rec_vector;
	while(getline(inFile,record)){
		if(NUM_THREADS != 0){ //Multithreaded sort
			unique_lock<mutex> lck(mtx);

			while(!(jobQ.size() < JOBQ_SIZE)) master.wait(lck); //Check job queue size

			jobQ.push(record);
		}
		else{
			if(rec_vector.size() < VECTOR_SIZE){
				rec_vector.push_back(record);
			}
			else{
				recordSort(rec_vector); //sort the words in the buffer
			}

		}
	}

	if(!rec_vector.empty())	
		recordSort(rec_vector); //sort the words in the buffer


	stop = true;
	inFile.close();

	//Join the worker threads with main thread
	for(int i=0; i<NUM_THREADS; i++){
		thread[i].join();	
	}
	
	//distribute sorted records
	distribution();
	
	if(NUM_THREADS == 0){
		reorder(0); //single thread reorder
	}

	//Launch a group of worker threads to reorder records in the chunk
        for(int i=0; i<NUM_THREADS; i++){
                thread[i] = std::thread(reorder,i);
        }

	for(int i=0; i<NUM_THREADS; i++){
		thread[i].join();	
	}

	msg_gather();


	/* Stop Clock */
	auto end = chrono::high_resolution_clock::now();    
	auto elap = end - begin;
	auto excTime = chrono::duration_cast<std::chrono::milliseconds>(elap).count();
	/* Display timing results */
	cout << "Elapsed time = " << excTime << " ms." << endl;
	cout << "\nDone!\n";

	return 0;

}
