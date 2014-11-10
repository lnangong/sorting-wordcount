/**
 *	MPI_sort.cpp
 *
 *	Copyright (c) 2014, Long(Ryan) Nangong.
 *	All right reserved.
 *
 *      Email: lnangong@hawk.iit.edu
 *	Created on: Oct. 9, 2014
 */
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>
//#include <sys/types.h>
//#include <sys/stat.h>
//#include <unistd.h>
#include <iomanip>

using namespace std;

const int MASTER = 0; // Master node
const size_t VECTOR_SIZE = 1024 * 1024; //# of records for each chunk = 1M
int chunk_index = 0;



void recordSort(vector<string> &record_v) {

        sort(record_v.begin(),record_v.end());

        string filename = "chunk_"+to_string(chunk_index)+".txt";
        chunk_index++;

        ofstream chunk(filename);
        if(chunk.fail()) {
                cout << "Cannot open chunk file.\n";
                MPI_Abort(MPI_COMM_WORLD, 1) ;
        }

        for(vector<string>::iterator iter=record_v.begin(); iter!=record_v.end(); ++iter)
                chunk << *iter << endl;

        record_v.clear();

}


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


void reorder(){
	ifstream inchunk;
	ofstream outchunk;
	vector<string> record_v;
	string record;

//	int num_worker = num_procs - 1;


	for(int index=0; index < chunk_index; index++){ //chunk index
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


void distribution(const int rank, const int num_procs, size_t nlines, size_t record_size) {	
	int num_worker = num_procs-1;
	size_t token_size;
	char token_meta[chunk_index*num_worker]; //split standard metadata

        if(rank == 1){
                string filename = "chunk_0.txt";
                ifstream inchunk(filename);
                if(inchunk.fail()) {
                        cout << "Cannot open chunk file.\n";
                        MPI_Abort(MPI_COMM_WORLD, 1) ;
                }

                string record;
                size_t lines = 0;
                while(getline(inchunk,record)){ lines++; }  //get lines in the chunk
                inchunk.close();

                token_size = lines / (chunk_index * num_worker);  //get split size
	
		//cout << token_size << endl;  //debuging
		splitter(token_meta, token_size); //get split standard metadata
	//	cout << strlen(token_meta)<<endl;
		
	//	for(int i=0; i < strlen(token_meta); i++)
	//		cout << setw(2) << token_meta[i] << endl;
	}
	
	MPI_Bcast(&token_meta,chunk_index*num_worker,MPI_CHAR,1,MPI_COMM_WORLD); //Broadcast split meta
//	if(rank == 1)cout << token_meta[0] << endl; //debuging

	vector<string> record_v;
	MPI_Status status;
	int buf_size = chunk_index * num_worker;

	for(int worker=1; worker <= num_worker; worker++){	//process

		for(int index=0; index < chunk_index; index++){ //chunk index
			if(worker == rank){
				
				string rec_buf[buf_size]; //sending record buffer

				do_split(rec_buf, token_meta, index); //token the records to the sending buffer
			
				for(int buf_index=0; buf_index < buf_size; buf_index++){
					int dest = buf_index % num_worker + 1;
					if(dest == rank){
						msg_parsing(rec_buf[buf_index],record_size,index);
					}else{
						int size = rec_buf[buf_index].size();
						//cout << rec_buf[buf_index].size()<<endl;
						MPI_Send(&size,1,MPI_INT,dest,0,MPI_COMM_WORLD); //send message size
	
						MPI_Send(const_cast<char*>(rec_buf[buf_index].c_str()), //sending record
							rec_buf[buf_index].size(),	//record size
							MPI_CHAR,
							dest,	//destination
							1,  //tag
							MPI_COMM_WORLD);
					}
				}
			}else{
				
				for(int index=0; index < (buf_size/num_worker); index++){  //receive sending message

					int msg_size;
					MPI_Recv(&msg_size,1,MPI_INT,worker,0,MPI_COMM_WORLD,&status);	//receive msg size

					// Probe for an incoming message from process zero
					//MPI_Probe(worker, 0, MPI_COMM_WORLD, &status);
					// Get the size of the message.
					//MPI_Get_count(&status, MPI_INT, &msg_size);

					//cout << msg_size<<endl;
					char *rev_buf = new char[msg_size]; //receive buffer
					MPI_Recv(rev_buf,	//rev buffer 
						msg_size,	// rev size
						MPI_CHAR,	//rev data type
						worker,	//source
						1,	//tag
						MPI_COMM_WORLD,
						&status);	
			
					string rev_msg(rev_buf);		
					delete rev_buf;
					
					msg_parsing(rev_msg,record_size,index);
					//cout << record << endl;

				}

			}
		}

	}

}


//final message gather
void msg_gather(string rev_msg, size_t record_size) {

	string outfile = "sort1MB-mpi.txt";

	ofstream outchunk;
	outchunk.open(outfile, std::ofstream::out | std::ofstream::app);

	string record;
	int index = 1;
//	static int size = 0;
//	int max_size = 10000;
//	if(size < max_size){	
		for(string::iterator iter=rev_msg.begin(); iter!=rev_msg.end(); iter++){
			record.append(1,*iter);
			if(index < (record_size-1)){
				index++;
			}else{
				index = 1;
				outchunk << record << endl;
				record.clear();
				//size++;
				//if(size == max_size) break;
			}
			
		}
//	}
	outchunk.close();

}


//master node
void master(const int rank, const int num_procs, char *filename, size_t &nlines, size_t &record_size) {	
		
	//struct stat f_stat; //File status structure       
	//stat( filename, &f_stat );
	//file_size = f_stat.st_size; //Get infile size
	

	ifstream inFile(filename);  //Input file stream

	if(inFile.fail()) {
		cerr << "Cannot open input file.\n";
		MPI_Abort(MPI_COMM_WORLD, 1) ;
	}

	string record;
	bool GET_SIZE=true;
	while(getline(inFile,record)){
		if(GET_SIZE){
			record_size = record.size(); //get record size
			GET_SIZE = false;
		}
		nlines++;  //count lines in the file

	} 		
	inFile.close();

	MPI_Bcast(&nlines,1,MPI_INT,0,MPI_COMM_WORLD); //Broadcast file size
	MPI_Bcast(&record_size,1,MPI_INT,0,MPI_COMM_WORLD); //Broadcast file size

	//inFile.seekg (0, ios::beg);

	inFile.open(filename);

	int dest=1;
	//Distribute date across ndoes
	while(getline(inFile,record)){
		MPI_Send(const_cast<char*>(record.c_str()), //sending record
			 record.size(),	//record size
			 MPI_CHAR,
			 dest,	//destination
			 0,  //tag
			 MPI_COMM_WORLD);	
		
		dest++;
		if(dest == num_procs) dest = 1;
	}

	inFile.close();
	
	MPI_Status status;
	MPI_Recv(&chunk_index,1,MPI_INT,1,0,MPI_COMM_WORLD,&status);	//receive chunk index from worker
	int num_worker = num_procs - 1;

	for(int index=0; index < chunk_index; index++){ //chunk index
		for(int worker=1; worker <= num_worker; worker++){ 
			int msg_size;
			MPI_Recv(&msg_size,1,MPI_INT,worker,0,MPI_COMM_WORLD,&status);	//receive msg size

			char *rev_buf = new char[msg_size]; //receive buffer
			MPI_Recv(rev_buf,	//rev buffer 
				msg_size,	// rev size
				MPI_CHAR,	//rev data type
				worker,	//source
				1,	//tag
				MPI_COMM_WORLD,
				&status);
			
			string rev_msg(rev_buf);		
			delete rev_buf;

			msg_gather(rev_msg,record_size);
		}
	}

}


//worker node
void worker(const int rank, const int num_procs, size_t nlines, size_t record_size){  

	vector<string> record_v;
	char *rev_buf;
	MPI_Status status;

	
	MPI_Bcast(&nlines,1,MPI_INT,0,MPI_COMM_WORLD); //Broadcast lines in the file
	MPI_Bcast(&record_size,1,MPI_INT,0,MPI_COMM_WORLD); //Broadcast record size

	rev_buf = new char[record_size];

	for(int i = nlines; rank <= i; i -= (num_procs-1)){
		
		MPI_Recv(rev_buf,	//rev buffer 
			 record_size,	// rev size
			 MPI_CHAR,	//rev data type
			 0,	//source
			 0,	//tag
			 MPI_COMM_WORLD,
			 &status);	
		
		string record(rev_buf);		
		//cout << record << endl;

		record_v.push_back(record);
		
		if(!(record_v.size() < VECTOR_SIZE)){
			recordSort(record_v);	//Sort the records in the buffer and save it
		}
	}

	if(!record_v.empty())
		recordSort(record_v);	//sort the remaining record vector

//	cout << nlines << endl;
//	cout << record_size << endl;

	//Re-distribute sorted records across nodes
	distribution(rank,num_procs,nlines,record_size);
	//re-sort the new record chunk
	reorder();
	
	if(rank == 1)
		MPI_Send(&chunk_index,1,MPI_INT,0,0,MPI_COMM_WORLD); //send chunk index to master

	int num_worker = num_procs - 1;
        for(int index=0; index < chunk_index; index++){ //chunk index
		for(int worker=1; worker <= num_worker; worker++){      //send sorted chunks to the master
			if(worker == rank){
				string infile = "sorted_chunk_"+to_string(index)+".txt";
				ifstream inchunk(infile);

				string record;
				string rec_buf;
				while(getline(inchunk,record)){
					rec_buf.append(record,0,record.size());	//store record to send buffera

				}
				inchunk.close(); //close inchunk 

				int size = rec_buf.size();
				MPI_Send(&size,1,MPI_INT,0,0,MPI_COMM_WORLD); //send buf size to master

				MPI_Send(const_cast<char*>(rec_buf.c_str()), //sending record
					rec_buf.size(),	//record size
					MPI_CHAR,
					0,	//destination
					1,  //tag
					MPI_COMM_WORLD);
			}
		}
	}

}


int main(int argc, char *argv[]) {
        double timer_start;
        double timer_end;
        int rank;
        int num_procs;

	if(argc!=2) { //Check input argument
                cout << "Usage: sort <sourceFile>\n";
                return -1;
        }
	

        // Initialization, get # of processes & this PID/rank
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	
	if(rank == MASTER)
		cout << "MPI sorting is started...\n";

	/*Start clock*/
	MPI_Barrier(MPI_COMM_WORLD);
	if(rank == MASTER)
		timer_start = MPI_Wtime();

	
	size_t nlines = 0;
	size_t record_size;
	if(rank == MASTER){
		master(rank,num_procs,argv[1],nlines,record_size);
	}
	else{
		worker(rank, num_procs, nlines, record_size);
	}


	//End timing
	MPI_Barrier(MPI_COMM_WORLD);
	if(rank == MASTER){
		timer_end = MPI_Wtime();

		cout << "\nElapsed time = " << (timer_end - timer_start)*1000 << " ms."
		<< endl;
	}

	//Shut down MPI and exit 
	MPI_Finalize();
	return 0;
}
