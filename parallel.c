#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ======== DEFINITIONS ======== */

#define VERBOSE 1
#define CARDINALITY 24
#define TRIALS 8
#define SLACK 1.5
#define CHUNK_PERC_NUM_VALUES 0.1

/* ======== DEFAULT VARIABLE VALUES ======== */

// see Knuth, "Sorting and Searching", v. 3 of "The Art of Computer Programming"
double A = 0.6180339887;

/* ======== STRUCT DEFINITIONS ======== */

// 16 byte tuple
typedef struct tuple {
    // we only deal with positive numbers
    uint64_t key;
    uint64_t payload;    
} Tuple;
// can use `Tuple* tp` instead of `struct tuple* tp`

typedef struct chunk {
    int nxt_free;
    Tuple* tuples;
} Chunk;

typedef Chunk * chunk_t;

typedef struct thread_info {
    int id;
    int start;
    int end;
} ThreadInfo;

typedef ThreadInfo * thread_info_t;

/* ======== ATTRIBUTE DECLARATIONS ======== */

/* array to store chunks */
chunk_t * chunks;

/* next free chunk available */
int nxt_free_chunk;

/* array to store writers */
pthread_t * writers;

/* number of threads to execute */
int NUM_THREADS;

/* number of hash bits to be used */
int HASH_BITS;

/* number of chunks */
int NUM_CHUNKS;

/* number of tuples in a chunk */
int NUM_TUPLES_PER_CHUNK;

/* number of values to be generated, based on the cardinality */
int NUM_VALUES;

/* number of partition ... just to calculate the hash key of the tuple */
int NUM_PARTITIONS;

/* chunk acquiral mutex */
pthread_mutex_t chunk_acquiral_mutex;

/* array that stores thread info for all threads */
thread_info_t * thread_info_array;

/* store timing information for each trial */
float RUN[TRIALS];

/* ======== THREAD FUNCTION DECLARATIONS ======== */

void * writer(void *args);

/* ======== FUNCTION DECLARATIONS ======== */

Tuple * Alloc_Tuples();

chunk_t Alloc_Chunk();

void Collect_Timing_Info();

void Output_Timing_Result_To_File();

static inline int Hash(uint64_t i);

void Default_Chunks();

void Touch_Pages();

/* ======== THREAD FUNCTION IMPLEMENTATION ======== */

void * writer(void *args) {
    
    int i, start, end, id, partition, nxt_free;
    uint64_t key, payload;
    chunk_t my_chunk;

    ThreadInfo * info = args;
    start = info->start;
    end = info->end;
    id = info->id;

#if (VERBOSE == 1)
    printf("Creating writer with id %d; start == %d end == %d\n", id, start, end);
#endif
	
    // For each value, mount the tuple and assigns it to given partition
    for (i = start; i <= end; i++) {
        
        // get last bits
        key = i & HASH_BITS;
        //partition = Hash(key);
        partition = key % NUM_PARTITIONS;

        // build tuple
        Tuple* tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        tuple->payload = (uint64_t) i;

        // if I don't have a chunk yet or chunk is full, acquire it
        if(my_chunk == NULL || my_chunk->nxt_free > NUM_TUPLES_PER_CHUNK){
            pthread_mutex_lock(&chunk_acquiral_mutex);
            my_chunk = chunks[nxt_free_chunk];
            nxt_free_chunk = nxt_free_chunk + 1;
            pthread_mutex_unlock(&chunk_acquiral_mutex);
        }

        nxt_free = my_chunk->nxt_free;
        memcpy( &(my_chunk->tuples[nxt_free]), tuple, sizeof(Tuple) );
        my_chunk->nxt_free = nxt_free + 1;

	    // free
        free(tuple);

    }
    
}

/* ======== FUNCTION IMPLEMENTATION ======== */

Tuple * Alloc_Tuples() {
    Tuple* tuples = malloc( NUM_TUPLES_PER_CHUNK * sizeof(Tuple) );
    return tuples;
}

chunk_t Alloc_Chunk() {
    chunk_t chunk = malloc( sizeof(Chunk) );
    chunk->tuples = Alloc_Tuples();
    return chunk;
}

void Collect_Timing_Info(){

    int i, trial;
    double time_spent;
    clock_t begin, end;

    for(trial = 0; trial < TRIALS; trial++){

        int start = 1;
        int aux = NUM_VALUES / NUM_THREADS;
        begin = clock();
       
        // create threads; pass by parameter its respective range of values
        for(i = 1; i <= NUM_THREADS; i++) {
            thread_info_array[i] = malloc( sizeof(ThreadInfo) );
            thread_info_array[i]->id = i;
            thread_info_array[i]->start = start;
            thread_info_array[i]->end = aux;
            pthread_create(&(writers[i]), NULL, writer, thread_info_array[i]);
            start = (aux * i) + 1;
            aux = aux * (i + 1);
        }

        for (i = 1; i <= NUM_THREADS; i++) {
            pthread_join(writers[i], NULL);
        }

        end = clock();

        time_spent = (double) (end - begin) / CLOCKS_PER_SEC;

        RUN[trial] = time_spent;
#if (VERBOSE == 1)
        printf("Trial #%d took %f seconds to execute\n", trial, time_spent);
#endif

        // set default idx (0) for each partition after a trial to avoid segmentation fault
        Default_Chunks();

    }

}

void Output_Timing_Result_To_File(){

    FILE *fptr;
    char filename[100];
    sprintf(filename, "%d_%d.txt", NUM_THREADS, HASH_BITS);

    fptr = fopen(filename,"w");

    if(fptr == NULL)
    {
        printf("Error!");
        exit(1);    
    }

    int trial;
    float avg, sum;
    sum = 0.0;

    for(trial = 0; trial < TRIALS; trial++){
        sum = sum + RUN[trial];
    }

    avg = sum / TRIALS;

    fprintf(fptr,"%f",avg);
    fclose(fptr);

}

// instructs the compiler to embed the function
static inline int Hash(uint64_t i){
    double s, x;
    // hash calc by multiplication method
    // h(k) = floor(m * (kA - floor(kA)))
    s = i * A;
    x = s - floor(s);
    int partition = floor( NUM_PARTITIONS * x );
    return partition;
}

void Default_Chunks(){
    int i;
    for(i = 0; i < NUM_CHUNKS; i++) {
        //chunks[i]->nxtfree = 0;
    }
}

// touch all pages before writing output
void Touch_Pages(){
    int i, j;
    // TODO do I need to touch it before each run or only one time before all runs?
    int v_touched;
    for(i = 0; i < NUM_CHUNKS; i++) {
        // TODO touch every index of every partition is not time-effective.. is the right thing to do?
        //for(j = 0; j < partition_sz; j++){
        for(j = 0; j < 1; j++){
#if (VERBOSE == 2)
            printf("Touching index %d of chunk %d .. value is %ld\n",j,i,chunks[i]->tuples[j].payload);
#endif
            v_touched = chunks[i]->tuples[j].payload;
        }
    }

}

/* ======== MAIN FUNCTION ======== */

int main(int argc, char *argv[]) {

    int i;

    printf("************************************************************************************\n");
    printf("Parameters expected are: (t) number of threads (b) hash bits\n");

    if(argc == 1) {
        fprintf(stderr, "ERROR: Cannot proceed without proper input\n");
        exit(0);
    }

    printf("Parameters received are: %s and %s\n", argv[1], argv[2]);

    NUM_THREADS = atoi(argv[1]);
    HASH_BITS = atoi(argv[2]);

// DEFAULT VALUES FOR TESTING PURPOSES
/*
    NUM_THREADS = 2;
    HASH_BITS = 3;
*/

    NUM_VALUES = pow(2,CARDINALITY);

    // just to calculate hash key of each tuple
    NUM_PARTITIONS = pow(2,HASH_BITS);
    
    NUM_TUPLES_PER_CHUNK = NUM_VALUES * CHUNK_PERC_NUM_VALUES;

    // alloc array that stores reference to chunks
    chunks = malloc( NUM_CHUNKS * sizeof(Chunk) * SLACK );

    nxt_free_chunk = 0;

    // alloc maximum number of chunks necessary for computation
    for(i = 0; i < NUM_CHUNKS; i++) {
        chunks[i] = Alloc_Chunk();
    }

    // allocate writers
    writers = (pthread_t *)malloc(NUM_THREADS * sizeof(pthread_t));

    // allocate thread info
    thread_info_array = malloc(NUM_THREADS * sizeof(ThreadInfo));

    Touch_Pages();

    Collect_Timing_Info();

    Output_Timing_Result_To_File();

#if (VERBOSE == 1)

    int idx;
    /*   
    // teste para ver o que esta em cada chunk
    for(i = 0; i < NUM_PARTITIONS; i++) {
        printf("Accessing partition %d\n",i);
        idx = partitions[i]->nxtfree;
        for(j = idx - 1; j>= 0; j--){
            printf("Tuple idx %d value %ld\n",j,partitions[i]->tuples[j].payload);
        }
    }
    */
    
#endif

    return 0;
}
