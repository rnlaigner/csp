#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ======== DEFINITIONS ======== */

#define VERBOSE 0
#define CARDINALITY 24
#define SLACK 1.5
#define CHUNK_PERC_PER_NUM_VALUES 0.1
#define DEFAULT_HASH 1

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
float * RUN;

/* store number of trials of the experiment */
int TRIALS;

/* ======== THREAD FUNCTION DECLARATIONS ======== */

void * writer(void *args);

/* ======== FUNCTION DECLARATIONS ======== */

void Acquire_Chunk(chunk_t * my_chunk);

Tuple * Alloc_Tuples();

chunk_t Alloc_Chunk();

void Collect_Timing_Info();

void Output_Timing_Result_To_File();

#if (DEFAULT_HASH == 0)
static inline int Hash(uint64_t i);
#endif

void Default_Chunks();

void Touch_Pages();

void Process_Input_And_Perform_Memory_Allocs(int argc, char *argv[]);

void Print_Output();

/* ======== THREAD FUNCTION IMPLEMENTATION ======== */

void * writer(void *args) {

    //printf("Writer called!");
    
    int i, start, end, id, partition, nxt_free;
    uint64_t key, payload;
    chunk_t my_chunk;

    if(args == NULL){
        fprintf(stderr, "ERROR: Cannot create thread without information passed as argument\n");
        exit(0); 
    }

    ThreadInfo * info = args;
    start = info->start;
    end = info->end;
    id = info->id;

#if (VERBOSE == 1)
    printf("Creating writer with id %d; start == %d end == %d\n", id, start, end);
#endif

    // printf("local_a == %d local_b == %d for thread %d\n",start,end, id);

    Acquire_Chunk(&my_chunk);

    //printf("Chunk acquired!");
	
    // For each value, mount the tuple and assigns it to given partition
    for (i = start; i <= end; i++) {
        
        // get last bits
        key = i & HASH_BITS;
#if (DEFAULT_HASH == 0)
        partition = Hash(key);
#else
        partition = key % NUM_PARTITIONS;
#endif

        // build tuple
        Tuple* tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        tuple->payload = (uint64_t) i;

        //printf("Checking if chunk is full for thread %d\n",id);

        // chunk is full, acquire another
        if(my_chunk->nxt_free > NUM_TUPLES_PER_CHUNK){
            printf("Chunk is full for thread %d\n",id);
            Acquire_Chunk(&my_chunk);
            printf("Chunk acquired is %d by thread %d\n",nxt_free_chunk,id);
        }        

        nxt_free = my_chunk->nxt_free;
        memcpy( &(my_chunk->tuples[nxt_free]), tuple, sizeof(Tuple) );
        my_chunk->nxt_free = nxt_free + 1;

        // free
        free(tuple);

    }
    
}

/* ======== FUNCTION IMPLEMENTATION ======== */

void Acquire_Chunk(chunk_t * my_chunk){
    pthread_mutex_lock(&chunk_acquiral_mutex);
    *my_chunk = (chunks[nxt_free_chunk]);
//    my_chunk = *(chunks[nxt_free_chunk]);
    nxt_free_chunk = nxt_free_chunk + 1;
    pthread_mutex_unlock(&chunk_acquiral_mutex);
    //printf("Chunk acquired!");
}

Tuple * Alloc_Tuples() {
    Tuple* tuples = malloc( NUM_TUPLES_PER_CHUNK * sizeof(Tuple) );
    return tuples;
}

chunk_t Alloc_Chunk() {
    chunk_t chunk = malloc( sizeof(Chunk) );
    chunk->tuples = Alloc_Tuples();
    chunk->nxt_free = 0;
    return chunk;
}

void Collect_Timing_Info(){

    //printf("Entered collect timing info\n");

    int i, trial;
    double time_spent;
    clock_t begin, end;
    int local_a, local_b;
    int start = 1;
    int aux = (NUM_VALUES / NUM_THREADS);

    for(trial = 0; trial < TRIALS; trial++){

        //printf("TRial %d\n",trial);

//        printf("start == %d aux == %d\n",start,aux);

        begin = clock();

        //printf("entering for llooop\n");
       
        // create threads; pass by parameter its respective range of values
        for(i = 0; i < NUM_THREADS; i++) {

            //printf("loop index %d\n",i);

            local_a = start + ((aux * (i+1)) - aux);     
            local_b = local_a + aux - 1;

            // TODO if i == NUM_THREADS - 1 then pega o resto da divisao de NUM_VALUES por NUM_THREADS a acrescenta a local_b

            
            if(i == (NUM_THREADS - 1) && (NUM_VALUES % NUM_THREADS) > 0){
                //printf("entreeeeei\n");
                //printf("%d\n",(NUM_VALUES % NUM_THREADS));
                //printf("local_b before %d\n",local_b);
                local_b = local_b + (NUM_VALUES % NUM_THREADS);
                       //local_b++;
                //printf("local_b after %d\n",local_b);
                
            } 
            //else {
            //    local_b = local_a + aux - 1;
            //}
    
            thread_info_array[i] = malloc( sizeof(ThreadInfo) );
            thread_info_array[i]->id = i;
            thread_info_array[i]->start = local_a;
            thread_info_array[i]->end = local_b;

            //printf("thread info array created!\n");

            if(pthread_create( &(writers[i]), NULL, writer, thread_info_array[i]) != 0) {
                fprintf(stderr, "ERROR: Cannot create thread # %d\n", i);
                exit(0);
            }

            //printf("thread created!\n");

        }

        for (i = 0; i < NUM_THREADS; i++) {
            pthread_join(writers[i], NULL);
        }

        end = clock();

        time_spent = (double) (end - begin) / CLOCKS_PER_SEC;

        RUN[trial] = time_spent;

#if (VERBOSE == 1)
        printf("Trial #%d took %f seconds to execute\n", trial, time_spent);
#endif

        // set default idx (0) for each chunk after a trial
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

#if (DEFAULT_HASH == 0)
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
#endif

void Default_Chunks(){
    int i;
    for(i = 0; i < NUM_CHUNKS; i++) {
        chunks[i]->nxt_free = 0;
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

void Process_Input_And_Perform_Memory_Allocs(int argc, char *argv[]){

    int i;

    printf("************************************************************************************\n");
    printf("Parameters expected are: (t) number of threads (b) hash bits (c) number of trials\n");

    // 3 arguments + number of arguments
    if(argc < 4) {
        printf("Number of arguments received are %d",argc);
        fprintf(stderr, "ERROR: Cannot proceed without proper input\n");
        exit(0);
    }

    printf("Parameters received are: %s, %s, and %s\n", argv[1], argv[2], argv[3]);

    NUM_THREADS = atoi(argv[1]);
    HASH_BITS = atoi(argv[2]);
    TRIALS = atoi(argv[3]);

// DEFAULT VALUES FOR TESTING PURPOSES
/*
    NUM_THREADS = 2;
    HASH_BITS = 3;
    TRIALS = 1;
*/

    RUN = malloc( TRIALS * sizeof(float) );

    NUM_VALUES = pow(2,CARDINALITY);

    // just to calculate hash key of each tuple
    NUM_PARTITIONS = pow(2,HASH_BITS);
    
    // set number of tuples per chunk given a threashold (chunk percentage per number of values)
    NUM_TUPLES_PER_CHUNK = NUM_VALUES * CHUNK_PERC_PER_NUM_VALUES;

    // set number of chunks
    NUM_CHUNKS = NUM_VALUES / NUM_TUPLES_PER_CHUNK;

    printf("Number of chunks == %d\n",NUM_CHUNKS);

    int NUM_CHUNKS_WITH_SLACK = NUM_CHUNKS * SLACK;

    // alloc array that stores reference to chunks
    chunks = malloc( NUM_CHUNKS_WITH_SLACK * sizeof(Chunk) );

    nxt_free_chunk = 0;

    // alloc maximum number of chunks necessary for computation
    for(i = 0; i < NUM_CHUNKS_WITH_SLACK; i++) {
        chunks[i] = Alloc_Chunk();
    }

    // allocate writers

    //printf("NUM_THREADS is %d\n",NUM_THREADS);

    writers = (pthread_t *)malloc(NUM_THREADS * sizeof(pthread_t));

    // allocate thread info
    thread_info_array = malloc(NUM_THREADS * sizeof(ThreadInfo));

    // init mutex
    pthread_mutex_init(&chunk_acquiral_mutex, NULL);

}

void Print_Output(){

    int i, j, idx;
    for(i = 0; i < NUM_CHUNKS; i++) {
        printf("Accessing chunk %d\n",i);
        for(j = 0; j < NUM_TUPLES_PER_CHUNK; j++){
            printf("Tuple idx %d value %ld\n",j,chunks[i]->tuples[j].payload);
        }
    }

}

/* ======== MAIN FUNCTION ======== */

int main(int argc, char *argv[]) {

    Process_Input_And_Perform_Memory_Allocs(argc, argv);

    Touch_Pages();

    Collect_Timing_Info();

    Output_Timing_Result_To_File();

#if (VERBOSE == 1)
    Print_Output();
#endif

    return 0;
}
