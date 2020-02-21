#include <stdint.h>
#include <stdio.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ======== DEFINITIONS ======== */

#define VERBOSE 1
#define CARDINALITY 4
#define SLACK 1.5
#define CHUNK_PERC_PER_NUM_VALUES 0.1
#define PRE_PROCESS_INPUT 1

/* ======== STRUCT DEFINITIONS ======== */

// 16 byte tuple
typedef struct tuple {
    // we only deal with positive numbers
    uint64_t key;
    uint64_t payload;
} Tuple;
// can use `Tuple* tp` instead of `struct tuple* tp`

typedef struct chunk {
    // next free position
    int nxt_free;
    // number of used tuples in the chunk
    int used;
} Chunk;

typedef Chunk * chunk_t;

typedef struct thread_info {
    int id;
    int start;
    int end;
} ThreadInfo;

typedef ThreadInfo * thread_info_t;

/* ======== ATTRIBUTE DECLARATIONS ======== */

/* input array of tuples */
Tuple * input;

/* array to store chunks */
chunk_t * chunks;

/* shared buffer of tuples */
Tuple * buffer;

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

/* chunk acquiral mutex */
pthread_mutex_t chunk_acquiral_mutex;

/* array that stores thread info for all threads */
thread_info_t * thread_info_array;

/* store timing information for each trial */
float * run;

/* store number of trials of the experiment */
int TRIALS;

/* ======== THREAD FUNCTION DECLARATIONS ======== */

void * writer(void *args);

/* ======== FUNCTION DECLARATIONS ======== */

void Process_Input();

void Acquire_Chunk(chunk_t * my_chunk);

chunk_t Alloc_Chunk(int chunk_idx);

void Collect_Timing_Info();

void Output_Timing_Result_To_File();

void Default_Chunks();

void Touch_Pages();

void Parse_Input_And_Perform_Memory_Allocs(int argc, char *argv[]);

void Print_Output();

/* ======== THREAD FUNCTION IMPLEMENTATION ======== */

void * writer(void *args) {
    
    int i, start, end, id, nxt_free;
    chunk_t my_chunk;
    Tuple * tuple;
#if(PRE_PROCESS_INPUT == 0)
    uint64_t key;
#endif

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

    Acquire_Chunk(&my_chunk);

#if (VERBOSE == 1)
    printf("Chunk acquired by thread %d\n",id);
#endif	

    // For each value, mount the tuple and assigns it to the given partition
    for (i = start; i <= end; i++) {
        
#if(PRE_PROCESS_INPUT == 0)
        // get last bits
        key = i & HASH_BITS;
        tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        tuple->payload = (uint64_t) i;
#else
        tuple = &input[i];
#endif

        // chunk is full, acquire another
        if(my_chunk->used >= NUM_TUPLES_PER_CHUNK){
            Acquire_Chunk(&my_chunk);
#if (VERBOSE == 1)
            printf("Chunk acquired by thread %d\n",id);
#endif	          
        }
        nxt_free = my_chunk->nxt_free;
        memcpy( &(buffer[nxt_free]), tuple, sizeof(Tuple) );
        my_chunk->nxt_free = nxt_free + 1;
        my_chunk->used++;

#if(PRE_PROCESS_INPUT == 0)
        free(tuple);
#endif
    }
    
}

/* ======== FUNCTION IMPLEMENTATION ======== */

void Process_Input(){
    int i;
    uint64_t key;
    Tuple * tuple;
    for(i = 0; i < NUM_VALUES;i++){
        key = i & HASH_BITS;
        tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        // it could also be a random number
        tuple->payload = (uint64_t) i;
        memcpy( &(input[i]), tuple, sizeof(Tuple) );
        free(tuple);
    }
}

void Acquire_Chunk(chunk_t * my_chunk){
    pthread_mutex_lock(&chunk_acquiral_mutex);
    *my_chunk = (chunks[nxt_free_chunk]);
    nxt_free_chunk = nxt_free_chunk + 1;
    pthread_mutex_unlock(&chunk_acquiral_mutex);
}

chunk_t Alloc_Chunk( int chunk_idx ) {
    chunk_t chunk = malloc( sizeof(Chunk) );
    chunk->nxt_free = chunk_idx * NUM_TUPLES_PER_CHUNK;
#if (VERBOSE == 1)
    printf("Chunk %d next free is %d\n",chunk_idx,chunk->nxt_free);
#endif
    chunk->used = 0;
    return chunk;
}

void Collect_Timing_Info(){
    int i, trial;
    double time_spent;
    clock_t begin, end;
    int local_a, local_b;
    int start = 0;
    int aux = (NUM_VALUES / NUM_THREADS);

    for(trial = 0; trial < TRIALS; trial++){

        begin = clock();
       
        // create threads; pass by parameter its respective range of values
        for(i = 0; i < NUM_THREADS; i++){

            local_a = start + ((aux * (i+1)) - aux);     
            local_b = local_a + aux - 1;
    
            thread_info_array[i] = malloc( sizeof(ThreadInfo) );
            thread_info_array[i]->id = i;
            thread_info_array[i]->start = local_a;
            thread_info_array[i]->end = local_b;

            if(pthread_create( &(writers[i]), NULL, writer, thread_info_array[i]) != 0) {
                fprintf(stderr, "ERROR: Cannot create thread # %d\n", i);
                exit(0);
            }

        }

        for (i = 0; i < NUM_THREADS; i++) {
            pthread_join(writers[i], NULL);
        }

        end = clock();

        time_spent = (double) (end - begin) / CLOCKS_PER_SEC;

        run[trial] = time_spent;

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
        sum = sum + run[trial];
    }

    avg = sum / TRIALS;

    fprintf(fptr,"%f",avg);
    fclose(fptr);

}

void Default_Chunks(){
    int i;
    for(i = 0; i < NUM_CHUNKS; i++) {
        chunks[i]->nxt_free = 0;
    }
}

// touch all pages before writing output
void Touch_Pages(){
    int i;
    // TODO do I need to touch it before each run or only one time before all runs?
    int v_touched;
    for(i = 0; i < NUM_VALUES; i++) {
#if (VERBOSE == 1)
        printf("Touching index %d shared buffer .. value is %ld\n",i,buffer[i].payload);
#endif
        v_touched = buffer[i].payload;
    }

}

void Parse_Input_And_Perform_Memory_Allocs(int argc, char *argv[]){

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

    if(NUM_THREADS % 2 > 0){
        fprintf(stderr, "ERROR: Cannot accept an odd number of threads\n");
        exit(0);
    }

    run = malloc( TRIALS * sizeof(float) );

    NUM_VALUES = pow(2,CARDINALITY);

    if(NUM_VALUES < NUM_THREADS || (NUM_VALUES % NUM_THREADS > 0 )){
        fprintf(stderr, "ERROR: Cannot accept number of threads provided (< NUM_VALUES or can't be evenly distributed)\n");
        exit(0);
    }
    
    // set number of tuples per chunk given a threashold (chunk percentage per number of values)
    NUM_TUPLES_PER_CHUNK = ceil(NUM_VALUES * CHUNK_PERC_PER_NUM_VALUES);

    // set number of chunks
    NUM_CHUNKS = NUM_VALUES / NUM_TUPLES_PER_CHUNK;

    int NUM_CHUNKS_WITH_SLACK = NUM_CHUNKS * SLACK;

    // alloc array that stores reference to chunks
    chunks = malloc( NUM_CHUNKS_WITH_SLACK * sizeof(Chunk) );

    nxt_free_chunk = 0;

    // alloc maximum number of chunks necessary for computation
    for(i = 0; i < NUM_CHUNKS_WITH_SLACK; i++) {
        chunks[i] = Alloc_Chunk(i);
    }

    // allocate shared buffer
    buffer = malloc( NUM_VALUES * sizeof(Tuple) * SLACK );

    // allocate writers
    writers = (pthread_t *)malloc(NUM_THREADS * sizeof(pthread_t));

    // allocate thread info
    thread_info_array = malloc(NUM_THREADS * sizeof(ThreadInfo));

    // init mutex
    pthread_mutex_init(&chunk_acquiral_mutex, NULL);

#if(PRE_PROCESS_INPUT == 1)
    input = malloc( NUM_VALUES * sizeof(Tuple) );
#endif

}

void Print_Output(){

    int i;
    for(i = 0; i < NUM_VALUES; i++) {
        printf("Tuple idx %d value %ld\n",i,buffer[i].payload);
    }

}

/* ======== MAIN FUNCTION ======== */

int main(int argc, char *argv[]) {

    Parse_Input_And_Perform_Memory_Allocs(argc, argv);

#if(PRE_PROCESS_INPUT == 1)
    Process_Input();
#endif

    Touch_Pages();

    Collect_Timing_Info();

    Output_Timing_Result_To_File();

#if (VERBOSE == 1)
    Print_Output();
#endif

    return 0;
}
