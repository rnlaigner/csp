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

typedef struct partition {
    int nxtfree;
    Tuple* tuples;
    // TODO change to atomic intrinsics
    pthread_mutex_t mutex;
} Partition;

typedef Partition * partition_t;

typedef struct thread_info {
    int id;
    int start;
    int end;
} ThreadInfo;

typedef ThreadInfo * thread_info_t;

/* ======== ATTRIBUTE DECLARATIONS ======== */

/* input array of tuples */
Tuple * input;

/* array to store output partitions */
partition_t * partitions;

/* array to store writers */
pthread_t * writers;

/* number of threads to execute */
int NUM_THREADS;

/* number of hash bits to be used */
int HASH_BITS;

/* number of partitions */
int NUM_PARTITIONS;

/* number of positions in a partition */
int PARTITION_SIZE;

/* number of values to be generated, based on the cardinality */
int NUM_VALUES;

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

Tuple * Alloc_Tuples();

partition_t Alloc_Partition();

void Collect_Timing_Info();

void Output_Timing_Result_To_File();

static inline int Hash(uint64_t i);

void Default_Output_Partitions();

void Touch_Partitions();

void Parse_Input_And_Perform_Memory_Allocs(int argc, char *argv[]);

void Print_Output();

/* ======== THREAD FUNCTION IMPLEMENTATION ======== */

void * writer(void *args) {
    
    int i, start, end, id, partition, nxtfree;
    Tuple * tuple;

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
	
    // For each value, mount the tuple and assigns it to given partition
    for (i = start; i <= end; i++) {
        
        tuple = &input[i];

        //partition = tuple->key % NUM_PARTITIONS;
        partition = Hash(i);

        // access partitions
        pthread_mutex_lock(&partitions[partition]->mutex);

        nxtfree = partitions[partition]->nxtfree;

        memcpy( &(partitions[partition]->tuples[nxtfree]), tuple, sizeof(Tuple) );

        partitions[partition]->nxtfree = nxtfree + 1;

        pthread_mutex_unlock(&partitions[partition]->mutex);

    }
    
}

/* ======== FUNCTION IMPLEMENTATION ======== */

void Process_Input(){
    int i;
    uint64_t key;
    Tuple * tuple;
    for(i = 0; i < NUM_VALUES;i++){
        // get last bits
        key = i & HASH_BITS;        

#if(VERBOSE == 1)
        printf("Value %d, hash %ld\n",i,key);
#endif
        tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        // it could also be a random number
        tuple->payload = (uint64_t) i;
        memcpy( &(input[i]), tuple, sizeof(Tuple) );
        free(tuple);
    }
}

Tuple * Alloc_Tuples() {
    Tuple* tuples = malloc( PARTITION_SIZE * sizeof(Tuple) );
    return tuples;
}

partition_t Alloc_Partition() {
    partition_t partition = malloc( sizeof(Partition) );
    partition->tuples = Alloc_Tuples();
    pthread_mutex_init(&partition->mutex, NULL);
    return partition;
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
       
        // create threads; pass by parameter its respective range of indexes
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

        // set default idx (0) for each partition after a trial to avoid segmentation fault
        Default_Output_Partitions();

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

// static inline instructs the compiler to embed the function in the caller function
static inline int Hash(uint64_t i){
    double s, x;
    // hash calc by multiplication method
    // h(k) = floor(m * (kA - floor(kA)))
    s = i * A;
    x = s - floor(s);
    int partition = floor( NUM_PARTITIONS * x );
    return partition;
}

void Default_Output_Partitions(){
    int i;
    for(i = 0; i < NUM_PARTITIONS; i++) {
        partitions[i]->nxtfree = 0;
    }
}

// touch all pages before writing output
void Touch_Partitions(){
    int i, j;
    // TODO do I need to touch it before each run or only one time before all runs?
    int v_touched;
    for(i = 0; i < NUM_PARTITIONS; i++) {
        // TODO touch every index of every partition is not time-effective.. is the right thing to do?
        //for(j = 0; j < partition_sz; j++){
        for(j = 0; j < 1; j++){
#if(VERBOSE == 1)
            printf("Touching index %d of partition %d .. value is %ld\n",j,i,partitions[i]->tuples[j].payload);
#endif
            v_touched = partitions[i]->tuples[j].payload;
        }
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
    NUM_PARTITIONS = pow(2,HASH_BITS);

    /* if we consider our hash function divides the tuples to partitions evenly,
     then we can increase 50% the size of the partition in order to not deal
     with resizing during partitioning process */
    int partition_sz = (NUM_VALUES / NUM_THREADS);
    PARTITION_SIZE = partition_sz * SLACK;

    // alloc array that stores reference to output partitions
    partitions = malloc( NUM_PARTITIONS * sizeof(Partition) );

    // alloc output partitions
    for(i = 0; i < NUM_PARTITIONS; i++) {
        partitions[i] = Alloc_Partition();
    }

    // allocate writers
    writers = (pthread_t *)malloc(NUM_THREADS * sizeof(pthread_t));

    // allocate thread info
    thread_info_array = malloc(NUM_THREADS * sizeof(ThreadInfo));

    // allocate input buffer
    input = malloc( NUM_VALUES * sizeof(Tuple) );

}

void Print_Output(){

    int i, j, idx;
    // Exhibit number of elements per partition
    for(i = 0; i < NUM_PARTITIONS; i++) {
        idx = partitions[i]->nxtfree;
	    printf("Accessing partition %d\n",i);
        printf("Number of elements == %d\n", idx);
    }    
   
    // teste para ver o que esta em cada particao
    for(i = 0; i < NUM_PARTITIONS; i++) {
        printf("Accessing partition %d\n",i);
        for(j = 0; j < PARTITION_SIZE; j++){
            printf("Tuple idx %d value %ld\n",j,partitions[i]->tuples[j].payload);
        }
    }

}

void Disalloc(){
    
    // TODO iterate through partitions and input
    // free(tuple) free(&input[i])
    
}

/* ======== MAIN FUNCTION ======== */

int main(int argc, char *argv[]) {

    Parse_Input_And_Perform_Memory_Allocs(argc, argv);

    Process_Input();

    Touch_Partitions();

    Collect_Timing_Info();

    Output_Timing_Result_To_File();

#if (VERBOSE == 1)
    Print_Output();
#endif

    return 0;
}
