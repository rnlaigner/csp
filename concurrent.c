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
float RUN[TRIALS];

/* ======== THREAD FUNCTION DECLARATIONS ======== */

void * writer(void *args);

/* ======== FUNCTION DECLARATIONS ======== */

Tuple * Alloc_Tuples();

partition_t Alloc_Partition();

void Collect_Timing_Info();

void Output_Timing_Result_To_File();

/* ======== FUNCTION IMPLEMENTATION ======== */

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

    for(trial = 1; trial <= TRIALS; trial++){

        int start = 1;
        int aux = NUM_VALUES / NUM_THREADS;
        clock_t begin = clock();
       
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

        clock_t end = clock();

        double time_spent = (double) (end - begin) / CLOCKS_PER_SEC;

        RUN[trial] = time_spent;
#if (VERBOSE == 1)
        printf("Trial #%d took %f seconds to execute\n", trial, time_spent);
#endif
        //printf("It took %f seconds to execute \n", time_spent);
    }

}

void Output_Timing_Result_To_File(){

}

/* ======== THREAD FUNCTION IMPLEMENTATION ======== */

void * writer(void *args) {
    
    int i, start, end, id, partition, nxtfree;
    uint64_t key, payload;
    double s, x;

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

        // hash calc by multiplication method
        // h(k) = floor(m * (kA - floor(kA)))
	    s = i * A;
        x = s - floor(s);
        partition = floor( NUM_PARTITIONS * x );

        // build tuple
        Tuple* tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        tuple->payload = (uint64_t) i;

#if (VERBOSE == 2)
        printf("Tentative to access partition %d from thread %d\n",partition,id);
#endif

        // access partitions
        pthread_mutex_lock(&partitions[partition]->mutex);

        nxtfree = partitions[partition]->nxtfree;

        memcpy( &(partitions[partition]->tuples[nxtfree]), tuple, sizeof(Tuple) );

        partitions[partition]->nxtfree = nxtfree + 1;

        pthread_mutex_unlock(&partitions[partition]->mutex);

	    // free
        free(tuple);

#if (VERBOSE == 2)
        printf("Left mutex of partition %d from thread %d\n",partition,id);        
#endif
    }
}


/* ======== MAIN FUNCTION ======== */

int main(int argc, char *argv[]) {

    int i, j;

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

    // touch all pages before writing output
    // TODO do I need to touch it before each run or only one time before all runs?
    int v_touched;
    for(i = 0; i < NUM_PARTITIONS; i++) {
        // TODO touch every index of every partition is not time-effective.. is the right thing to do?
        //for(j = 0; j < partition_sz; j++){
        for(j = 0; j < 1; j++){
#if (VERBOSE == 2)
            printf("Touching index %d of partition %d .. value is %ld\n",j,i,partitions[i]->tuples[j].payload);
#endif
            v_touched = partitions[i]->tuples[j].payload;
        }
    }

    Collect_Timing_Info();

    Output_Timing_Result_To_File();

#if (VERBOSE == 1)

    int idx;

    /*
    // Exhibit number of elements per partition
    for(i = 0; i < NUM_PARTITIONS; i++) {
        idx = partitions[i]->nxtfree;
	printf("Accessing partition %d\n",i);
        printf("Number of elements == %d\n", idx);
    }    
   
    // teste para ver o que esta em cada particao
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
