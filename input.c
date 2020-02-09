#include <stdint.h>

#include <stdio.h>

#include <math.h>

#include <pthread.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ======== DEFINITIONS ======== */

#define VERBOSE 1



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



/* number of hash bits to be used */

int HASH_BITS;



/* number of partitions */

int NUM_PARTITIONS;



/* number of positions in a partition */

int PARTITION_SIZE;



/* array that stores thread info for all threads */

thread_info_t * thread_info_array;



/* ======== THREAD FUNCTION DECLARATIONS ======== */



void * writer(void *args);



/* ======== FUNCTION DECLARATIONS ======== */



Tuple * Alloc_Tuples();

partition_t Alloc_Partition();



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


#if (VERBOSE == 1)

        printf("Tentative to access partition %d from thread %d\n",partition,id);

#endif


        // access partitions

        pthread_mutex_lock(&partitions[partition]->mutex);



        nxtfree = partitions[partition]->nxtfree;


        // Which method to use?
        memcpy( &(partitions[partition]->tuples[nxtfree]), tuple, sizeof(Tuple) );

        // partitions[partition]->tuples[nxtfree] = *tuple;



        partitions[partition]->nxtfree = nxtfree + 1;



        pthread_mutex_unlock(&partitions[partition]->mutex);


#if (VERBOSE == 1)

        printf("Left mutex of partition %d from thread %d\n",partition,id);        

#endif

    }

}





/* ======== MAIN FUNCTION ======== */



int main(int argc, char *argv[]) {



    int NUM_THREADS, CARDINALITY;

    int NUM_VALUES;

    int i;



    printf("************************************************************************************\n");

    printf("Parameters expected are: (t) number of threads (b) hash bits (c) cardinality expoent\n");



    if(argc == 1) {

        fprintf(stderr, "ERROR: Cannot proceed without proper input\n");

        exit(0);

    }



    printf("Parameters received are: %s, %s, and %s\n", argv[1], argv[2], argv[3]);



    NUM_THREADS = atoi(argv[1]);

    HASH_BITS = atoi(argv[2]);

    CARDINALITY = atoi(argv[3]);


// DEFAULT VALUES FOR TESTING PURPOSES

/*

    NUM_THREADS = 2;

    HASH_BITS = 3;

    CARDINALITY = 24;

*/

    NUM_VALUES = pow(2,CARDINALITY);

    NUM_PARTITIONS = pow(2,HASH_BITS);



    // if we consider our hash function divides the tuples to partitions evenly,

    // then we can increase 50% the size of the partition in order to not deal

    // with resizing during partitioning process

    PARTITION_SIZE = (NUM_VALUES / NUM_THREADS) + ( (NUM_VALUES / NUM_THREADS) / 2 );



    // alloc array that stores reference to output partitions

    partitions = malloc( NUM_PARTITIONS * sizeof(Partition) );



    // alloc output partitions

    for(i = 0; i < NUM_PARTITIONS; i++) {

        partitions[i] = Alloc_Partition();

    }



    int start = 1;

    int aux = NUM_VALUES / NUM_THREADS;



    // allocate writers

    writers = (pthread_t *)malloc(NUM_THREADS * sizeof(pthread_t));



    thread_info_array = malloc(NUM_THREADS * sizeof(ThreadInfo));



    // TODO touch all pages before writing output

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

    double time_spent = (end - begin) / CLOCKS_PER_SEC;


#if (VERBOSE == 1)
    printf("Elapsed: %f seconds\n", (double));


    int idx, j;

    // Exhibit number of elements per partition

    for(i = 0; i < NUM_PARTITIONS; i++) {

        idx = partitions[i]->nxtfree;
	printf("Accessing partition %d\n",i);

        printf("Number of elements == %d\n", idx);

    }    

   

    // teste para ver o que esta em cada particao
/*

    for(i = 0; i < NUM_PARTITIONS; i++) {

        printf("Accessing partition %d\n",i);

        idx = partitions[i]->nxtfree;

        for(j = idx - 1; j>= 0; j--){

            printf("Tuple idx %d value %ld\n",j,partitions[i]->tuples[j].payload);

        }

    }
*/

#endif

    // TODO output time spent



}