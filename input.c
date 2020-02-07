#include <stdint.h>

#include <stdio.h>

#include <math.h>

#include <pthread.h>
#include <stdlib.h>

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







/* ======== FUNCTION DECLARATIONS ======== */







/* ======== FUNCTION IMPLEMENTATION ======== */



Tuple * Alloc_Tuples() {

    Tuple* tuples = malloc( PARTITION_SIZE * sizeof(Tuple) );

    return tuples;

}



partition_t Alloc_Partition(){

    

    partition_t partition = malloc( sizeof(Partition) );

    partition->tuples = Alloc_Tuples();

    partition->nxtfree = 0;

    

    // TODO verify if it works    

    pthread_mutex_init(&partition->mutex, NULL);



    return partition;



}

/*
void Insert_Partition(int pos, Tuple * tuple){
    
    pthread_mutex_lock(&e);

    partitions[pos]->mutex

}
*/

int h(uint64_t n){
    // h(k) = floor(m * (kA - floor(kA)))
    int pos = floor( NUM_PARTITIONS * ( n * A - floor(n * A)  ) );
    // printf("Positions based on hash function is: %d",pos);
    return pos;
}



/* ======== THREAD FUNCTION IMPLEMENTATION ======== */



void * writer(void *args) {
    
    int i, start, end, id, partition, nxtfree;
    uint64_t key, payload;

/*

    int * idx_t = (int *)args[0];

    int idx = *idx_t;


	int * start_t = (int *)args[1];
    int start = *start_t;


    int * end_t = (int *)args[2];

	int end = *end_t;
*/

    ThreadInfo * info = args;
    start = info->start;
    end = info->end;
    id = info->id;


	printf("Creating writer with id %d; start == %d end == %d\n", id, start, end);

	

    // For each value, mount the tuple and assigns it to given partition

    for (i = start; i <= end; i++) {

        

        // get last bits

        key = i & HASH_BITS;

        // hash calc
        partition = h(key);

        // build tuple
        Tuple* tuple = malloc( sizeof(Tuple) );
        tuple->key = key;
        tuple->payload = (uint64_t) i;

        // access partitions
        //Insert_Partition(partition, tuple);
        pthread_mutex_lock(&partitions[partition]->mutex);

        nxtfree = partitions[partition]->nxtfree;

        partitions[partition]->tuples[nxtfree] = *tuple;

        partitions[partition]->nxtfree = nxtfree + 1;

        pthread_mutex_unlock(&partitions[partition]->mutex);
        



    }

}





/* ======== MAIN FUNCTION ======== */



int main(int argc, char *argv[]) {



    int NUM_THREADS, CARDINALITY;

    int NUM_VALUES;

    int i;




    printf("************************************************************************************\n");

    printf("Parameters expected are: (t) number of threads (b) hash bits (c) cardinality expoent\n");

/*
    if(argv[1] && argv[2] && argv[3]){} else {
        fprintf(stderr, "ERROR: Cannot proceed without proper input\n");

        exit(0);
    }



    printf("Parameters received are: %c, %c, and %c\n", *argv[1], *argv[2], *argv[3]);


    printf("Parsing input ...\n");


    NUM_THREADS = atoi(argv[1]);

    HASH_BITS = atoi(argv[2]);

    CARDINALITY = atoi(argv[3]);
*/

    NUM_THREADS = 1;

    HASH_BITS = 3;

    CARDINALITY = 10;//24;
    

    NUM_VALUES = pow(2,CARDINALITY);

    NUM_PARTITIONS = pow(2,HASH_BITS);



    // if we consider our hash function divides the tuples to partitions evenly,

    // then we can increase 50% the size of the partition in order to not deal

    // with resizing during partitioning process

    PARTITION_SIZE = (NUM_VALUES / NUM_THREADS) + ( (NUM_VALUES / NUM_THREADS) / 2 );



    // alloc array that stores reference to output partitions

    partitions = malloc( NUM_PARTITIONS * sizeof(Partition) );


    printf("Start allocation of partitions");


    // alloc output partitions

    for(i = 0; i < NUM_PARTITIONS; i++) {

        partitions[i] = Alloc_Partition();

    }

    printf("Partitions allocation finished");


    int start = 1;

    int aux = NUM_VALUES / NUM_THREADS;

    printf("Aux is %d\n",aux);

    // allocate writers
    writers = (pthread_t *)malloc(NUM_THREADS * sizeof(pthread_t));

    thread_info_array = malloc(NUM_THREADS * sizeof(ThreadInfo));

   

    // create threads; pass by parameter its respective range of values

    for(i = 1; i <= NUM_THREADS; i++) {
        thread_info_array[i] = malloc( sizeof(ThreadInfo) );
        thread_info_array[i]->id = i;
        thread_info_array[i]->start = start;
        thread_info_array[i]->end = aux;

        pthread_create(&(writers[i]), NULL, writer, thread_info_array[i]);

        start = (aux * i) + 1;

        aux = aux * i;

    }



    for (i = 1; i <= NUM_THREADS; i++) {

        pthread_join(writers[i], NULL);

    }

    // Exhibit number of elements per partition
    

    int idx, j;
    // teste para ver o que esta em cada particao
    for(i = 0; i < NUM_PARTITIONS; i++) {

        printf("Accessing partition %d\n",i);


        idx = partitions[i]->nxtfree;
        
        for(j = idx - 1; j>= 0; j--){
            
            printf("Tuple idx %d value %ld\n",j,partitions[i]->tuples[j].payload);

        }
        


    }


}