#include <stdint.h>

#include <stdio.h>

#include <math.h>
#include<pthread.h>



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

/* ======== THREAD FUNCTION DECLARATIONS ======== */



/* ======== FUNCTION DECLARATIONS ======== */



/* ======== FUNCTION IMPLEMENTATION ======== */

Tuple * Alloc_Tuples(int n) {
    Tuple* tuples = malloc( n * sizeof(Tuple) );
    return tuples;
}

Partition * Alloc_Partition(int n){
    
    Partition * partition = malloc(sizeof(Partition));
    partition->tuples = Alloc_Tuples(n);
    partition->nxtfree = 0;
    
    // TODO verify if it works    
    pthread_mutex_init(&partition->mutex, NULL);

    return partition;

}

/* ======== THREAD FUNCTION IMPLEMENTATION ======== */





/* ======== MAIN FUNCTION ======== */



int main(int argc, char *argv[]) {



    int NUM_THREADS, HASH_BITS, CARDINALITY;

    int NUM_VALUES, NUM_PARTITIONS, PARTITION_SIZE;

    int i;
    Partition* partitions;



    printf("************************************************************************************\n");

    printf("Parameters expected are: (t) number of threads (b) hash bits (c) cardinality expoent");


    // get three last bits

    //int val = 11 & 3;

    //printf("%d", val);



    printf("Parameters received are: %c, %c, and %c\n", *argv[1], *argv[2], *argv[3]);



    // parsing input

    NUM_THREADS = atoi(argv[1]);

    HASH_BITS = atoi(argv[2]);

    CARDINALITY = atoi(argv[3]);



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
        partitions[i] = Alloc_Partition(PARTITION_SIZE);
    }
    



    // For each value, mount the tuple and assigns it to given partition

    for (i = 1; i <= NUM_VALUES; i++) {

        

        



    }



}