#include <config.h>
#include <stddef.h>
#include <stdio.h>
#include "lisp.h"
int main(void) {
  printf("{\"sizeof\":%zu,\"alignment\":%zu,\"tag\":%d,"
         "\"index\":%zu,\"hash\":%zu,\"key_and_value\":%zu,"
         "\"test\":%zu,\"next\":%zu,\"count\":%zu,"
         "\"next_free\":%zu,\"table_size\":%zu,\"index_bits\":%zu,"
         "\"next_weak\":%zu,\"hash_idx_size\":%zu,\"hash_code_size\":%zu}\n",
         sizeof(struct Lisp_Hash_Table), _Alignof(struct Lisp_Hash_Table), PVEC_HASH_TABLE,
         offsetof(struct Lisp_Hash_Table,index), offsetof(struct Lisp_Hash_Table,hash),
         offsetof(struct Lisp_Hash_Table,key_and_value), offsetof(struct Lisp_Hash_Table,test),
         offsetof(struct Lisp_Hash_Table,next), offsetof(struct Lisp_Hash_Table,count),
         offsetof(struct Lisp_Hash_Table,next_free), offsetof(struct Lisp_Hash_Table,table_size),
         offsetof(struct Lisp_Hash_Table,index_bits), offsetof(struct Lisp_Hash_Table,next_weak),
         sizeof(hash_idx_t), sizeof(hash_hash_t));
  return 0;
}
