#include <config.h>
#include <stddef.h>
#include <stdio.h>
#include "lisp.h"
int main(void) {
  struct Lisp_Hash_Table h = {0};
  h.weakness = Weak_Key; h.frozen_test = Test_equal;
  h.purecopy = true; h.mutable = true;
  const unsigned char *bytes = (const unsigned char *)&h;
  printf("{\"vec_payload_words\":%zu,\"flag_byte_offset\":%zu,\"combined_flags\":%u,\"test_descriptor_size\":%zu,\"test_name_offset\":%zu}\n",
    (size_t)VECSIZE(struct Lisp_Hash_Table),offsetof(struct Lisp_Hash_Table,index_bits)+1,bytes[offsetof(struct Lisp_Hash_Table,index_bits)+1],sizeof(struct hash_table_test),offsetof(struct hash_table_test,name));
}
