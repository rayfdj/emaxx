#include <config.h>
#include "lisp.h"
#include <stddef.h>
#include <stdio.h>
int main(void) {
  printf("{\"char_table\":{\"tag\":%d,\"size\":%zu,\"default\":%zu,\"parent\":%zu,\"purpose\":%zu,\"ascii\":%zu,\"contents\":%zu,\"extras\":%zu,\"standard_slots\":%d},\"sub_char_table\":{\"tag\":%d,\"size\":%zu,\"depth\":%zu,\"min_char\":%zu,\"contents\":%zu,\"slot_offset\":%d},\"bits\":[%d,%d,%d,%d]}\n",
         PVEC_CHAR_TABLE, sizeof(struct Lisp_Char_Table),
         offsetof(struct Lisp_Char_Table, defalt), offsetof(struct Lisp_Char_Table, parent),
         offsetof(struct Lisp_Char_Table, purpose), offsetof(struct Lisp_Char_Table, ascii),
         offsetof(struct Lisp_Char_Table, contents), offsetof(struct Lisp_Char_Table, extras),
         CHAR_TABLE_STANDARD_SLOTS,
         PVEC_SUB_CHAR_TABLE, sizeof(struct Lisp_Sub_Char_Table),
         offsetof(struct Lisp_Sub_Char_Table, depth), offsetof(struct Lisp_Sub_Char_Table, min_char),
         offsetof(struct Lisp_Sub_Char_Table, contents), SUB_CHAR_TABLE_OFFSET,
         CHARTAB_SIZE_BITS_0, CHARTAB_SIZE_BITS_1, CHARTAB_SIZE_BITS_2, CHARTAB_SIZE_BITS_3);
  return 0;
}
