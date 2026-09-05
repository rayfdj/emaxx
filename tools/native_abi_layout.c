/* Test-only layout probe for the configured GNU Emacs headers.
   Compile with the oracle's emacs.c flags, without linking GNU runtime code.
   These are ABI and configured-path facts consumed by generated .eln files
   and their normal lookup, not Lisp behavior. */
#include <config.h>
#include <stddef.h>
#include <stdio.h>
#include "lisp.h"
#include "thread.h"
#include "epaths.h"

int
main (void)
{
  printf ("LISP_CONS_SIZE=%zu\n", sizeof (struct Lisp_Cons));
  printf ("SYS_JMP_BUF_SIZE=%zu\n", sizeof (sys_jmp_buf));
  printf ("HANDLER_VALUE_OFFSET=%zu\n", offsetof (struct handler, val));
  printf ("HANDLER_NEXT_OFFSET=%zu\n", offsetof (struct handler, next));
  printf ("HANDLER_JMP_OFFSET=%zu\n", offsetof (struct handler, jmp));
  printf ("HANDLER_SIZE=%zu\n", sizeof (struct handler));
  printf ("THREAD_HANDLERLIST_OFFSET=%zu\n",
          offsetof (struct thread_state, m_handlerlist));
  printf ("THREAD_STATE_SIZE=%zu\n", sizeof (struct thread_state));
  printf ("PATH_REL_LOADSEARCH=%s\n", PATH_REL_LOADSEARCH);
  printf ("PATH_DUMPLOADSEARCH=%s\n", PATH_DUMPLOADSEARCH);
  return ferror (stdout) ? 1 : 0;
}
