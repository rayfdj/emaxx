#include <config.h>
#include "lisp.h"
#include "itree.h"
#undef NDEBUG
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef eassert
#define eassert(cond) assert(cond)
#include "itree.c"
void *xmalloc(size_t n) { void *p = malloc(n ? n : 1); assert(p); return p; }
void *xrealloc(void *p, size_t n) { p = realloc(p, n ? n : 1); assert(p); return p; }
void xfree(void *p) { free(p); }
AVOID emacs_abort(void) { abort(); }
int main(void) {
 struct itree_tree tree; itree_clear(&tree);
 struct itree_node nodes[256] = {0};
 bool live[256] = {0};
 char op;
 long id, a, b, c, d;
 size_t step=0;
 while (scanf(" %c", &op) == 1) {
  ++step;
  switch(op) {
   case 'I': assert(scanf("%ld %ld %ld %ld %ld", &id,&a,&b,&c,&d)==5); assert(!live[id]);
    itree_node_init(&nodes[id], c, d, make_fixnum(id)); itree_insert(&tree,&nodes[id],a,b); live[id]=true; break;
   case 'R': assert(scanf("%ld", &id)==1); assert(live[id]); itree_node_begin(&tree,&nodes[id]); itree_remove(&tree,&nodes[id]); live[id]=false; break;
   case 'M': assert(scanf("%ld %ld %ld", &id,&a,&b)==3); assert(live[id]); itree_node_set_region(&tree,&nodes[id],a,b); break;
   case '+': assert(scanf("%ld %ld %ld", &a,&b,&c)==3); itree_insert_gap(&tree,a,b,c); break;
   case '-': assert(scanf("%ld %ld", &a,&b)==2); itree_delete_gap(&tree,a,b); break;
   case 'Q': {
    assert(scanf("%ld %ld %ld %ld %ld", &a,&b,&c,&id,&d)==5);
    struct itree_iterator iter; itree_iterator_start(&iter,&tree,a,b,c);
    struct itree_node *node;
    printf("Q%zu",step);
    bool first=true;
    while((node=itree_iterator_next(&iter))) {
     printf(" %ld:%ld:%ld",(long)XFIXNUM(node->data),node->begin,node->end);
     if(first && id>=0) itree_iterator_narrow(&iter,id,d);
     first=false;
    }
    puts(""); break;
   }
   case 'S':
    printf("S%zu %ld",step,(long)tree.size);
    for(id=0;id<256;id++) if(live[id]) printf(" %ld:%ld:%ld",id,itree_node_begin(&tree,&nodes[id]),itree_node_end(&tree,&nodes[id]));
    puts(""); break;
   default: abort();
  }
  assert(check_tree(&tree,true));
 }
 return 0;
}
