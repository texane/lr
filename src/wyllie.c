#include <stdint.h>
#include <sys/types.h>

typedef unsigned long atomic_t;

static void inline atomic_write(atomic_t* dest, atomic_t value)
{
  *dest = value;
}

typedef union lr_node
{
  atomic_t atomic __attribute__((aligned));

  struct
  {
    uint16_t rank;
    uint16_t next;
  } pair __attribute__((packed));

} lr_node_t;

static inline lr_node_t* next_node(lr_node_t* nodes, size_t i)
{
  return &nodes[nodes[i].pair.next];
}

static void rank_from(lr_node_t* nodes, size_t i)
{
  if (nodes[i].pair.next == 0xffff)
  {
    nodes[i].pair.rank = 0;
    return ;
  }

  while ((nodes[i].pair.next != 0xffff) && (next_node(nodes, i)->pair.next != 0xffff))
  {
    lr_node_t* const next = next_node(nodes, i);
    const atomic_t tmp = ((atomic_t)(nodes[i].pair.rank + next->pair.rank)) | ((atomic_t)next->pair.next << 16);
    atomic_write(&nodes[i].atomic, tmp);
  }
}

#include <pthread.h>

typedef struct thread_work
{
  pthread_t thread;
  lr_node_t* nodes;
  size_t i;
  size_t j;
} thread_work_t;

static void rank_range(lr_node_t* nodes, size_t i, size_t j)
{
  for (; i < j; ++i)
    rank_from(nodes, i);
}

static void* thread_entry(void* p)
{
  thread_work_t* const w = (thread_work_t*)p;
  rank_range(w->nodes, w->i, w->j);
  return NULL;
}

static void rank_par(lr_node_t* nodes, size_t count)
{
#define CONFIG_THREAD_COUNT 2
  struct thread_work works[CONFIG_THREAD_COUNT];

  const size_t stride = count / CONFIG_THREAD_COUNT;

  size_t k = 0;
  size_t i = 0;

  for (; k < CONFIG_THREAD_COUNT; ++k, i += stride)
  {
    thread_work_t* const w = &works[k];

    w->nodes = nodes;
    w->i = i;
    w->j = (i + stride) <= count ? (i + stride) : i + count;

    pthread_create(&w->thread, NULL, thread_entry, (void*)w);
  }

  for (k = 0; k < CONFIG_THREAD_COUNT; ++k)
    pthread_join(works[k].thread, NULL);
}

static void rank_seq(lr_node_t* nodes, size_t count)
{
  size_t i;

  for (i = 0; i < count; ++i)
    rank_from(nodes, i);
}


/* main */

#include <stdio.h>

static void print_nodes(const lr_node_t* nodes, size_t count)
{
  for (; count; --count, ++nodes)
    printf("%u ", nodes->pair.rank);
  printf("\n");
}

static void init_nodes(lr_node_t* nodes, size_t count)
{
  size_t i;

  for (i = 0; i < (count - 1); ++i, ++nodes)
  {
    nodes->pair.rank = 1;
    nodes->pair.next = i + 1;
  }

  nodes->pair.rank = 1;
  nodes->pair.next = 0xffff;
}

#if 0 /* todo */
static int check_nodes(void)
{
  return -1;
}
#endif

int main(int ac, char** av)
{
  lr_node_t nodes[20];
  const size_t count = sizeof(nodes) / sizeof(*nodes);

  init_nodes(nodes, count);
  rank_seq(nodes, count);
  print_nodes(nodes, count);

  init_nodes(nodes, count);
  rank_par(nodes, count);
  print_nodes(nodes, count);

  return 0;
}
