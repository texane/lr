#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/types.h>


#define CONFIG_LR_NODE_COUNT 100000
#define CONFIG_THREAD_COUNT 15


typedef unsigned long atomic_t;

static void inline atomic_write(atomic_t* dest, atomic_t value)
{
  *dest = value;
  __asm__ __volatile__ ("":::"memory");
}

typedef struct lr_node
{
  atomic_t lock __attribute__((aligned));

  struct
  {
    uint32_t rank;
    uint32_t next;
  } pair __attribute__((packed));

/*   double pad[(64 - 16) / sizeof(double)]; */

} lr_node_t;

static inline void lock_node(lr_node_t* node)
{
  while (!__sync_bool_compare_and_swap(&node->lock, 0, 1))
    ;
}

static inline void unlock_node(lr_node_t* node)
{
  atomic_write(&node->lock, 0);
}

#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>


#if 0
static void rank_gpu()
{
  while (node->pair.next != -1U)
  {
    __syncthread();
    
  }
}
#endif

static void rank_from(lr_node_t* nodes, size_t i)
{
  /* dont account for the last node but ranks
     have to be initialized with 1
   */

  lr_node_t* const node = &nodes[i];

  if (node->pair.next == -1U)
  {
    node->pair.rank = 0;
    return ;
  }

  while (node->pair.next != -1U)
  {
    lr_node_t* const next = nodes + node->pair.next;

    lock_node(node);
    lock_node(next);

    if (next->pair.next == -1U)
    {
      unlock_node(next);
      unlock_node(node);
      break ;
    }

    node->pair.rank += next->pair.rank;
    node->pair.next = next->pair.next;

    unlock_node(next);
    unlock_node(node);
  }
}

#ifndef __USE_GNU
#define __USE_GNU
#endif
#include <sched.h>
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
  struct thread_work works[CONFIG_THREAD_COUNT];

  const size_t stride = count / CONFIG_THREAD_COUNT;

  size_t k = 0;
  size_t i = 0;

  pthread_attr_t attr;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  for (; k < CONFIG_THREAD_COUNT; ++k, i += stride)
  {
    thread_work_t* const w = &works[k];

    w->nodes = nodes;
    w->i = i;
    w->j = i + stride;

    if (k == (CONFIG_THREAD_COUNT - 1))
      w->j = count;

    CPU_ZERO(&cpuset);
    CPU_SET((k + 1), &cpuset);
    pthread_attr_init(&attr);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
    pthread_create(&w->thread, &attr, thread_entry, (void*)w);
  }

  for (k = 0; k < CONFIG_THREAD_COUNT; ++k)
    pthread_join(works[k].thread, NULL);
}

static void rank_seq(lr_node_t* nodes, size_t count)
{
  lr_node_t* pos = nodes;
  uint32_t rank = 0;

  while (pos->pair.next != -1U)
  {
    pos->pair.rank += rank;
    pos = nodes + pos->pair.next;
    ++rank;
  }
}


/* main */

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
    nodes->lock = 0;
    nodes->pair.rank = 1;
    nodes->pair.next = i + 1;
  }

  nodes->lock = 0;
  nodes->pair.rank = 1;
  nodes->pair.next = -1U;
}

static int check_nodes(const lr_node_t* nodes, size_t count)
{
  const size_t saved_count = count;
  for (; count; --count, ++nodes)
    if (nodes->pair.rank != (count - 1))
    {
      printf("error: @%lu %u\n", saved_count - count, nodes->pair.rank);
      break ;
    }
  return count ? -1 : 0;
}


/* list allocator */

#include <unistd.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>

static unsigned char lr_node_bitmap[CONFIG_LR_NODE_COUNT];
static size_t lr_node_count;

static int lr_init_node_allocator(size_t count)
{
  srand(getpid() * time(NULL));
  lr_node_count = count;
  memset(lr_node_bitmap, 0, sizeof(lr_node_bitmap));
  return 0;
}

static size_t lr_allocate_node(void) 
{
  const size_t saved_pos = rand() % lr_node_count;
  size_t pos;

  for (pos = saved_pos; pos < lr_node_count; ++pos)
    if (lr_node_bitmap[pos] == 0)
    {
      lr_node_bitmap[pos] = 1;
      goto on_index_found;
    }

  for (pos = 0; pos < saved_pos; ++pos)
    if (lr_node_bitmap[pos] == 0)
    {
      lr_node_bitmap[pos] = 1;
      goto on_index_found;
    }

 on_index_found:
  return pos;
}

static void lr_list_init(lr_node_t* nodes, size_t count)
{
  lr_node_t* prev = &nodes[0];

  /* init node allocator */
  lr_init_node_allocator(count);

  /* allocate one and dont care about this case */
  lr_node_bitmap[0] = 1;
  nodes[0].pair.rank = 1U;

  while (--count)
  {
    const size_t index = lr_allocate_node();

    lr_node_t* const node = nodes + index;
    prev->pair.next = index;
    prev = nodes + index;

    node->pair.rank = 1U;
  }

  prev->pair.next = -1U;
}

int main(int ac, char** av)
{
  static lr_node_t nodes[CONFIG_LR_NODE_COUNT] __attribute__((aligned(64)));
  const size_t count = sizeof(nodes) / sizeof(*nodes);
  struct timeval tms[3];

  size_t i;
  for (i = 0; i < 10; ++i)
  {
    lr_list_init(nodes, count);
    gettimeofday(&tms[0], NULL);
    rank_seq(nodes, count);
    gettimeofday(&tms[1], NULL);
    timersub(&tms[1], &tms[0], &tms[2]);
    printf("seq: %lu\n", tms[2].tv_sec * 1000000 + tms[2].tv_usec);

    lr_list_init(nodes, count);
    gettimeofday(&tms[0], NULL);
    rank_par(nodes, count);
    gettimeofday(&tms[1], NULL);
    timersub(&tms[1], &tms[0], &tms[2]);
    printf("par: %lu\n", (tms[2].tv_sec * 1000000 + tms[2].tv_usec) - 600);

#if 0
    if (check_nodes(nodes, count))
      return -1;
#endif
  }

  return 0;
}
