/* static time config */
#define CONFIG_LR_PARALLEL 1
#define CONFIG_LR_SEQUENTIAL 1
#define CONFIG_LR_SUBLIST_COUNT 1 /* per thread sublist count */
#define CONFIG_LR_THREAD_COUNT 16 /* assume >= node_count */
#define CONFIG_LR_NODE_COUNT 1000000
#define CONFIG_LR_ITER_COUNT 10
#define CONFIG_LR_CONTIGUOUS_LIST 0 /* below ones mutually exclusive */
#define CONFIG_LR_REVERSE_LIST 1
#define CONFIG_LR_RANDOM_LIST 0


#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/time.h>


/* list node */

typedef int lr_index_t;

typedef struct lr_node
{
  lr_index_t next;
  lr_index_t rank;
} lr_node_t;


/* lr list */

typedef struct lr_list
{
  size_t size;
  lr_node_t* head;
  lr_node_t nodes[1] __attribute__((aligned(64)));
} lr_list_t;


/* lr list functions */

#if CONFIG_LR_CONTIGUOUS_LIST

static size_t lr_node_index;

static int lr_init_node_allocator(size_t count)
{
  /* unused */
  count = count;
  lr_node_index = 0;
  return 0;
}

static lr_index_t lr_allocate_node(void)
{
  return (lr_index_t)(lr_node_index++);
}

#endif /* CONFIG_LR_CONTIGUOUS_LIST */

#if CONFIG_LR_REVERSE_LIST

static size_t lr_node_count;

static int lr_init_node_allocator(size_t count)
{
  lr_node_count = count;
  return 0;
}

static lr_index_t lr_allocate_node(void)
{
  return (lr_index_t)--lr_node_count;
}

#endif /* CONFIG_LR_REVERSE_LIST */

#if CONFIG_LR_RANDOM_LIST

static int lr_init_node_allocator(size_t count)
{
  return -1;
}

static lr_index_t lr_allocate_node(void) 
{
  return 0;
}

#endif /* CONFIG_LR_RANDOM_LIST */

static int lr_list_create(lr_list_t** l, size_t count)
{
  const size_t total_size =
    offsetof(lr_list_t, nodes) + count * sizeof(lr_node_t);

  lr_node_t* prev;
  lr_index_t index;

  *l = malloc(total_size);
  if (*l == NULL)
    return -1;

  (*l)->size = count;

  /* init node allocator */
  lr_init_node_allocator(count);

  /* allocate one and dont care about this case */
  index = lr_allocate_node();
  prev = (*l)->nodes + (size_t)index;
  (*l)->head = prev;

  while (--count)
  {
    index = lr_allocate_node();
    prev->next = index;
    prev = (*l)->nodes + (size_t)index;
  }

  prev->next = (lr_index_t)-(*l)->size;

  return 0;
}


static void lr_list_destroy(lr_list_t* l)
{
  free(l);
}


static inline lr_node_t* lr_list_next(lr_list_t* l, lr_node_t* pos)
{
  return &l->nodes[pos->next];
}


static inline const lr_node_t* lr_list_next_const
(const lr_list_t* l, const lr_node_t* pos)
{
  return &l->nodes[pos->next];
}


static inline lr_node_t* lr_list_head(lr_list_t* l)
{
  return l->head;
}


static inline unsigned int lr_list_is_last_index
(const lr_list_t* l, lr_index_t i)
{
  return i == (lr_index_t)-l->size;
}


static inline unsigned int lr_list_is_last_node
(const lr_list_t* l, const lr_node_t* n)
{
  return lr_list_is_last_index(l, n->next);
}


static inline const lr_node_t* lr_list_head_const(const lr_list_t* l)
{
  return l->head;
}


static void lr_list_unrank(lr_list_t* l)
{
  /* todo: contiguous accesses */

  lr_node_t* pos = lr_list_head(l);
  for (; ; pos = lr_list_next(l, pos))
  {
    pos->rank = (lr_index_t)-1;

    if (lr_list_is_last_node(l, pos))
      break ;
  }
}


static void __attribute__((unused)) lr_print(const lr_list_t* l)
{
  const lr_node_t* pos = lr_list_head_const(l);
  for (; ; pos = lr_list_next_const(l, pos))
  {
    printf("%u\n", pos->rank);

    if (lr_list_is_last_node(l, pos))
      break ;
  }
}


#if CONFIG_LR_PARALLEL

#ifndef __USE_GNU
#define __USE_GNU
#endif
#include <sched.h>
#include <pthread.h>

typedef struct lr_sublist
{
  lr_index_t head;

  /* index in the list */
  lr_index_t next;
  lr_index_t saved_next;

  lr_index_t last_rank;
  lr_index_t prefix_rank;

} lr_sublist_t;

typedef struct lr_shared_data
{
  pthread_barrier_t barrier;
  lr_list_t* list;
  lr_sublist_t* volatile sublists;
  lr_sublist_t* volatile sublists_head;
  struct timeval* tm;
} lr_shared_data_t;

typedef struct lr_thread_data
{
  pthread_t thread;
  lr_shared_data_t* shared;
  unsigned int tid;
} lr_thread_data_t;


static inline lr_sublist_t* lr_sublist_alloc_array(size_t count)
{
  return malloc(count * sizeof(lr_sublist_t));
}


static inline void lr_sublist_free_array(lr_sublist_t* sl)
{
  free(sl);
}


static inline int lr_sublist_is_last_index(lr_index_t i)
{
  /* last node not included in sublist */
  return i < 0;
}


static inline int lr_sublist_is_last_node(const lr_node_t* n)
{
  /* last node not included in sublist */
  return lr_sublist_is_last_index(n->next);
}


static inline size_t lr_list_node_to_index
(const lr_list_t* l, const lr_node_t* n)
{
  return n - l->nodes;
}


static lr_sublist_t* lr_list_split
(lr_list_t* list, unsigned int tid, lr_sublist_t* sublists, size_t count)
{
  /* list the whole list to split
     ti the thread identifier
     sublists the whole sublist array
     count the per thread sublist count
  */

  /* updated to point the head of sublists list */
  lr_sublist_t* sublists_head = NULL;

  /* perthread node count */
  const size_t perthread_node_count = list->size / CONFIG_LR_THREAD_COUNT;

  /* perthread pos in sublists */
  size_t sublist_pos = (size_t)tid * count;

  /* list step size */
  const size_t list_step = perthread_node_count / CONFIG_LR_SUBLIST_COUNT;

  /* choose sublist_count heads in [list_lo, list_hi[ */
  size_t list_lo = (size_t)tid * perthread_node_count;
  const size_t list_hi = list_lo + perthread_node_count;

  const size_t head_index = lr_list_node_to_index(list, list->head);

  /* split into equally spaced list subranges */
  for (; count; --count, ++sublist_pos)
  {
    lr_sublist_t* const pos = &sublists[sublist_pos];
    lr_node_t* node;

    pos->last_rank = 0;
    pos->prefix_rank = 0;

    /* special case if this block contains the list head */
    if ((head_index >= list_lo) && (head_index < list_hi))
    {
      node = list->head;
      pos->saved_next = node->next;
      pos->head = head_index;
      sublists_head = pos;
    }
    else
    {
      node = &list->nodes[list_lo];
      pos->saved_next = node->next;
      /* +1 since may be 0. assume sublist_pos < (list->size-1) */
      node->next = (lr_index_t)-(sublist_pos + 1);
      pos->head = (lr_index_t)list_lo;
    }

    /* update sublists pos, count */
    list_lo += list_step;
  }

  return sublists_head;
}

static void* lr_thread_entry(void* p)
{
  lr_thread_data_t* const td = (lr_thread_data_t*)p;
  lr_shared_data_t* const sd = td->shared;
  lr_list_t* const list = sd->list;
  const unsigned int tid = td->tid;
  lr_sublist_t* sublists = NULL;
  size_t sublist_count;
  size_t sublist_head;
  struct timeval tms[2];

  if (tid == 0)
  {
    /* start time measures */
    gettimeofday(&tms[0], NULL);

    sd->sublists = lr_sublist_alloc_array
      (CONFIG_LR_SUBLIST_COUNT * CONFIG_LR_THREAD_COUNT);
    if (sd->sublists == NULL)
      goto on_error;
  }

  /* step1: split list into sublists */
  pthread_barrier_wait(&sd->barrier);
  {
    /* per thread */
    sublist_head = tid * CONFIG_LR_SUBLIST_COUNT;
    sublists = sd->sublists;
    sublist_count = CONFIG_LR_SUBLIST_COUNT;

    /* updating sd->sublists_head is safe since only 1 writer */
    lr_sublist_t* const sublists_head = lr_list_split
      (list, tid, sublists, sublist_count);
    if (sublists_head != NULL)
      sd->sublists_head = sublists_head;
  }

#if 0 /* debug */
  pthread_barrier_wait(&sd->barrier);
  if (tid == 0)
  {
    printf("-- step1\n");

    size_t i;
    for (i = 0; i < CONFIG_LR_THREAD_COUNT * sublist_count; ++i)
      printf("[%lu]: %d\n", i, sublists[i].head);
  }
#endif

  /* step2: compute sublist ranks */
  pthread_barrier_wait(&sd->barrier);
  {
    lr_sublist_t* sublist = &sublists[sublist_head];
    size_t j;

    for (j = 0; j < sublist_count; ++j, ++sublist)
    {
      lr_node_t* pos = &list->nodes[sublist->head];
      lr_index_t next_index = sublist->saved_next;
      lr_index_t rank = 0;

      while (!lr_sublist_is_last_index(next_index))
      {
	pos->rank = rank++;
	pos = &list->nodes[next_index];
	next_index = pos->next;
      }

      /* pos points to the last non included node */
      if (lr_list_is_last_index(list, next_index))
      {
	/* compute now for the last item */
	pos->rank = (lr_index_t)(list->size - 1);
	sublist->last_rank = rank;
	sublist->next = -1;
      }
      else
      {
	sublist->last_rank = rank;
	/* next_index encodes -(sublist_index + 1) */
	sublist->next = -(next_index + 1);
      }
    }
  }

#if 0 /* debug */
  pthread_barrier_wait(&sd->barrier);
  if (tid == 0)
  {
    printf("-- step2\n");

    lr_sublist_t* pos = sd->sublists_head;
    while (1)
    {
      printf(" %d", pos->head);
      if (pos->next == -1)
	break;
      pos = &sublists[pos->next];
    }
    printf("\n");
    getchar();
  }
#endif

  /* step3: prefix ranks */
  pthread_barrier_wait(&sd->barrier);
  if ((tid == 0) && (sd->sublists_head->next != -1))
  {
    lr_index_t prev_rank = sd->sublists_head->last_rank;
    lr_sublist_t* pos = &sd->sublists[sd->sublists_head->next];

    while (1)
    {
      /* pos->last_rank already adds 1 */
      pos->prefix_rank = prev_rank;

      /* last sublist */
      if (pos->next == -1)
	break ;

      prev_rank += pos->last_rank;
      pos = &sublists[pos->next];
    }
  }

#if 0 /* debug */
  pthread_barrier_wait(&sd->barrier);
  if (tid == 0)
  {
    printf("-- step3\n");
    const size_t total_count =
      CONFIG_LR_SUBLIST_COUNT * CONFIG_LR_THREAD_COUNT;
    size_t i;
    for (i = 0; i < total_count; ++i)
      printf("[%lu]: %d\n", i, sublists[i].prefix_rank);
    getchar();
  }
#endif

  /* step4: global update */
  pthread_barrier_wait(&sd->barrier);
  {
    lr_sublist_t* sublist = &sublists[sublist_head];
    size_t j;
    for (j = 0; j < sublist_count; ++j, ++sublist)
    {
      lr_node_t* pos = &list->nodes[sublist->head];
      lr_index_t next_index = sublist->saved_next;
      while (!lr_sublist_is_last_index(next_index))
      {
	pos->rank += sublist->prefix_rank;
	pos = &list->nodes[next_index];
	next_index = pos->next;
      }
    }
  }

  /* step5: restore pointers */
  pthread_barrier_wait(&sd->barrier);
  {
    lr_sublist_t* sublist = &sublists[sublist_head];
    size_t j;
    for (j = 0; j < sublist_count; ++j, ++sublist)
      list->nodes[sublist->head].next = sublist->saved_next;
  }

 on_error:
  pthread_barrier_wait(&sd->barrier);
  if (tid == 0)
  {
    if (sublists != NULL)
      lr_sublist_free_array(sublists);

    gettimeofday(&tms[1], NULL);
    timersub(&tms[1], &tms[0], sd->tm);
  }

  return NULL;
}

static void lr_list_rank_par(lr_list_t* list, struct timeval* tm)
{
  lr_thread_data_t threads[CONFIG_LR_THREAD_COUNT];
  lr_shared_data_t shared;
  unsigned int tid;

  /* init shared */
  pthread_barrier_init(&shared.barrier, NULL, CONFIG_LR_THREAD_COUNT);
  shared.tm = tm;
  shared.list = list;
  shared.sublists_head = NULL;

  /* init threads */
  for (tid = 0; tid < CONFIG_LR_THREAD_COUNT; ++tid)
  {
    lr_thread_data_t* const td = &threads[tid];
    td->shared = &shared;
    td->tid = tid;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(tid, &cpuset);

    if (tid == 0)
    {
      pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
      continue ;
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
    pthread_create(&td->thread, &attr, lr_thread_entry, (void*)td);
  }

  /* thread[0] entry */
  lr_thread_entry(&threads[0]);

  for (tid = 1; tid < CONFIG_LR_THREAD_COUNT; ++tid)
    pthread_join(threads[tid].thread, NULL);
}

#endif /* CONFIG_LR_PARALLEL */


#if CONFIG_LR_SEQUENTIAL

static void lr_list_rank_seq(lr_list_t* l, struct timeval* tm)
{
  lr_node_t* pos = lr_list_head(l);
  lr_index_t rank = 0;

  struct timeval tms[2];

  gettimeofday(&tms[0], NULL);

  for (; ; pos = lr_list_next(l, pos), ++rank)
  {
    pos->rank = rank;

    if (lr_list_is_last_node(l, pos))
      break ;
  }

  gettimeofday(&tms[1], NULL);
  timersub(&tms[1], &tms[0], tm);
}

#endif /* CONFIG_LR_SEQUENTIAL */


static int lr_list_check(const lr_list_t* l)
{
  const lr_node_t* pos = lr_list_head_const(l);
  lr_index_t rank = 0;

  for (; ; ++rank, pos = lr_list_next_const(l, pos))
  {
    if (pos->rank != rank)
    {
      printf("[!] lr_check @%lu, %d\n", (size_t)rank, pos->rank);
      return -1;
    }

    if (lr_list_is_last_node(l, pos))
      break ;
  }

  return 0;
}


/* main */

int main(int ac, char** av)
{
  lr_list_t* list;
  size_t iter;
  struct timeval tm;

  if (lr_list_create(&list, CONFIG_LR_NODE_COUNT) == -1)
    return -1;

  for (iter = 0; iter < CONFIG_LR_ITER_COUNT; ++iter)
  {
#if CONFIG_LR_SEQUENTIAL
    lr_list_unrank(list);
    lr_list_rank_seq(list, &tm);
    printf("seq_time: %lu\n", tm.tv_sec * 1000000 + tm.tv_usec);
    lr_list_check(list);
#endif

#if CONFIG_LR_PARALLEL
    lr_list_unrank(list);
    lr_list_rank_par(list, &tm);
    printf("par_time: %lu\n", tm.tv_sec * 1000000 + tm.tv_usec);
    lr_list_check(list);
#endif
  }

  lr_list_destroy(list);

  return 0;
}
