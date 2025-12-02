// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include "packet.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	ring->data = malloc(cap);
	if (!ring->data)
		return -1;
	ring->cap = cap;
	ring->len = 0;
	ring->read_pos = 0;
	ring->write_pos = 0;
	pthread_mutex_init(&ring->mutex, NULL);
	sem_init(&ring->sem_empty, 0, cap / PKT_SZ);
	sem_init(&ring->sem_full, 0, 0);
	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	sem_wait(&ring->sem_empty);
	pthread_mutex_lock(&ring->mutex);

	if (ring->write_pos == ring->cap)
		ring->write_pos = 0;
	char *dest = (char *)(ring->data + ring->write_pos);

	for (size_t i = 0; i < size; i++)
		dest[i] = *((char *)data + i);

	ring->write_pos += size;
	ring->len += size;

	pthread_mutex_unlock(&ring->mutex);
	sem_post(&ring->sem_full);
	return -1;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	sem_wait(&ring->sem_full);
	pthread_mutex_lock(&ring->mutex);

	if (ring->len == 0) {
		pthread_mutex_unlock(&ring->mutex);
		// pentru urmatorul consumator
		// sem_post(&ring->sem_full);
		return 0;
	}

	if (ring->read_pos == ring->cap)
		ring->read_pos = 0;
	char *dest = (char *)data;

	for (size_t i = 0; i < size; i++)
		dest[i] = ring->data[ring->read_pos + i];

	for (size_t i = 0; i < size; i++)
		ring->data[ring->read_pos + i] = 0;

	ring->read_pos += size;
	ring->len -= size;

	pthread_mutex_unlock(&ring->mutex);
	sem_post(&ring->sem_empty);
	return -1;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	free(ring->data);
	pthread_mutex_destroy(&ring->mutex);
	sem_destroy(&ring->sem_empty);
	sem_destroy(&ring->sem_full);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	for (size_t i = 0; i < ring->cap / PKT_SZ; i++) {
		sem_post(&ring->sem_full);
	}
}
