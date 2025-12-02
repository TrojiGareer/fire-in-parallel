// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	char out_buf[256];
	while (1) {
		struct so_packet_t pkt;
		ssize_t ret = ring_buffer_dequeue(ctx->producer_rb, &pkt, PKT_SZ);
		if (ret == 0) {
			break;
		}

		int action = process_packet(&pkt);
		unsigned long hash = packet_hash(&pkt);
		unsigned long timestamp = pkt.hdr.timestamp;

		// scriem in fisier
		// pthread_mutex_lock(&ctx->shared_data->mutex);
		int len = snprintf(out_buf, 256, "%s %016lx %lu\n",
			RES_TO_STR(action), hash, timestamp);
		write(ctx->shared_data->out_fd, out_buf, len);
		// pthread_mutex_unlock(&ctx->shared_data->mutex);

	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{
	(void) tids;
	(void) num_consumers;
	(void) rb;
	(void) out_filename;

	shared_data_t *shared_data = malloc(sizeof(shared_data_t));
	shared_data->out_fd = open(out_filename, O_RDWR|O_CREAT|O_TRUNC, 0666);

	
	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));
		ctx->producer_rb = rb;
		ctx->shared_data = shared_data;
		// pthread_mutex_init(&shared_data->mutex, NULL);
		
		pthread_create(&tids[i], NULL, (void *)consumer_thread, ctx);
	}

	return num_consumers;
}
