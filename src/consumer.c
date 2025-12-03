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
	struct so_packet_t pkt;
	int id;
	while (ring_buffer_dequeue(ctx->producer_rb, &pkt, PKT_SZ, &id) != 0) {

		int action = process_packet(&pkt);
		unsigned long hash = packet_hash(&pkt);
		unsigned long timestamp = pkt.hdr.timestamp;

		pthread_mutex_lock(&ctx->shared_data->mutex);

        while (id != ctx->shared_data->timestamp) {
            pthread_mutex_unlock(&ctx->shared_data->mutex);
            sem_wait(&ctx->shared_data->sem);
            pthread_mutex_lock(&ctx->shared_data->mutex);

            if (id != ctx->shared_data->timestamp) {
                sem_post(&ctx->shared_data->sem);
            }
        }

        int len = snprintf(out_buf, 256, "%s %016lx %lu\n",
            RES_TO_STR(action), hash, timestamp);

        write(ctx->shared_data->out_fd, out_buf, len);

        ctx->shared_data->timestamp++;

        pthread_mutex_unlock(&ctx->shared_data->mutex);
        sem_post(&ctx->shared_data->sem);
	}
	free(ctx);
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
	shared_data->timestamp = 0;
	
	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));
		ctx->producer_rb = rb;
		ctx->shared_data = shared_data;

		pthread_create(&tids[i], NULL, (void *)consumer_thread, ctx);
	}

	return num_consumers;
}
