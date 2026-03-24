#include "caml/mlvalues.h"
#include <endian.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/bigarray.h>
#include <aeron/aeronc.h>

// Helper function to ensure all bytes are written
static ssize_t write_all(int fd, const void *buffer, size_t length) {
    const char *ptr = (const char *)buffer;
    size_t remaining = length;
    ssize_t written;

    while (remaining > 0) {
        written = write(fd, ptr, remaining);
        if (written <= 0) {
            if (errno == EINTR)
                continue;  // Interrupted by signal, try again
            return -1;     // Real error
        }

        ptr += written;
        remaining -= written;
    }

    return length;
}

static void forward_errors(void *clientd, int errcode, const char *message) {
    // How do I read a be 16 bits integer from clientd?
    int fd = be16toh(*(uint16_t *)clientd);
    size_t message_len = strlen(message);
    if (message_len > UINT16_MAX) {
        message_len = UINT16_MAX;  // Limit message length
    }

    uint32_t be_errcode = htobe32(errcode);
    uint32_t be_message_len = htobe32(message_len);

    // Write all parts with error checking
    write_all(fd, &be_errcode, sizeof(be_errcode));
    write_all(fd, &be_message_len, sizeof(be_message_len));
    write_all(fd, message, message_len);
}

CAMLprim value ml_aeron_context_init(value ba) {
    CAMLparam1(ba);
    aeron_context_t *ctx;
    int ret = aeron_context_init(&ctx);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    ret = aeron_context_set_error_handler(ctx, forward_errors, Caml_ba_data_val(ba));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_ptr(ctx));
}

CAMLprim value ml_aeron_context_close (value ctx) {
    CAMLparam1(ctx);
    int ret = aeron_context_close(Ptr_val(ctx));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_set_dir(value ctx, value dir) {
    CAMLparam2(ctx, dir);
    int ret = aeron_context_set_dir(Ptr_val(ctx), String_val(dir));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_set_driver_timeout_ms(value ctx, value ms) {
    CAMLparam2(ctx, ms);
    int ret = aeron_context_set_driver_timeout_ms(Ptr_val(ctx), Long_val(ms));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_get_driver_timeout_ms(value ctx) {
    uint64_t ret = aeron_context_get_driver_timeout_ms(Ptr_val(ctx));
    return Val_long(ret);
}

CAMLprim value ml_aeron_context_set_use_conductor_agent_invoker(value ctx, value x) {
    CAMLparam2(ctx, x);
    int ret = aeron_context_set_use_conductor_agent_invoker(Ptr_val(ctx), Bool_val(x));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_get_use_conductor_agent_invoker(value ctx) {
    bool ret = aeron_context_get_use_conductor_agent_invoker(Ptr_val(ctx));
    return Val_bool(ret);
}

CAMLprim value ml_aeron_init (value ctx) {
    CAMLparam1(ctx);
    aeron_t *cli;
    int ret = aeron_init(&cli, Ptr_val(ctx));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_ptr(cli));
}

CAMLprim value ml_aeron_start (value client) {
    CAMLparam1(client);
    int ret = aeron_start(Ptr_val(client));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_main_do_work(value client) {
    return Val_int(aeron_main_do_work(Ptr_val(client)));
}

CAMLprim value ml_aeron_errmsg(value unit) {
  CAMLparam1(unit);
  CAMLlocal1(msg);
  msg = caml_copy_string(aeron_errmsg());
  CAMLreturn(msg);
}

CAMLprim value ml_aeron_errcode(value unit) {
    return Val_long(aeron_errcode());
}

CAMLprim value ml_aeron_close (value client) {
    CAMLparam1(client);
    int ret = aeron_close(Ptr_val(client));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_async_add_publication (value client, value uri, value stream_id) {
  CAMLparam3(client, uri, stream_id);
  aeron_async_add_publication_t *pub;
  int ret = aeron_async_add_publication(&pub,
                                        Ptr_val(client),
                                        String_val(uri),
                                        Int32_val(stream_id));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_ptr(pub));
}

CAMLprim value ml_aeron_async_add_excl_publication (value client, value uri, value stream_id) {
  CAMLparam3(client, uri, stream_id);
  aeron_async_add_exclusive_publication_t *pub;
  int ret = aeron_async_add_exclusive_publication(&pub,
                                                  Ptr_val(client),
                                                  String_val(uri),
                                                  Int32_val(stream_id));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_ptr(pub));
}

CAMLprim value ml_aeron_async_add_publication_poll (value async) {
    CAMLparam1(async);
    CAMLlocal1(res);
    aeron_publication_t *pub;
    int ret = aeron_async_add_publication_poll(&pub, Ptr_val(async));
    switch(ret) {
    case 0:
        CAMLreturn(Val_none);
    case 1:
        res = caml_alloc_some(Val_ptr(pub));
        CAMLreturn(res);
    default:
        caml_failwith(aeron_errmsg());
    }
}

CAMLprim value ml_aeron_async_add_excl_publication_poll (value async) {
    CAMLparam1(async);
    CAMLlocal1(res);
    aeron_exclusive_publication_t *pub;
    int ret = aeron_async_add_exclusive_publication_poll(&pub, Ptr_val(async));
    switch (ret) {
    case 0:
        CAMLreturn(Val_none);
    case 1:
        res = caml_alloc_some(Val_ptr(pub));
        CAMLreturn(res);
    default:
      caml_failwith(aeron_errmsg());
    }
}

CAMLprim value ml_aeron_publication_close(value pub, value a) {
    CAMLparam1(pub);
    int ret = aeron_publication_close(Ptr_val(pub), NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_excl_publication_close(value pub, value a) {
    CAMLparam1(pub);
    int ret = aeron_exclusive_publication_close(Ptr_val(pub), NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_publication_is_closed(value pub) {
    return(Val_bool(aeron_publication_is_closed(Ptr_val(pub))));
}

CAMLprim value ml_aeron_excl_publication_is_closed(value pub) {
    return(Val_bool(aeron_exclusive_publication_is_closed(Ptr_val(pub))));
}

CAMLprim value ml_aeron_publication_is_connected(value pub) {
    return(Val_bool(aeron_publication_is_connected(Ptr_val(pub))));
}

CAMLprim value ml_aeron_excl_publication_is_connected(value pub) {
    return(Val_bool(aeron_exclusive_publication_is_connected(Ptr_val(pub))));
}

CAMLprim value ml_aeron_publication_offer(value pub, value buf, value pos, value len) {
    int ret = aeron_publication_offer(Ptr_val(pub),
                                      Caml_ba_data_val(buf)+Long_val(pos),
                                      Long_val(len), NULL, NULL);
    return(Val_long(ret));
}

// Is buf still needed after publication_offer completes or not?


/* Based on the code you've shown, let me clarify about memory
   management in the `ml_aeron_publication_offer` function: */

/* When `aeron_publication_offer` is called, it takes the buffer data,
   position, and length to publish a message. The function is just
   passing a pointer to the buffer data (offset by the position) to
   the C function, which will copy the data to send it over the
   network. */

/* After `aeron_publication_offer` completes, the original buffer is
   no longer needed by Aeron itself - the data would have been either
   copied to internal buffers or transmitted directly. The C function
   doesn't keep a reference to the original buffer. */

/* So, once the function returns, OCaml's garbage collector can safely
   collect the buffer if there are no other references to it in your
   OCaml code. */

CAMLprim value ml_aeron_excl_publication_offer(value pub, value buf, value pos, value len) {
    int ret = aeron_exclusive_publication_offer(Ptr_val(pub),
                                                Caml_ba_data_val(buf)+Long_val(pos),
                                                Long_val(len), NULL, NULL);
    return(Val_long(ret));
}

CAMLprim value ml_aeron_async_add_subscription (value client, value uri, value stream_id) {
    CAMLparam3(client, uri, stream_id);
    aeron_async_add_subscription_t *sub;
    int ret = aeron_async_add_subscription(&sub,
                                           Ptr_val(client),
                                           String_val(uri),
                                           Int32_val(stream_id),
                                           NULL, NULL, NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_ptr(sub));
}

struct ml_aeron_sub {
    int fd;
    aeron_subscription_t *sub;
};

CAMLprim value ml_aeron_async_add_subscription_poll(value async, value ba) {
    struct ml_aeron_sub *sub = Caml_ba_data_val(ba);
    int ret = aeron_async_add_subscription_poll(&sub->sub, Ptr_val(async));
    return Val_int(ret);
}

CAMLprim value ml_aeron_subscription_close(value ba) {
    CAMLparam1(ba);
    struct ml_aeron_sub *sub = Caml_ba_data_val(ba);
    int ret = aeron_subscription_close(sub->sub, NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_subscription_is_closed(value ba) {
    struct ml_aeron_sub *sub = Caml_ba_data_val(ba);
    return(Val_bool(aeron_subscription_is_closed(sub->sub)));
}

CAMLprim value ml_aeron_subscription_channel_status(value ba) {
    struct ml_aeron_sub *sub = Caml_ba_data_val(ba);
    return(Val_long(aeron_subscription_channel_status(sub->sub)));
}

CAMLprim value ml_aeron_subscription_constants(value ba) {
    CAMLparam1(ba);
    CAMLlocal1(x);
    struct ml_aeron_sub *sub = Caml_ba_data_val(ba);
    aeron_subscription_constants_t consts;
    int ret = aeron_subscription_constants(sub->sub, &consts);
    if (ret < 0)
        caml_failwith(aeron_errmsg());

    x = caml_alloc_tuple(3);
    Store_field(x, 0, caml_copy_int64(consts.registration_id));
    Store_field(x, 1, caml_copy_int32(consts.stream_id));
    Store_field(x, 2, caml_copy_int32(consts.channel_status_indicator_id));
    CAMLreturn(x);
}

void ml_alloc_publication_consts(value x, aeron_publication_constants_t *consts) {
    Store_field(x, 0, caml_copy_int64(consts->original_registration_id));
    Store_field(x, 1, caml_copy_int64(consts->registration_id));
    Store_field(x, 2, caml_copy_int64(consts->max_possible_position));
    Store_field(x, 3, caml_copy_int64(consts->position_bits_to_shift));
    Store_field(x, 4, caml_copy_int64(consts->term_buffer_length));
    Store_field(x, 5, caml_copy_int64(consts->max_message_length));
    Store_field(x, 6, caml_copy_int64(consts->max_payload_length));
    Store_field(x, 7, caml_copy_int32(consts->stream_id));
    Store_field(x, 8, caml_copy_int32(consts->session_id));
    Store_field(x, 9, caml_copy_int32(consts->initial_term_id));
    Store_field(x, 10, caml_copy_int32(consts->publication_limit_counter_id));
    Store_field(x, 11, caml_copy_int32(consts->channel_status_indicator_id));
}

CAMLprim value ml_aeron_publication_constants(value pub) {
    CAMLparam1(pub);
    CAMLlocal1(x);
    aeron_publication_constants_t consts;
    int ret = aeron_publication_constants(Ptr_val(pub), &consts);
    if (ret < 0)
        caml_failwith(aeron_errmsg());

    x = caml_alloc_tuple(12);
    ml_alloc_publication_consts(x, &consts);
    CAMLreturn(x);
}

CAMLprim value ml_aeron_excl_publication_constants(value pub) {
    CAMLparam1(pub);
    CAMLlocal1(x);
    aeron_publication_constants_t consts;
    int ret = aeron_exclusive_publication_constants(Ptr_val(pub), &consts);
    if (ret < 0)
      caml_failwith(aeron_errmsg());

    x = caml_alloc_tuple(12);
    ml_alloc_publication_consts(x, &consts);
    CAMLreturn(x);
}

void poll_handler(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header) {
    if (clientd == NULL) {
        return; // No client data, cannot continue
    }
    struct ml_aeron_sub *sub = clientd;

    aeron_header_values_t values;
    int ret = aeron_header_values(header, &values);
    if (ret < 0) {
        // Handle error - log or report it
        return;
    }

    // Write header values
    if (write_all(sub->fd, &values, sizeof(aeron_header_values_t)) != sizeof(aeron_header_values_t)) {
        // Handle error
        return;
    }

    // Write buffer content
    if (write_all(sub->fd, buffer, length) != length) {
        // Handle error
        return;
    }
}

CAMLprim value ml_aeron_subscription_poll(value ba, value limit) {
    CAMLparam2(ba, limit);
    struct ml_aeron_sub *sub = Caml_ba_data_val(ba);
    int nb_read = aeron_subscription_poll(sub->sub,
                                          poll_handler,
                                          sub,
                                          Long_val(limit));
    if (nb_read < 0)
        caml_failwith(aeron_errmsg());
    CAMLreturn(Val_int(nb_read));
}

CAMLprim value alloc_claim(value unit) {
  CAMLparam1(unit);
  CAMLlocal1(claim);
  claim = caml_alloc(sizeof(aeron_buffer_claim_t), Abstract_tag);
  CAMLreturn(claim);
}

CAMLprim value bigstring_of_claim(value claim) {
  CAMLparam1(claim);
  CAMLlocal1(ba);
  aeron_buffer_claim_t *c = Data_abstract_val(claim);
  ba = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT, 1, c->data, c->length);
  CAMLreturn(ba);
}

CAMLprim value ml_aeron_publication_try_claim(value pub, value len, value claim) {
  int64_t ret = aeron_publication_try_claim(Ptr_val(pub), Long_val(len), Data_abstract_val(claim));
  return Long_val(ret);
}

CAMLprim value ml_aeron_excl_publication_try_claim(value pub, value len, value claim) {
  int64_t ret = aeron_exclusive_publication_try_claim(Ptr_val(pub), Long_val(len), Data_abstract_val(claim));
  return Long_val(ret);
}

CAMLprim value ml_aeron_buffer_claim_commit(value claim) {
    return Val_int(aeron_buffer_claim_commit(Data_abstract_val(claim)));
}

