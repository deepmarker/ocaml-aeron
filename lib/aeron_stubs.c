#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/bigarray.h>
#include <aeron/aeronc.h>

static struct custom_operations context_ops = {
  "aeron.context",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations client_ops = {
  "aeron.client",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations add_publication_ops = {
  "aeron.add_publication",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations publication_ops = {
  "aeron.publication",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations add_excl_publication_ops = {
  "aeron.add_exclusive_publication",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations excl_publication_ops = {
  "aeron.exclusive_publication",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations add_subscription_ops = {
  "aeron.add_subscription",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

static struct custom_operations subscription_ops = {
  "aeron.subscription",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default,
  custom_fixed_length_default
};

struct ml_aeron_sub {
    aeron_subscription_t* sub;
    int fd;
};

#define Context_val(v) (*(aeron_context_t **) Data_custom_val(v))
#define Client_val(v) (*(aeron_t **) Data_custom_val(v))
#define Add_publication_val(v) (*((aeron_async_add_publication_t **) Data_custom_val(v)))
#define Publication_val(v) (*((aeron_publication_t **) Data_custom_val(v)))
#define Add_excl_publication_val(v) (*((aeron_async_add_exclusive_publication_t **) Data_custom_val(v)))
#define Excl_publication_val(v) (*((aeron_exclusive_publication_t **) Data_custom_val(v)))
#define Add_subscription_val(v) (*((aeron_async_add_subscription_t **) Data_custom_val(v)))
#define Subscription_val(v) (((struct ml_aeron_sub *) Data_custom_val(v)))

CAMLprim value ml_aeron_context_init(value unit) {
    CAMLparam1(unit);
    CAMLlocal1(x);
    x = caml_alloc_custom(&context_ops, sizeof(aeron_context_t **), 0, 1);
    int ret = aeron_context_init(&Context_val(x));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_context_close (value ctx) {
    CAMLparam1(ctx);
    int ret = aeron_context_close(Context_val(ctx));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_set_dir(value ctx, value dir) {
    CAMLparam2(ctx, dir);
    int ret = aeron_context_set_dir(Context_val(ctx), String_val(dir));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_set_driver_timeout_ms(value ctx, value ms) {
    CAMLparam2(ctx, ms);
    int ret = aeron_context_set_driver_timeout_ms(Context_val(ctx), Long_val(ms));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_init (value ctx) {
    CAMLparam1(ctx);
    CAMLlocal1(x);
    x = caml_alloc_custom(&client_ops, sizeof(aeron_t **), 0, 1);
    int ret = aeron_init(&Client_val(x), Context_val(ctx));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_start (value client) {
    CAMLparam1(client);
    int ret = aeron_start(Client_val(client));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_close (value client) {
    CAMLparam1(client);
    int ret = aeron_close(Client_val(client));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_async_add_publication (value client, value uri, value stream_id) {
    CAMLparam3(client, uri, stream_id);
    CAMLlocal1(x);
    x = caml_alloc_custom(&add_publication_ops,
                          sizeof (aeron_async_add_publication_t **),
                          0, 1);
    int ret = aeron_async_add_publication(&Add_publication_val(x),
                                          Client_val(client),
                                          String_val(uri),
                                          Int32_val(stream_id));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_async_add_excl_publication (value client, value uri, value stream_id) {
    CAMLparam3(client, uri, stream_id);
    CAMLlocal1(x);
    x = caml_alloc_custom(&add_excl_publication_ops,
                          sizeof (aeron_async_add_exclusive_publication_t **),
                          0, 1);
    int ret = aeron_async_add_exclusive_publication(&Add_excl_publication_val(x),
                                                    Client_val(client),
                                                    String_val(uri),
                                                    Int32_val(stream_id));
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_async_add_publication_poll (value async) {
    CAMLparam1(async);
    CAMLlocal2(x, res);
    x = caml_alloc_custom(&publication_ops,
                          sizeof (aeron_publication_t **),
                          0, 1);
    int ret = aeron_async_add_publication_poll(&Publication_val(x),
                                               Add_publication_val(async));
    switch(ret) {
    case -1:
        caml_failwith(aeron_errmsg());
    case 0:
        CAMLreturn(Val_none);
    case 1:
        res = caml_alloc_some(x);
        CAMLreturn(res);
    }
}

CAMLprim value ml_aeron_async_add_excl_publication_poll (value async) {
    CAMLparam1(async);
    CAMLlocal2(x, res);
    x = caml_alloc_custom(&excl_publication_ops,
                          sizeof (aeron_exclusive_publication_t **),
                          0, 1);
    int ret = aeron_async_add_exclusive_publication_poll(&Excl_publication_val(x),
                                                         Add_excl_publication_val(async));
    switch (ret) {
    case -1:
      caml_failwith(aeron_errmsg());
    case 0:
      CAMLreturn(Val_none);
    case 1:
      res = caml_alloc_some(x);
      CAMLreturn(res);
    }
}

CAMLprim value ml_aeron_publication_close(value pub, value a) {
    CAMLparam1(pub);
    int ret = aeron_publication_close(Publication_val(pub), NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_excl_publication_close(value pub, value a) {
    CAMLparam1(pub);
    int ret = aeron_exclusive_publication_close(Excl_publication_val(pub), NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_publication_is_closed(value pub) {
    return(Val_bool(aeron_publication_is_closed(Publication_val(pub))));
}

CAMLprim value ml_aeron_excl_publication_is_closed(value pub) {
    return(Val_bool(aeron_exclusive_publication_is_closed(Excl_publication_val(pub))));
}

CAMLprim value ml_aeron_publication_is_connected(value pub) {
    return(Val_bool(aeron_publication_is_connected(Publication_val(pub))));
}

CAMLprim value ml_aeron_excl_publication_is_connected(value pub) {
    return(Val_bool(aeron_exclusive_publication_is_connected(Excl_publication_val(pub))));
}

CAMLprim value ml_aeron_publication_offer(value pub, value buf, value pos, value len) {
    int ret = aeron_publication_offer(Publication_val(pub),
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
    int ret = aeron_exclusive_publication_offer(Excl_publication_val(pub),
                                                Caml_ba_data_val(buf)+Long_val(pos),
                                                Long_val(len), NULL, NULL);
    return(Val_long(ret));
}

CAMLprim value ml_aeron_async_add_subscription (value client, value uri, value stream_id) {
    CAMLparam3(client, uri, stream_id);
    CAMLlocal1(x);
    x = caml_alloc_custom(&add_subscription_ops,
                          sizeof (aeron_async_add_subscription_t **),
                          0, 1);
    int ret = aeron_async_add_subscription(&Add_subscription_val(x),
                                           Client_val(client),
                                           String_val(uri),
                                           Int32_val(stream_id),
                                           NULL, NULL, NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_async_add_subscription_poll (value async, value fd) {
    CAMLparam2(async, fd);
    CAMLlocal2(x, res);
    x = caml_alloc_custom(&subscription_ops, sizeof(struct ml_aeron_sub), 0, 1);
    Subscription_val(x)->fd = Int_val(fd);
    int ret = aeron_async_add_subscription_poll(&Subscription_val(x)->sub,
                                                Add_subscription_val(async));
    switch(ret) {
    case -1:
        caml_failwith(aeron_errmsg());
    case 0:
        CAMLreturn(Val_none);
    default:
        res = caml_alloc_some(x);
        CAMLreturn(res);
    }
}

CAMLprim value ml_aeron_subscription_close(value sub) {
    CAMLparam1(sub);
    int ret = aeron_subscription_close(Subscription_val(sub)->sub, NULL, NULL);
    if (ret < 0) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_subscription_is_closed(value pub) {
    return(Val_bool(aeron_subscription_is_closed(Subscription_val(pub)->sub)));
}

CAMLprim value ml_aeron_subscription_channel_status(value pub) {
    return(Val_long(aeron_subscription_channel_status(Subscription_val(pub)->sub)));
}

CAMLprim value ml_aeron_subscription_constants(value sub) {
    CAMLparam1(sub);
    CAMLlocal1(x);
    aeron_subscription_constants_t consts;
    int ret = aeron_subscription_constants(Subscription_val(sub)->sub, &consts);
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
    int ret = aeron_publication_constants(Publication_val(pub), &consts);
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
    int ret = aeron_exclusive_publication_constants(Excl_publication_val(pub), &consts);
    if (ret < 0)
      caml_failwith(aeron_errmsg());

    x = caml_alloc_tuple(12);
    ml_alloc_publication_consts(x, &consts);
    CAMLreturn(x);
}

void poll_handler(void *clientd, const uint8_t *buffer, size_t length,
                  aeron_header_t *header) {

    aeron_header_values_t values;
    int ret = aeron_header_values(header, &values);
    // TODO: handle error

    struct ml_aeron_sub *sub = clientd;
    ssize_t bytes_written = 0;
    size_t total_written = 0;
    size_t remaining = sizeof(aeron_header_values_t);

    while (total_written < sizeof(aeron_header_values_t)) {
        bytes_written = write(sub->fd,
                              (char *)&values + total_written,
                              remaining);

        if (bytes_written < 0) {
            if (errno == EINTR)
                continue;  // Interrupted by signal, try again
            // Handle error - could return error code if needed
            break;
        }

        total_written += bytes_written;
        remaining -= bytes_written;
    }
    total_written = 0;
    bytes_written = 0;
    remaining = length;
    while (total_written < length) {
        bytes_written = write(sub->fd, buffer, remaining);
        if (bytes_written < 0) {
            if (errno == EINTR)
                continue;  // Interrupted by signal, try again
            // Handle error - could return error code if needed
            break;
        }

        total_written += bytes_written;
        remaining -= bytes_written;
    }
}

CAMLprim value ml_aeron_subscription_poll(value sub, value limit) {
    CAMLparam2(sub, limit);
    int nb_read = aeron_subscription_poll(Subscription_val(sub)->sub,
                                          poll_handler,
                                          Subscription_val(sub),
                                          Long_val(limit));
    if (nb_read < 0)
        caml_failwith(aeron_errmsg());
    CAMLreturn(Val_int(nb_read));
}
