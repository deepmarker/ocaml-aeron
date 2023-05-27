#include <string.h>
#include <caml/mlvalues.h>
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

#define Context_val(v) (*((aeron_context_t **) Data_custom_val(v)))
#define Client_val(v) (*((aeron_t **) Data_custom_val(v)))
#define Add_publication_val(v) (*((aeron_async_add_publication_t **) Data_custom_val(v)))
#define Publication_val(v) (*((aeron_publication_t **) Data_custom_val(v)))
#define Add_subscription_val(v) (*((aeron_async_add_subscription_t **) Data_custom_val(v)))
#define Subscription_val(v) (*((aeron_subscription_t **) Data_custom_val(v)))

CAMLprim value ml_aeron_context_init (value unit) {
    CAMLparam1(unit);
    CAMLlocal1(x);
    x = caml_alloc_custom(&context_ops,
                          sizeof (aeron_context_t **),
                          0, 1);
    int ret = aeron_context_init(&Context_val(x));
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_context_close (value ctx) {
    CAMLparam1(ctx);
    int ret = aeron_context_close(Context_val(ctx));
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_set_dir(value ctx, value dir) {
    CAMLparam2(ctx, dir);
    int ret = aeron_context_set_dir(Context_val(ctx), String_val(dir));
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_context_set_driver_timeout_ms(value ctx, value ms) {
    CAMLparam2(ctx, ms);
    int ret = aeron_context_set_driver_timeout_ms(Context_val(ctx), Long_val(ms));
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_init (value ctx) {
    CAMLparam1(ctx);
    CAMLlocal1(x);
    x = caml_alloc_custom(&client_ops,
                          sizeof (aeron_t **),
                          0, 1);
    int ret = aeron_init(&Client_val(x), Context_val(ctx));
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_start (value client) {
    CAMLparam1(client);
    int ret = aeron_start(Client_val(client));
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_close (value client) {
    CAMLparam1(client);
    int ret = aeron_close(Client_val(client));
    if (ret == -1) {
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
    if (ret == -1) {
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

CAMLprim value ml_aeron_publication_close(value pub) {
    CAMLparam1(pub);
    int ret = aeron_publication_close(Publication_val(pub), NULL, NULL);
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_publication_is_closed(value pub) {
    return(Val_bool(aeron_publication_is_closed(Publication_val(pub))));
}

CAMLprim value ml_aeron_publication_is_connected(value pub) {
    return(Val_bool(aeron_publication_is_connected(Publication_val(pub))));
}

CAMLprim value ml_aeron_publication_offer(value pub, value buf, value len) {
    int ret = aeron_publication_offer(Publication_val(pub),
                                      Caml_ba_data_val(buf),
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
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(x);
}

CAMLprim value ml_aeron_async_add_subscription_poll (value async) {
    CAMLparam1(async);
    CAMLlocal2(x, res);
    x = caml_alloc_custom(&subscription_ops,
                          sizeof (aeron_subscription_t **),
                          0, 1);
    int ret = aeron_async_add_subscription_poll(&Subscription_val(x),
                                                Add_subscription_val(async));
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

CAMLprim value ml_aeron_subscription_close(value sub) {
    CAMLparam1(sub);
    int ret = aeron_subscription_close(Subscription_val(sub), NULL, NULL);
    if (ret == -1) {
        caml_failwith(aeron_errmsg());
    }
    CAMLreturn(Val_unit);
}

CAMLprim value ml_aeron_subscription_is_closed(value pub) {
    return(Val_bool(aeron_subscription_is_closed(Subscription_val(pub))));
}

CAMLprim value ml_aeron_subscription_channel_status(value pub) {
    return(Val_long(aeron_subscription_channel_status(Subscription_val(pub))));
}
