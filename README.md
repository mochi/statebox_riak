statebox_riak - statebox + riak-erlang-client convenience library
=================================================================

<bob@redivi.com>

Overview:
---------

Convenience library that makes it easier to use statebox with riak,
extracted from best practices in our production code at Mochi Media.

Status:
-------

Extracted from production code, but not yet used in production.

Usage:
------

    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    S = statebox_riak:new([{riakc_pb_socket, Pid}]),
    statebox_riak:apply_bucket_ops(
        <<"friends">>,
        [{[<<"bob">>], statebox_orddict:f_union(following, [<<"alice">>])},
         {[<<"alice">>], statebox_orddict:f_union(followers, [<<"bob">>])}],
        S),
    [<<"alice">>] = orddict:fetch(
        following, statebox_riak:get_value(<<"friends">>, <<"bob">>, S)),
    [<<"bob">>] = orddict:fetch(
        followers, statebox_riak:get_value(<<"friends">>, <<"alice">>, S)).
