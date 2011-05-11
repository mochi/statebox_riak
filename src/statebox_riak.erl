%% @doc Convenience library that makes it easier to use statebox with riak,
%%      extracted from best practices in our code at Mochi.
-module(statebox_riak).
-export([new/1, get_pair/3, get_value/3,
         apply_bucket_ops/3, put_if_changed/4, put_value/3]).
-export([choose_first_metadata/2, identity/1, serialize/1, deserialize/1]).

-record(statebox_riak, {
          get=undefined :: undefined | fun(),
          put=undefined :: undefined | fun(),
          from_values=fun statebox_orddict:from_values/1 :: fun(),
          resolve_metadatas=fun ?MODULE:choose_first_metadata/2 :: fun(),
          truncate=fun ?MODULE:identity/1 :: fun(),
          expire=fun ?MODULE:identity/1 :: fun(),
          serialize=fun ?MODULE:serialize/1 :: fun(),
          deserialize=fun ?MODULE:deserialize/1 :: fun()}).

-type statebox() :: statebox:statebox().
-type riakc_obj() :: riakc_obj:riakc_obj().
-opaque statebox_riak() :: #statebox_riak{}.
-type option() :: {get, fun()} |
                  {put, fun()} |
                  {from_values, fun()} |
                  {resolve_metadatas, fun()} |
                  {max_queue, undefined | non_neg_integer()} |
                  {expire_ms, undefined | statebox:timedelta()} |
                  {serialize, fun()} |
                  {deserialize, fun()} |
                  {riakc_pb_socket, pid()}.
-type bucket() :: binary().
-type key() :: binary().

%% External API

-spec new([option()]) -> statebox_riak().
new(Opts) ->
    lists:foldl(fun parse_option/2, #statebox_riak{}, Opts).

-spec get_pair(bucket(), key(), statebox_riak()) -> {riakc_obj(), statebox()}.
get_pair(Bucket, Key, S=#statebox_riak{get=Get}) ->
    resolve_box(Bucket, Key, Get(Bucket, Key), S).

-spec get_value(bucket(), key(), statebox_riak()) -> term().
get_value(Bucket, Key, S) ->
    {_O, V} = get_pair(Bucket, Key, S),
    statebox:value(V).

-spec apply_bucket_ops(bucket(), [{[key()], statebox:op()}], statebox_riak()) -> ok.
apply_bucket_ops(_Bucket, [], _S) ->
    ok;
apply_bucket_ops(Bucket, [{Keys, Ops} | Rest], S=#statebox_riak{get=Get}) ->
    F = fun (Key) ->
                {Obj, Box} = Get(Bucket, Key),
                ok = put_if_changed(Box, statebox:modify(Ops, Box), Obj, S)
        end,
    lists:foreach(F, Keys),
    apply_bucket_ops(Bucket, Rest, S).

-spec put_value(statebox(), riakc_obj(), statebox_riak()) -> ok.
put_value(New, O, S=#statebox_riak{put=Put}) ->
    Put(update_value(O, serialize(New, S), S), O).

-spec put_if_changed(statebox(), statebox(), riakc_obj(), statebox_riak()) -> ok.
put_if_changed(Old, New, O, S) ->
    case statebox:value(Old) =:= statebox:value(New) of
        true ->
            ok;
        false ->
            put_value(New, O, S)
    end.

%% Exported private API

%% @private
-spec identity(statebox()) -> statebox().
identity(V) ->
    V.

%% @private
-spec serialize(statebox()) -> binary().
serialize(V) ->
    term_to_binary(V, [compressed, {minor_version, 1}]).

%% @private
-spec deserialize(binary()) -> statebox().
deserialize(Bin) ->
    binary_to_term(Bin).

%% @private
-spec choose_first_metadata(riakc_obj(), [term()]) -> riakc_obj().
choose_first_metadata(O, [First, _ | _]) ->
    %% We only need to arbitrarily choose metadata if there
    %% are siblings.
    riakc_obj:update_metadata(O, First);
choose_first_metadata(O, _Metadatas) ->
    O.

%% Internal API


parse_option({get, Get}, S) ->
    S#statebox_riak{get=Get};
parse_option({put, Put}, S) ->
    S#statebox_riak{put=Put};
parse_option({max_queue, undefined}, S) ->
    S#statebox_riak{truncate=fun identity/1};
parse_option({max_queue, MaxQueue}, S) ->
    S#statebox_riak{
      truncate=fun (Box) -> statebox:truncate(MaxQueue, Box) end};
parse_option({expire_ms, undefined}, S) ->
    S#statebox_riak{expire=fun identity/1};
parse_option({expire_ms, ExpireMS}, S) ->
    S#statebox_riak{
      expire=fun (Box) -> statebox:expire(ExpireMS, Box) end};
parse_option({serialize, Serialize}, S) ->
    S#statebox_riak{serialize=Serialize};
parse_option({deserialize, Deserialize}, S) ->
    S#statebox_riak{deserialize=Deserialize};
parse_option({resolve_metadatas, ResolveMetadatas}, S) ->
    S#statebox_riak{resolve_metadatas=ResolveMetadatas};
parse_option({from_values, FromValues}, S) ->
    S#statebox_riak{from_values=FromValues};
parse_option({riakc_pb_socket, Pid}, S) ->
    S#statebox_riak{
      get=fun (Bucket, Key) -> riakc_pb_socket:get(Pid, Bucket, Key) end,
      put=fun (Obj) -> ok = riakc_pb_socket:put(Pid, Obj) end}.

resolve_box(Bucket, _Key, {ok, O},
            #statebox_riak{deserialize=Deserialize, from_values=FromValues}) ->
    {O, FromValues(
          Bucket,
          lists:map(Deserialize, riakc_obj:get_values(O)))};
resolve_box(Bucket, Key, {error, notfound},
            #statebox_riak{from_values=FromValues}) ->
    {riakc_obj:new(Bucket, Key),
     FromValues(Bucket, [])}.

serialize(V, #statebox_riak{expire=Expire,
                            truncate=Truncate,
                            serialize=Serialize}) ->
    Serialize(Truncate(Expire(V))).

update_value(O, Value, #statebox_riak{resolve_metadatas=ResolveMetadatas}) ->
    ResolveMetadatas(
      riakc_obj:update_value(O, Value),
      riakc_obj:get_metadatas(O)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
new_test() ->
    ?assertEqual(
       #statebox_riak{},
       new([])),
    ok.

-endif.
