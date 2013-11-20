%% @doc Convenience library that makes it easier to use statebox with riak,
%%      extracted from best practices in our code at Mochi.
-module(statebox_riak).
-export([new/1, get_pair/3, get_value/3,
         apply_bucket_ops/3, apply_and_return/4,
         put_if_changed/4, put_value/3]).
-export([choose_first_metadata/2, identity/1, serialize/1, deserialize/1]).
-export([bad_get_fun/2, bad_put_fun/1]).

-type statebox() :: statebox:statebox().
-type op() :: statebox:op().
-type riakc_obj() :: riakc_obj:riakc_obj().
-type bucket() :: binary().
-type key() :: binary().
-type get_fun() :: fun ((bucket(), key()) -> {ok, riakc_obj()} | {error, notfound | term()}).
-type put_fun() :: fun ((riakc_obj()) -> ok).
-type from_values_fun() :: fun (([statebox()]) -> statebox()).
-type resolve_metadatas_fun() :: fun ((riakc_obj(), [term()]) -> riakc_obj()).
-type statebox_transform_fun() :: fun ((statebox()) -> statebox()).
-type serialize_fun() :: fun ((statebox()) -> binary()).
-type deserialize_fun() :: fun ((binary()) -> statebox()).

-record(statebox_riak, {
          get=fun ?MODULE:bad_get_fun/2 :: get_fun(),
          put=fun ?MODULE:bad_put_fun/1 :: put_fun(),
          from_values=fun statebox_orddict:from_values/1 :: from_values_fun(),
          resolve_metadatas=
              fun ?MODULE:choose_first_metadata/2 :: resolve_metadatas_fun(),
          truncate=fun ?MODULE:identity/1 :: statebox_transform_fun(),
          expire=fun ?MODULE:identity/1 :: statebox_transform_fun(),
          serialize=fun ?MODULE:serialize/1 :: serialize_fun(),
          deserialize=fun ?MODULE:deserialize/1 :: deserialize_fun()}).

-opaque statebox_riak() :: #statebox_riak{}.
-type option() :: {get, get_fun()} |
                  {put, put_fun()} |
                  {from_values, from_values_fun()} |
                  {resolve_metadatas, resolve_metadatas_fun()} |
                  {max_queue, undefined | non_neg_integer()} |
                  {expire_ms, undefined | statebox:timedelta()} |
                  {serialize, serialize_fun()} |
                  {deserialize, deserialize_fun()} |
                  {riakc_pb_socket, pid()}.

-export_type([statebox_riak/0]).

%% External API

%% @doc Create a new <code>statebox_riak()</code> wrapper, has a number of
%%      options for customizing the behavior. It is required to specify
%%      get and put, which can be done directly or set with
%%      riakc_pb_socket. Available options:
%%      <dl>
%%          <dt><code>{riakc_pb_socket, pid()}</code></dt>
%%          <dd>Use the given riakc_pb_socket with default settings for
%%              get and put.</dd>
%%          <dt><code>{get, get_fun()}</code></dt>
%%          <dd>Specify a custom <code>get(bucket(), key())</code> fun</dd>
%%          <dt><code>{put, put_fun()}</code></dt>
%%          <dd>Specify a custom <code>put(riakc_obj())</code> fun</dd>
%%          <dt><code>{max_queue, undefined | non_neg_integer()}</code></dt>
%%          <dd>Use <code>statebox:truncate/2</code> to ensure that at most
%%              this many events are peristed</dd>
%%          <dt><code>{expire_ms, undefined | non_neg_integer()}</code></dt>
%%          <dd>Use <code>statebox:expire/2</code> to ensure that events older
%%              than this many msec are not persisted</dd>
%%          <dt><code>{from_values, from_values_fun()}</code></dt>
%%          <dd>Override the default
%%              <code>fun statebox_orddict:from_values/1</code></dd>
%%          <dt><code>{resolve_metadatas, resolve_metadatas_fun()}</code></dt>
%%          <dd>Override the default
%%              <code>fun statebox_riak:choose_first_metadata/2</code></dd>
%%          <dt><code>{serialize, serialize_fun()}</code></dt>
%%          <dd>Override the default <code>term_to_binary/2</code>
%%              based serialization</dd>
%%          <dt><code>{deserialize, deserialize_fun()}</code></dt>
%%          <dd>Override the default <code>fun binary_to_term/1</code>
%%              deserialization</dd>
%%      </dl>
-spec new([option()]) -> statebox_riak().
new(Opts) ->
    lists:foldl(fun parse_option/2, #statebox_riak{}, Opts).

%% @doc Get a <code>{riakc_obj(), statebox()}</code> pair for the given
%%      <code>bucket()</code> and <code>key()</code>. If not found, an
%%      empty <code>riakc_obj()</code> (containing only the
%%      <code>bucket()</code> and <code>key()</code>) and
%%      new <code>statebox()</code> (using <code>from_values([])</code>)
%%      will be returned.
-spec get_pair(bucket(), key(), statebox_riak()) -> {riakc_obj(), statebox()}.
get_pair(Bucket, Key, S=#statebox_riak{get=Get}) ->
    resolve_box(Bucket, Key, Get(Bucket, Key), S).

%% @doc Get the value inside the <code>statebox()</code> at
%%      <code>bucket()</code> and <code>key()</code>. Convenient for
%%      read-only operations.
%% @equiv pair_value(get_pair(Bucket, Key, S))
-spec get_value(bucket(), key(), statebox_riak()) -> term().
get_value(Bucket, Key, S) ->
    pair_value(get_pair(Bucket, Key, S)).

%% @doc Get the value inside the <code>statebox()</code> in the
%%      <code>{riakc_obj(), statebox()}</code> pair.
-spec pair_value({riakc_obj(), statebox()}) -> term().
pair_value({_Obj, Box}) ->
    statebox:value(Box).

%% @doc For the given <code>[{[key()], op()}]</code> get each key,
%%      apply the op() to the statebox at that key, and put it back
%%      if the value changes.
-spec apply_bucket_ops(bucket(), [{[key()], op()}], statebox_riak()) -> ok.
apply_bucket_ops(_Bucket, [], _S) ->
    ok;
apply_bucket_ops(Bucket, [{Keys, Ops} | Rest], S) ->
    F = fun (Key) ->
                {Obj, Box} = get_pair(Bucket, Key, S),
                ok = put_if_changed(Box, statebox:modify(Ops, Box), Obj, S)
        end,
    lists:foreach(F, Keys),
    apply_bucket_ops(Bucket, Rest, S).

-spec apply_and_return(bucket(), [key()], op(), statebox_riak()) -> [statebox()].
apply_and_return(Bucket, Keys, Ops, S) ->
    [begin
        {Obj, Box} = get_pair(Bucket, Key, S),
        NewBox = statebox:modify(Ops, Box),
        ok = put_if_changed(Box, NewBox, Obj, S),
        NewBox
     end || Key <- Keys].

%% @doc Update the value in Obj with NewValue and put it in riak.
-spec put_value(statebox(), riakc_obj(), statebox_riak()) -> ok.
put_value(NewValue, Obj, S=#statebox_riak{put=Put}) ->
    Put(update_value(Obj, serialize(NewValue, S), S)).

%% @doc <code>put_value(New, Obj, S)</code> if the value inside the statebox
%%      <code>New</code> differs from the value inside the statebox
%%      <code>Old</code>.
-spec put_if_changed(statebox(), statebox(), riakc_obj(), statebox_riak()) -> ok.
put_if_changed(Old, New, Obj, S) ->
    case statebox:value(Old) =:= statebox:value(New) of
        true ->
            ok;
        false ->
            put_value(New, Obj, S)
    end.

%% @doc If there are more than one given metadatas, update Obj with the
%%      first. Otherwise, return Obj as-is.
-spec choose_first_metadata(riakc_obj(), [term()]) -> riakc_obj().
choose_first_metadata(Obj, [First, _ | _]) ->
    %% We only need to arbitrarily choose metadata if there
    %% are siblings.
    riakc_obj:update_metadata(Obj, First);
choose_first_metadata(Obj, _Metadatas) ->
    Obj.

%% @private
-spec bad_get_fun(bucket(), key()) -> none().
bad_get_fun(_Bucket, _Key) ->
    throw(get_fun_undefined).

%% @private
-spec bad_put_fun(riakc_obj()) -> none().
bad_put_fun(_Obj) ->
    throw(put_fun_undefined).

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
    Get = fun (Bucket, Key) -> riakc_pb_socket:get(Pid, Bucket, Key) end,
    Put = fun (Obj) -> ok = riakc_pb_socket:put(Pid, Obj) end,
    parse_option({get, Get}, parse_option({put, Put}, S)).

resolve_box(_Bucket, _Key, {ok, O},
            #statebox_riak{deserialize=Deserialize, from_values=FromValues}) ->
    {O, FromValues(
          lists:map(Deserialize, riakc_obj:get_values(O)))};
resolve_box(Bucket, Key, {error, notfound},
            #statebox_riak{from_values=FromValues}) ->
    {riakc_obj:new(Bucket, Key),
     FromValues([])}.

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

put_siblings(Pid, All=[O | _]) ->
    Bucket = riakc_obj:bucket(O),
    Key = riakc_obj:key(O),
    Vclock = riakc_obj:vclock(O),
    Contents = [{riakc_obj:get_update_metadata(Obj),
                 riakc_obj:get_value(Obj)} || Obj <- All],
    fake_statebox_riak_socket:put_raw(
      Pid,
      riakc_obj:new_obj(Bucket, Key, Vclock, Contents)).

new_test() ->
    ?assertEqual(
       #statebox_riak{},
       new([])),
    ok.

bad_config_test() ->
    {B, K, V} = {<<"b">>, <<"k">>, <<"v">>},
    S = new([]),
    ?assertThrow(
       get_fun_undefined,
       get_value(B, K, S)),
    ?assertThrow(
       put_fun_undefined,
       put_value(V, riakc_obj:new(B, K), S)),
    ok.

option_test() ->
    new([{max_queue, 0},
         {max_queue, undefined},
         {expire_ms, 0},
         {expire_ms, undefined},
         {serialize, fun serialize/1},
         {deserialize, fun deserialize/1},
         {resolve_metadatas, fun choose_first_metadata/2},
         {from_values, fun statebox_orddict:from_values/1}]),
    ok.

riak_test_() ->
    {setup,
     fun fake_statebox_riak_socket:meck_on/0,
     fun fake_statebox_riak_socket:meck_off/1,
     [{"README.md example", fun readme_riak/0},
      {"put_if_changed", fun put_if_changed_riak/0},
      {"choose_first_metadata", fun choose_first_metadata_riak/0},
      {"operations results", fun operations_results/0}]}.

choose_first_metadata_riak() ->
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    S = statebox_riak:new([{riakc_pb_socket, Pid}]),
    statebox_riak:apply_bucket_ops(
        <<"friends">>,
      [{[<<"bob">>],
        statebox_orddict:f_union(following, [<<"alice">>])}],
      S),
    {O1, _} = get_pair(<<"friends">>, <<"bob">>, S),
    riakc_pb_socket:delete(Pid, <<"friends">>, <<"bob">>),
    ?assertEqual(
       [],
       get_value(<<"friends">>, <<"bob">>, S)),
    statebox_riak:apply_bucket_ops(
        <<"friends">>,
      [{[<<"bob">>],
        statebox_orddict:f_union(following, [<<"charlie">>])}],
      S),
    {O2, _} = get_pair(<<"friends">>, <<"bob">>, S),
    put_siblings(Pid, [O1, O2]),
    statebox_riak:apply_bucket_ops(
        <<"friends">>,
      [{[<<"bob">>],
        statebox_orddict:f_union(following, [<<"brogramming">>])}],
      S),
    ?assertEqual(
       [<<"alice">>, <<"brogramming">>, <<"charlie">>],
       orddict:fetch(
         following, statebox_riak:get_value(<<"friends">>, <<"bob">>, S))),
    ok.

put_if_changed_riak() ->
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    S = statebox_riak:new([{riakc_pb_socket, Pid}]),
    statebox_riak:apply_bucket_ops(
        <<"friends">>,
        [{[<<"bob">>, <<"bob">>],
          statebox_orddict:f_union(following, [<<"alice">>])}],
        S),
    ?assertEqual(
       [<<"alice">>],
       orddict:fetch(
         following, statebox_riak:get_value(<<"friends">>, <<"bob">>, S))),
    ok.

readme_riak() ->
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    S = statebox_riak:new([{riakc_pb_socket, Pid}]),
    statebox_riak:apply_bucket_ops(
        <<"friends">>,
        [{[<<"bob">>], statebox_orddict:f_union(following, [<<"alice">>])},
         {[<<"alice">>], statebox_orddict:f_union(followers, [<<"bob">>])}],
        S),
    ?assertEqual(
       [<<"alice">>],
       orddict:fetch(
         following, statebox_riak:get_value(<<"friends">>, <<"bob">>, S))),
    ?assertEqual(
       [<<"bob">>],
       orddict:fetch(
         followers, statebox_riak:get_value(<<"friends">>, <<"alice">>, S))),
    ok.


operations_results() ->
    {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
    S = statebox_riak:new([{riakc_pb_socket, Pid}]),

    put_and_assert([<<"bob">>, <<"alice">>], [<<"bob">>, <<"alice">>], S),
    put_and_assert([<<"bob">>, <<"jim">>], [<<"bob">>, <<"alice">>, <<"jim">>], S),

    ok.

put_and_assert(Followers, ExpectResult, S) ->
    Result = statebox_riak:apply_and_return(
        <<"friends">>,
        [<<"bob">>],
        statebox_orddict:f_union(followers, Followers),
        S),
    ?assertMatch([_], Result),
    [Box] = Result,
    ?assertEqual(
        ExpectResult,
        orddict:fetch(followers, statebox:value(Box))
    ).

-endif.
