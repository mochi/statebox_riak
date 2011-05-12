%% @doc Module for mocking riakc_pb_socket with an ETS table backed fake.
%%      Usage is as follows:
%%      <pre>
%%      -ifdef(TEST).
%%      -include_lib("eunit/include/eunit.hrl").
%%      riak_test_() ->
%%          {setup,
%%           %% NOTE: meck_off/1 requires the return value of meck_on/0!
%%           fun fake_mochiak_client:meck_on/0,
%%           fun fake_mochiak_client:meck_off/1,
%%           %% NOTE: It's important that the name of your test functions
%%           %%       do not end with test or test_, otherwise eunit
%%           %%       would pick them up!
%%           [fun your_test_riak/0]}.
%%      -endif.
%%      </pre>
-module(fake_statebox_riak_socket).
-export([meck_on/0,
         meck_off/1,
         delete/3,
         get/3,
         put/2,
         put_raw/2]).
-compile({no_auto_import,[put/2]}).
-define(TABLE, ?MODULE).

-type obj() :: riakc_obj:riakc_obj().

-spec meck_on() -> [module()].
meck_on() ->
    M = riakc_pb_socket,
    ?TABLE = ets:new(?TABLE, [public, named_table]),
    true = ets:delete_all_objects(?TABLE),
    meck:new(M),
    meck:expect(M, set_bucket, 3, ok),
    meck:expect(M, start_link, 2, {ok, self()}),
    meck:expect(M, delete,
                fun ?MODULE:delete/3),
    meck:expect(M, delete,
                fun (Pid, Bucket, Key, _, _) -> delete(Pid, Bucket, Key) end),
    meck:expect(M, get,
                fun ?MODULE:get/3),
    meck:expect(M, get,
                fun (Pid, Bucket, Key, _, _) -> get(Pid, Bucket, Key) end),
    meck:expect(M, put,
                fun ?MODULE:put/2),
    meck:expect(M, put,
                fun (Pid, Obj, _, _) -> put(Pid, Obj) end),
    [M].

-spec meck_off([module()]) -> ok.
meck_off(Mods) ->
    catch ets:delete(?TABLE),
    meck:unload(Mods),
    ok.

-spec delete(pid(), binary(), binary()) -> ok | {error, notfound}.
delete(_Pid, Bucket, Key) ->
    case ets:member(?TABLE, {Bucket, Key}) of
        false ->
            {error, notfound};
        true ->
            ets:delete(?TABLE, {Bucket, Key}),
            ok
    end.

-spec get(pid(), binary(), binary()) -> {ok, obj()} | {error, notfound}.
get(_Pid, Bucket, Key) ->
    case ets:lookup(?TABLE, {Bucket, Key}) of
        [{{_Bucket, _Key}, O}] ->
            {ok, O};
        [] ->
            {error, notfound}
    end.

-spec put(pid(), obj()) -> ok.
put(Pid, O) ->
    Bucket = riakc_obj:bucket(O),
    Key = riakc_obj:key(O),
    Vclock = riakc_obj:vclock(O),
    Value = riakc_obj:get_update_value(O),
    Contents = [{riakc_obj:get_update_metadata(O), Value}],
    put_raw(Pid, riakc_obj:new_obj(Bucket, Key, Vclock, Contents)).

-spec put_raw(pid(), obj()) -> ok.
put_raw(_Pid, O) ->
    Bucket = riakc_obj:bucket(O),
    Key = riakc_obj:key(O),
    true = ets:insert(?TABLE, [{{Bucket, Key}, O}]),
    ok.
