%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for keys commands using erldis
%%% @end
%%%-------------------------------------------------------------------
-module(erldis_keys_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-keys">>).

-include("edis.hrl").
-include("edis_bench.hrl").

-export([bench/1, bench/2, bench/4, bench_all/0, bench_all/1, bench_all/3]).
-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([del/2, exists/2, expire/2, expireat/2, keys/2, move/2, object_refcount/2, ttl/2, type/2,
         object_encoding/2, object_idletime/2, persist/2, randomkey/2, rename/2, renamenx/2,
         sort_list_n/2, sort_list_m/2, sort_set_n/2, sort_set_m/2, sort_zset_n/2, sort_zset_m/2]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec bench_all() -> [{atom(), float()}].
bench_all() ->
  lists:map(fun(F) ->
                    io:format("Benchmarking ~p...~n", [F]),
                    Bench = bench(F),
                    io:format("~n~n\t~p: ~p~n", [F, Bench]),
                    {F, Bench}
            end, all()).

-spec bench_all([edis_bench:option()]) -> [{atom(), float()}].
bench_all(Options) ->
  lists:map(fun(F) ->
                    io:format("Benchmarking ~p...~n", [F]),
                    Bench = bench(F, Options),
                    io:format("~n~n\t~p: ~p~n", [F, Bench]),
                    {F, Bench}
            end, all()).

-spec bench_all(pos_integer(), pos_integer(), [edis_bench:option()]) -> [{atom(), float()}].
bench_all(P1, P2, Options) ->
  lists:map(fun(F) ->
                    io:format("Benchmarking ~p...~n", [F]),
                    Bench = bench(F, P1, P2, Options),
                    io:format("~n~n\t~p: ~p~n", [F, Bench]),
                    {F, Bench}
            end, all()).

-spec bench(atom()) -> float().
bench(Function) -> bench(Function, []).

-spec bench(atom(), [edis_bench:option()]) -> float().
bench(Function, Options) -> bench(Function, 6380, 6379, Options).

-spec bench(atom(), pos_integer(), pos_integer(), [edis_bench:option()]) -> float().
bench(Function, P1, P2, Options) ->
  edis_bench:bench({?MODULE, Function, [P1]}, {?MODULE, Function, [P2]},
                   Options ++
                     [{step, 10}, {start, 50},
                      {outliers,100}, {symbols, #symbols{down_down  = $x,
                                                         up_up      = $x,
                                                         up_down    = $x,
                                                         down_up    = $x,
                                                         down_none  = $e,
                                                         up_none    = $e,
                                                         none_down  = $r,
                                                         none_up    = $r}}]).

-spec all() -> [atom()].
all() -> [Fun || {Fun, _} <- ?MODULE:module_info(exports) -- edis_bench:behaviour_info(callbacks),
                 Fun =/= module_info, Fun =/= bench_all, Fun =/= bench].

-spec init([pos_integer()]) -> ok.
init([Port]) ->
  case erldis:connect(localhost,Port) of
    {ok, Client} ->
      Name = process(Port),
      case erlang:whereis(Name) of
        undefined -> true;
        _ -> erlang:unregister(Name)
      end,
      erlang:register(Name, Client),
      ok;
    Error -> throw(Error)
  end.

-spec quit([pos_integer()]) -> ok.
quit([Port]) ->
  Name = process(Port),
  case erlang:whereis(Name) of
    undefined -> ok;
    Client -> erldis_client:stop(Client)
  end,
  ok.

-spec init_per_testcase(atom(), [pos_integer()]) -> ok.
init_per_testcase(_Function, _Extra) -> ok.

-spec quit_per_testcase(atom(), [pos_integer()]) -> ok.
quit_per_testcase(_Function, _Extra) -> ok.

-spec init_per_round(atom(), [binary()], [pos_integer()]) -> ok.
init_per_round(move, Keys, [Port]) ->
  ok = erldis:select(process(Port), 1),
  _ = del(Keys, Port),
  ok = erldis:select(process(Port), 0),
  ok = erldis:flushdb(process(Port)),
  erldis:mset(process(Port), [{Key, Key} || Key <- Keys]);
init_per_round(keys, Keys, [Port]) ->
  erldis:mset(process(Port), [{Key, Key} || Key <- Keys]);
init_per_round(sort_list_n, Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  _ = erldis:lpush(process(Port), ?KEY, Keys),
  ok;
init_per_round(sort_list_m, _Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  _ = erldis:lpush(process(Port), ?KEY, [edis_util:integer_to_binary(I) || I <- lists:seq(1, 1000)]),
  ok;
init_per_round(sort_set_n, Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  _ = erldis:sadd(process(Port), ?KEY, Keys),
  ok;
init_per_round(sort_set_m, _Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  _ = erldis:sadd(process(Port), ?KEY, [edis_util:integer_to_binary(I) || I <- lists:seq(1, 1000)]),
  ok;
init_per_round(sort_zset_n, Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  _ = erldis:zadd(process(Port), ?KEY, [{1.0, Key} || Key <- Keys]),
  ok;
init_per_round(sort_zset_m, _Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  _ = erldis:zadd(process(Port), ?KEY, [{1.0 * I, edis_util:integer_to_binary(I)} || I <- lists:seq(1, 1000)]),
  ok;
init_per_round(_Fun, Keys, [Port]) ->
  erldis:mset(process(Port), [{Key, iolist_to_binary(Keys)} || Key <- Keys]).

-spec quit_per_round(atom(), [binary()], []) -> ok.
quit_per_round(exists, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(expire, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(expireat, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(move, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(object_refcount, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(object_encoding, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(object_idletime, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(persist, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(rename, Keys, [Port]) -> del([?KEY|Keys], Port), ok;
quit_per_round(renamenx, Keys, [Port]) -> del([?KEY|Keys], Port), ok;
quit_per_round(ttl, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(type, Keys, [Port]) -> del(Keys, Port), ok;
quit_per_round(_, _Keys, _Extra) -> ok.

-spec del([binary()], pos_integer()) -> pos_integer().
del(Keys, Port) ->
  erldis:delkeys(process(Port), Keys).

-spec exists([binary(),...], pos_integer()) -> boolean().
exists([Key|_], Port) ->
  erldis:exists(process(Port), Key).

-spec expire([binary(),...], pos_integer()) -> boolean().
expire([Key|_], Port) ->
  erldis:expire(process(Port), Key, edis_util:binary_to_integer(Key, 0)).

-spec expireat([binary(),...], pos_integer()) -> boolean().
expireat([Key|_], Port) ->
  erldis:expireat(process(Port), Key, edis_util:binary_to_integer(Key, 0)).

-spec keys([binary()], pos_integer()) -> pos_integer().
keys(_Keys, Port) ->
  erldis:keys(process(Port), <<"*">>).

-spec move([binary(),...], pos_integer()) -> boolean().
move([Key|_], Port) ->
  erldis:move(process(Port), Key, 1).

-spec object_refcount([binary(),...], pos_integer()) -> integer().
object_refcount([Key|_], Port) ->
  erldis:object_refcount(process(Port), Key).

-spec object_encoding([binary(),...], pos_integer()) -> binary().
object_encoding([Key|_], Port) ->
  erldis:object_encoding(process(Port), Key).

-spec object_idletime([binary(),...], pos_integer()) -> integer().
object_idletime([Key|_], Port) ->
  erldis:object_idletime(process(Port), Key).

-spec persist([binary(),...], pos_integer()) -> boolean().
persist([Key|_], Port) ->
  erldis:persist(process(Port), Key).

-spec randomkey([binary()], pos_integer()) -> binary().
randomkey(_Keys, Port) ->
  erldis:randomkey(process(Port)).

-spec rename([binary(),...], pos_integer()) -> ok.
rename([Key|_], Port) ->
  erldis:rename(process(Port), Key, ?KEY).

-spec renamenx([binary(),...], pos_integer()) -> boolean().
renamenx([Key|_], Port) ->
  erldis:renamenx(process(Port), Key, ?KEY).

-spec ttl([binary(),...], pos_integer()) -> number().
ttl([Key|_], Port) ->
  erldis:ttl(process(Port), Key).

-spec type([binary(),...], pos_integer()) -> binary().
type([Key|_], Port) ->
  erldis:type(process(Port), Key).

-spec sort_list_n([binary(),...], pos_integer()) -> [binary()].
sort_list_n(Keys, Port) -> sort_n(Keys, Port).

-spec sort_set_n([binary(),...], pos_integer()) -> [binary()].
sort_set_n(Keys, Port) -> sort_n(Keys, Port).

-spec sort_zset_n([binary(),...], pos_integer()) -> [binary()].
sort_zset_n(Keys, Port) -> sort_n(Keys, Port).

-spec sort_n([binary(),...], pos_integer()) -> [binary()].
sort_n(_Keys, Port) ->
  erldis:sort(process(Port), ?KEY).

-spec sort_list_m([binary(),...], pos_integer()) -> [binary()].
sort_list_m(Keys, Port) -> sort_m(Keys, Port).

-spec sort_set_m([binary(),...], pos_integer()) -> [binary()].
sort_set_m(Keys, Port) -> sort_m(Keys, Port).

-spec sort_zset_m([binary(),...], pos_integer()) -> [binary()].
sort_zset_m(Keys, Port) -> sort_m(Keys, Port).

-spec sort_m([binary(),...], pos_integer()) -> [binary()].
sort_m(Keys, Port) ->
  erldis:sort(process(Port), ?KEY, <<"LIMIT 0 ", (edis_util:integer_to_binary(length(Keys)))/binary>>).

process(Port) -> list_to_atom("erldis-tester-" ++ integer_to_list(Port)).