%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for zsets commands using erldis
%%% @end
%%%-------------------------------------------------------------------
-module(erldis_zsets_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-zset">>).
-define(KEY2, <<"test-zset2">>).

-include("edis.hrl").
-include("edis_bench.hrl").

-export([bench/1, bench/2, bench/4, bench_all/0, bench_all/1, bench_all/3]).
-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([zadd/2, zadd_one/2, zcard/2, zcount_n/2, zcount_m/2, zincrby/2,
         zinterstore_min/2, zinterstore_n/2, zinterstore_k/2, zinterstore_m/2,
         zrange_n/2, zrange_m/2, zrangebyscore_n/2, zrangebyscore_m/2, zrank/2,
         zrem/2, zrem_one/2, zremrangebyrank_n/2, zremrangebyrank_m/2,
         zremrangebyscore_n/2, zremrangebyscore_m/2, zrevrange_n/2, zrevrange_m/2,
         zrevrangebyscore_n/2, zrevrangebyscore_m/2, zrevrank/2, zscore/2,
         zunionstore_n/2, zunionstore_m/2]).

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
init_per_round(Fun, Keys, [Port]) when Fun =:= zcard;
                                       Fun =:= zadd_one;
                                       Fun =:= zincrby;
                                       Fun =:= zremrangebyrank_n;
                                       Fun =:= zrevrange_n ->
  zadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= zinterstore_min ->
  erldis:zadd(
    process(Port), ?KEY2,
    [{1.0, <<"1">>}] ++
      [{random:uniform(I) * 1.0,
        <<(edis_util:integer_to_binary(I))/binary, "-never-match">>} || I <- lists:seq(1, 10000)]),
  zadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= zinterstore_n ->
  erldis:zadd(
    process(Port), ?KEY2,
    [{1.0, <<"1">>} |
       [{random:uniform(length(Keys)) * 1.0,
         <<Key/binary, "-never-match">>} || Key <- Keys, Key =/= <<"1">>]]),
  zadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= zinterstore_k ->
  lists:foreach(
    fun(Key) ->
            erldis:zadd(process(Port), Key, [{1.0, ?KEY}, {2.0, ?KEY2}, {3.0, Key}])
    end, Keys);
init_per_round(Fun, Keys, [Port]) when Fun =:= zinterstore_m ->
  L = length(Keys),
  erldis:zadd(
    process(Port), ?KEY2, 
    [{random:uniform(L) * 1.0, Key} || Key <- Keys] ++
      [{random:uniform(I) * 1.0,
        <<(edis_util:integer_to_binary(I))/binary, "-never-match">>} || I <- lists:seq(L, 10000)]),
  erldis:zadd(
    process(Port), ?KEY, 
    [{random:uniform(L) * 1.0, Key} || Key <- Keys] ++
      [{random:uniform(I) * 1.0, edis_util:integer_to_binary(I)} || I <- lists:seq(L, 10000)]),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= zunionstore_n ->
  erldis:zadd(
    process(Port), ?KEY2, 
    [{random:uniform(I) * 1.0, edis_util:integer_to_binary(I)} || I <- lists:seq(1, 10000)]),
  zadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= zunionstore_m ->
  L = length(Keys),
  erldis:zadd(
    process(Port), ?KEY2,
    [{random:uniform(L) * 1.0, <<Key/binary, "-never-match">>} || Key <- Keys] ++
      [{random:uniform(I) * 1.0, edis_util:integer_to_binary(I)} || I <- lists:seq(L, 10000)]),
  erldis:zadd(
    process(Port), ?KEY,
    [{random:uniform(L) * 1.0, Key} || Key <- Keys] ++
      [{random:uniform(I) * 1.0, edis_util:integer_to_binary(I)} || I <- lists:seq(L, 10000)]),
  ok;
init_per_round(Fun, _Keys, [Port]) when Fun =:= zcount_m;
                                        Fun =:= zrange_m;
                                        Fun =:= zrem;
                                        Fun =:= zrangebyscore_m;
                                        Fun =:= zremrangebyrank_m;
                                        Fun =:= zremrangebyscore_m;
                                        Fun =:= zrevrange_m;
                                        Fun =:= zrevrangebyscore_m ->
  erldis:zadd(process(Port), ?KEY,
              [{1.0 * I, edis_util:integer_to_binary(I)} || I <- lists:seq(1, 10000)]),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= zcount_n;
                                       Fun =:= zrange_n;
                                       Fun =:= zrank;
                                       Fun =:= zrevrank;
                                       Fun =:= zrangebyscore_n;
                                       Fun =:= zremrangebyscore_n;
                                       Fun =:= zrevrangebyscore_n;
                                       Fun =:= zrem_one;
                                       Fun =:= zscore ->
  erldis:zadd(process(Port), ?KEY, [{edis_util:binary_to_float(Key), Key} || Key <- Keys]),
  ok;
init_per_round(_Fun, _Keys, [Port]) ->
  erldis:del(process(Port), ?KEY),
  ok.

-spec quit_per_round(atom(), [binary()], [pos_integer()]) -> ok.
quit_per_round(_, Keys, [Port]) ->
  erldis:delkeys(process(Port), [?KEY, ?KEY2 | Keys]),
  ok.

-spec zadd_one([binary()], pos_integer()) -> pos_integer().
zadd_one(_Keys, Port) ->
  erldis:zadd(process(Port), ?KEY, 1.0, ?KEY2).

-spec zadd([binary()], pos_integer()) -> pos_integer().
zadd(Keys, Port) ->
  erldis:zadd(process(Port), ?KEY, [{1.0, Key} || Key <- Keys]).

-spec zcard([binary()], pos_integer()) -> pos_integer().
zcard(_Keys, Port) ->
  erldis:zcard(process(Port), ?KEY).

-spec zcount_n([binary()], pos_integer()) -> pos_integer().
zcount_n(_Keys, Port) ->
  erldis:zcount(process(Port), ?KEY, 1.0, 1.0).

-spec zcount_m([binary()], pos_integer()) -> pos_integer().
zcount_m([Key|_], Port) ->
  erldis:zcount(process(Port), ?KEY, 1.0, edis_util:binary_to_float(Key)).

-spec zincrby([binary()], pos_integer()) -> binary().
zincrby([Key|_], Port) ->
  erldis:zincrby(process(Port), ?KEY, 1.0, Key).

-spec zinterstore_min([binary()], pos_integer()) -> number().
zinterstore_min(_Keys, Port) ->
  erldis:zinterstore(process(Port), ?KEY, [?KEY2, ?KEY], sum).

-spec zinterstore_n([binary()], pos_integer()) -> number().
zinterstore_n(_Keys, Port) ->
  erldis:zinterstore(process(Port), ?KEY, [?KEY, ?KEY2], max).

-spec zinterstore_k([binary()], pos_integer()) -> number().
zinterstore_k(Keys, Port) ->
  erldis:zinterstore(process(Port), ?KEY, Keys, min).

-spec zinterstore_m([binary()], pos_integer()) -> number().
zinterstore_m(_Keys, Port) ->
  erldis:zinterstore(process(Port), ?KEY, [?KEY, ?KEY2], sum).

-spec zrange_n([binary()], pos_integer()) -> [binary()].
zrange_n(_Keys, Port) ->
  erldis:zrange(process(Port), ?KEY, -2, -1).

-spec zrange_m([binary()], pos_integer()) -> [binary()].
zrange_m([Key|_], Port) ->
  erldis:zrange_withscores(process(Port), ?KEY, 0, edis_util:binary_to_integer(Key)).

-spec zrangebyscore_n([binary()], pos_integer()) -> [binary()].
zrangebyscore_n(_Keys, Port) ->
  erldis:zrangebyscore(process(Port), ?KEY, <<"-inf">>, 1.0).

-spec zrangebyscore_m([binary()], pos_integer()) -> [binary()].
zrangebyscore_m([Key|_], Port) ->
  erldis:zrangebyscore(process(Port), ?KEY, <<"-inf">>, edis_util:binary_to_float(Key)).

-spec zrank([binary()], pos_integer()) -> number().
zrank([Key|_], Port) ->
  erldis:zrank(process(Port), ?KEY, Key).

-spec zrem_one([binary()], pos_integer()) -> pos_integer().
zrem_one([Key|_], Port) ->
  erldis:zrem(process(Port), ?KEY, Key).

-spec zrem([binary()], pos_integer()) -> pos_integer().
zrem([Key|_], Port) ->
  erldis:zrem(process(Port), ?KEY, [Key]).

-spec zremrangebyrank_n([binary()], pos_integer()) -> number().
zremrangebyrank_n(_Keys, Port) ->
  erldis:zremrangebyrank(process(Port), ?KEY, -20, -10).

-spec zremrangebyrank_m([binary()], pos_integer()) -> [binary()].
zremrangebyrank_m([Key|_], Port) ->
  erldis:zremrangebyrank(process(Port), ?KEY, 0, edis_util:binary_to_integer(Key)).

-spec zremrangebyscore_n([binary()], pos_integer()) -> number().
zremrangebyscore_n([Key|_], Port) ->
  L = edis_util:binary_to_float(Key),
  erldis:zremrangebyscore(process(Port), ?KEY, L, L).

-spec zremrangebyscore_m([binary()], pos_integer()) -> [binary()].
zremrangebyscore_m([Key|_], Port) ->
  erldis:zremrangebyscore(process(Port), ?KEY, <<"-inf">>, edis_util:binary_to_float(Key)).

-spec zrevrange_n([binary()], pos_integer()) -> [binary()].
zrevrange_n(_Keys, Port) ->
  erldis:zrevrange(process(Port), ?KEY, -2, -1).

-spec zrevrange_m([binary()], pos_integer()) -> [binary()].
zrevrange_m([Key|_], Port) ->
  erldis:zrevrange(process(Port), ?KEY, 0, edis_util:binary_to_integer(Key)).

-spec zrevrangebyscore_n([binary()], pos_integer()) -> [binary()].
zrevrangebyscore_n(_Keys, Port) ->
  erldis:zrevrangebyscore(process(Port), ?KEY, 1.0, <<"-inf">>).

-spec zrevrangebyscore_m([binary()], pos_integer()) -> [binary()].
zrevrangebyscore_m([Key|_], Port) ->
  erldis:zrevrangebyscore(process(Port), ?KEY, edis_util:binary_to_float(Key), <<"-inf">>).

-spec zrevrank([binary()], pos_integer()) -> number().
zrevrank(_Keys, Port) ->
  erldis:zrevrank(process(Port), ?KEY, <<"1">>).

-spec zscore([binary()], pos_integer()) -> number().
zscore([Key|_], Port) ->
  erldis:zscore(process(Port), ?KEY, Key).

-spec zunionstore_n([binary()], pos_integer()) -> number().
zunionstore_n(_Keys, Port) ->
  erldis:zunionstore(process(Port), ?KEY, [?KEY, ?KEY2], sum).

-spec zunionstore_m([binary()], pos_integer()) -> number().
zunionstore_m(_Keys, Port) ->
  erldis:zunionstore(process(Port), ?KEY, [?KEY2, ?KEY], max).

process(Port) -> list_to_atom("erldis-tester-" ++ integer_to_list(Port)).