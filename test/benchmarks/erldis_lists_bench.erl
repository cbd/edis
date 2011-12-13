%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for lists commands using erldis
%%% @end
%%%-------------------------------------------------------------------
-module(erldis_lists_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-list">>).

-include("edis.hrl").
-include("edis_bench.hrl").

-export([bench/1, bench/2, bench/4, bench_all/0, bench_all/1, bench_all/3]).
-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([blpop/2, blpop_nothing/2, brpop/2, brpop_nothing/2, brpoplpush/2, lindex/2, linsert/2,
         llen/2, lpop/2, lpush/2, lpushx/2, lrange_s/2, lrange_n/2, lrem_x/2, lrem_y/2, lrem_0/2,
         lset/2, ltrim/2, rpop/2, rpoplpush/2, rpoplpush_self/2, rpush/2, rpushx/2]).

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
                 Fun =/= module_info, Fun =/= bench_all, Fun =/= bench, Fun =/= blpop_nothing,
                 Fun =/= brpop_nothing].

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
init_per_round(Fun, Keys, [Port]) when Fun =:= blpop_nothing;
                                       Fun =:= brpop_nothing ->
  _ = erldis:del(process(Port), [?KEY | Keys]),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= lindex;
                                       Fun =:= lrange_s;
                                       Fun =:= lrange_n;
                                       Fun =:= lrem;
                                       Fun =:= ltrim ->
  _ =
    erldis:lpush(process(Port), ?KEY, [<<"x">> || _ <- lists:seq(1, erlang:max(5000, length(Keys)))]),
  ok;
init_per_round(lset, Keys, [Port]) ->
  _ = erldis:lpush(process(Port), ?KEY, [<<"y">> || _ <- Keys]),
  ok;
init_per_round(_Fun, Keys, [Port]) ->
  _ = erldis:lpush(process(Port), ?KEY, Keys),
  ok.

-spec quit_per_round(atom(), [binary()], []) -> ok.
quit_per_round(_, _Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  ok.

-spec blpop_nothing([binary()], pos_integer()) -> timeout.
blpop_nothing(Keys, Port) ->
  erldis:blpop(process(Port), Keys ++ [1]).

-spec blpop([binary()], pos_integer()) -> [binary()].
blpop(_Keys, Port) ->
  erldis:blpop(process(Port), [?KEY, 1000]).

-spec brpop_nothing([binary()], pos_integer()) -> timeout.
brpop_nothing(Keys, Port) ->
  erldis:brpop(process(Port), Keys ++ [1]).

-spec brpop([binary()], pos_integer()) -> undefined.
brpop(_Keys, Port) ->
  erldis:brpop(process(Port), [?KEY, 1000]).

-spec brpoplpush([binary()], pos_integer()) -> undefined.
brpoplpush(_Keys, Port) ->
  erldis:brpoplpush(process(Port), [?KEY, <<(?KEY)/binary, "-2">>, 1000], infinity).

-spec lindex([binary()], pos_integer()) -> binary().
lindex(Keys, Port) ->
  erldis:lindex(process(Port), ?KEY, length(Keys)).

-spec linsert([binary()], pos_integer()) -> binary().
linsert([Key|_], Port) ->
  erldis:linsert(process(Port), ?KEY, <<"BEFORE">>, Key, <<"x">>).

-spec llen([binary()], pos_integer()) -> binary().
llen(_Keys, Port) ->
  erldis:llen(process(Port), ?KEY).

-spec lpop([binary()], pos_integer()) -> binary().
lpop(_Keys, Port) ->
  erldis:lpop(process(Port), ?KEY).

-spec lpush([binary()], pos_integer()) -> integer().
lpush(_Keys, Port) ->
  erldis:lpush(process(Port), ?KEY, ?KEY).

-spec lpushx([binary()], pos_integer()) -> integer().
lpushx(_Keys, Port) ->
  erldis:lpushx(process(Port), ?KEY, ?KEY).

-spec lrange_s([binary()], pos_integer()) -> [binary()].
lrange_s([Key|_], Port) ->
  erldis:lrange(process(Port), ?KEY, edis_util:binary_to_integer(Key), edis_util:binary_to_integer(Key)).

-spec lrange_n([binary()], pos_integer()) -> [binary()].
lrange_n([Key|_], Port) ->
  erldis:lrange(process(Port), ?KEY, 0, edis_util:binary_to_integer(Key)).

-spec lrem_x([binary()], pos_integer()) -> integer().
lrem_x([Key|_], Port) ->
  erldis:lrem(process(Port), ?KEY, edis_util:binary_to_integer(Key), <<"x">>).

-spec lrem_y([binary()], pos_integer()) -> integer().
lrem_y([Key|_], Port) ->
  erldis:lrem(process(Port), ?KEY, edis_util:binary_to_integer(Key), <<"y">>).

-spec lrem_0([binary()], pos_integer()) -> integer().
lrem_0(_Keys, Port) ->
  erldis:lrem(process(Port), ?KEY, 0, <<"x">>).

-spec lset([binary()], pos_integer()) -> ok.
lset([Key|_], Port) ->
  erldis:lset(process(Port), ?KEY, erlang:trunc(edis_util:binary_to_integer(Key) / 2), <<"x">>).

-spec ltrim([binary()], pos_integer()) -> ok.
ltrim([Key|_], Port) ->
  erldis:ltrim(process(Port), ?KEY, edis_util:binary_to_integer(Key), -1).

-spec rpop([binary()], pos_integer()) -> binary().
rpop(_Keys, Port) ->
  erldis:rpop(process(Port), ?KEY).

-spec rpush([binary()], pos_integer()) -> integer().
rpush(_Keys, Port) ->
  erldis:rpush(process(Port), ?KEY, ?KEY).

-spec rpushx([binary()], pos_integer()) -> integer().
rpushx(_Keys, Port) ->
  erldis:rpushx(process(Port), ?KEY, ?KEY).

-spec rpoplpush([binary()], pos_integer()) -> binary().
rpoplpush(_Keys, Port) ->
  erldis:rpoplpush(process(Port), ?KEY, <<(?KEY)/binary, "-2">>).

-spec rpoplpush_self([binary()], pos_integer()) -> binary().
rpoplpush_self(_Keys, Port) ->
  erldis:rpoplpush(process(Port), ?KEY, ?KEY).

process(Port) -> list_to_atom("erldis-tester-" ++ integer_to_list(Port)).