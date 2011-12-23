%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for string commands using erldis
%%% @end
%%%-------------------------------------------------------------------
-module(erldis_strings_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-string">>).

-include("edis.hrl").
-include("edis_bench.hrl").

-export([bench/1, bench/2, bench/4, bench_all/0, bench_all/1, bench_all/3]).
-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([append/2, decr/2, decrby/2, get/2, getbit/2, getrange/2, getset/2, incr/2, incrby/2,
         mget/2, mset/2, msetnx/2, set/2, setex/2, setnx/2, setbit/2, setrange/2, strlen/2]).

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
                 Fun =/= module_info, Fun =/= bench_all, Fun =/= bench, Fun =/= msetnx].

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
init_per_round(Fun, [Key|_], [Port]) when Fun =:= decr;
                                          Fun =:= decrby;
                                          Fun =:= incr;
                                          Fun =:= incrby ->
  erldis:set(process(Port), ?KEY, Key);
init_per_round(mget, Keys, [Port]) ->
  erldis:mset(process(Port), [{Key, <<"X">>} || Key <- Keys]);
init_per_round(Fun, Keys, [Port]) when Fun =:= mset;
                                       Fun =:= msetnx ->
  _ = erldis:delkeys(process(Port), Keys),
  ok;
init_per_round(Fun, _Keys, [Port]) when Fun =:= set;
                                        Fun =:= setex;
                                        Fun =:= setnx->
  _ = erldis:del(process(Port), ?KEY),
  ok;
init_per_round(_Fun, Keys, [Port]) ->
  erldis:set(process(Port), ?KEY, list_to_binary(lists:duplicate(length(Keys), $X))).

-spec quit_per_round(atom(), [binary()], [pos_integer()]) -> ok.
quit_per_round(_, _Keys, [Port]) ->
  erldis:flushdb(process(Port)).

-spec append([binary()], pos_integer()) -> pos_integer().
append([Key|_], Port) ->
  erldis:append(process(Port), ?KEY, Key).

-spec decr([binary()], pos_integer()) -> integer().
decr(_, Port) ->
  erldis:decr(process(Port), ?KEY).

-spec decrby([binary()], pos_integer()) -> integer().
decrby([Key|_], Port) ->
  erldis:decrby(process(Port), ?KEY, edis_util:binary_to_integer(Key)).

-spec get([binary()], pos_integer()) -> binary().
get(_, Port) ->
  erldis:get(process(Port), ?KEY).

-spec getbit([binary()], pos_integer()) -> 1 | 0.
getbit([Key|_], Port) ->
  erldis:getbit(process(Port), ?KEY, random:uniform(edis_util:binary_to_integer(Key)) - 1).

-spec getrange([binary()], pos_integer()) -> binary().
getrange(_, Port) ->
  erldis:getrange(process(Port), ?KEY, 1, -2).

-spec getset([binary()], pos_integer()) -> binary().
getset([Key|_], Port) ->
  erldis:getset(process(Port), ?KEY, Key).

-spec incr([binary()], pos_integer()) -> integer().
incr(_, Port) ->
  erldis:incr(process(Port), ?KEY).

-spec incrby([binary()], pos_integer()) -> integer().
incrby([Key|_], Port) ->
  erldis:incrby(process(Port), ?KEY, edis_util:binary_to_integer(Key)).

-spec mget([binary()], pos_integer()) -> [binary()].
mget(Keys, Port) ->
  erldis:mget(process(Port), Keys).

-spec mset([binary()], pos_integer()) -> ok.
mset(Keys, Port) ->
  erldis:mset(process(Port), [{Key, <<"X">>} || Key <- Keys]).

-spec msetnx([binary()], pos_integer()) -> ok.
msetnx(Keys, Port) ->
  erldis:msetnx(process(Port), [{Key, <<"X">>} || Key <- Keys]).

-spec set([binary()], pos_integer()) -> ok.
set([Key|_], Port) ->
  erldis:set(process(Port), ?KEY,
             unicode:characters_to_binary(lists:duplicate(edis_util:binary_to_integer(Key), $X))).

-spec setex([binary()], pos_integer()) -> ok.
setex([Key|_], Port) ->
  erldis:setex(process(Port), ?KEY, edis_util:binary_to_integer(Key),
               unicode:characters_to_binary(lists:duplicate(edis_util:binary_to_integer(Key), $X))).

-spec setnx([binary()], pos_integer()) -> ok.
setnx([Key|_], Port) ->
  erldis:setnx(process(Port), ?KEY,
               unicode:characters_to_binary(lists:duplicate(edis_util:binary_to_integer(Key), $X))).

-spec setbit([binary()], pos_integer()) -> 1 | 0.
setbit([Key|_], Port) ->
  erldis:setbit(process(Port), ?KEY,
                random:uniform(edis_util:binary_to_integer(Key)) - 1, random:uniform(2) - 1).

-spec setrange([binary()], pos_integer()) -> pos_integer().
setrange([Key|_], Port) ->
  erldis:setrange(process(Port), ?KEY, random:uniform(edis_util:binary_to_integer(Key)) - 1, <<"xxx">>).

-spec strlen([binary()], pos_integer()) -> pos_integer().
strlen(_Keys, Port) ->
  erldis:strlen(process(Port), ?KEY).

process(Port) -> list_to_atom("erldis-tester-" ++ integer_to_list(Port)).