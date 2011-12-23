%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for sets commands using erldis
%%% @end
%%%-------------------------------------------------------------------
-module(erldis_sets_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-set">>).
-define(KEY2, <<"test-set2">>).

-include("edis.hrl").
-include("edis_bench.hrl").

-export([bench/1, bench/2, bench/4, bench_all/0, bench_all/1, bench_all/3]).
-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([sadd/2, scard/2, sdiff/2, sdiffstore/2, sinter_min/2, sinter_n/2, sinter_m/2,
         sinterstore_min/2, sinterstore_n/2, sinterstore_m/2, sismember/2, smembers/2,
         smove/2, spop/2, srandmember/2, srem/2, sunion/2, sunionstore/2]).

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
                 Fun =/= module_info, Fun =/= bench_all, Fun =/= bench, Fun =/= sadd].

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
init_per_round(Fun, Keys, [Port]) when Fun =:= scard;
                                       Fun =:= sismember;
                                       Fun =:= smembers;
                                       Fun =:= smove;
                                       Fun =:= spop;
                                       Fun =:= srandmember ->
  sadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= sinter_min;
                                       Fun =:= sinterstore_min ->
  erldis:sadd(process(Port), ?KEY2, <<"1">>),
  sadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= sinter_n;
                                       Fun =:= sinterstore_n ->
  erldis:sadd(process(Port), ?KEY2, Keys),
  sadd(Keys, Port),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= sinter_m;
                                       Fun =:= sinterstore_m ->
  lists:foreach(
    fun(Key) ->
            erldis:sadd(process(Port), Key, [?KEY, ?KEY2, Key])
    end, Keys);
init_per_round(Fun, Keys, [Port]) when Fun =:= sdiff;
                                       Fun =:= sdiffstore;
                                       Fun =:= sunion;
                                       Fun =:= sunionstore ->
  erldis:sadd(process(Port), ?KEY2,
              lists:map(
                fun edis_util:integer_to_binary/1, lists:seq(1, 100))),
  sadd(Keys, Port),
  ok;
init_per_round(Fun, _Keys, [Port]) when Fun =:= srem ->
  erldis:sadd(
    process(Port), ?KEY,
    lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, 10000))),
  ok;
init_per_round(_Fun, _Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  ok.

-spec quit_per_round(atom(), [binary()], [pos_integer()]) -> ok.
quit_per_round(_, Keys, [Port]) ->
  _ = erldis:delkeys(process(Port), [?KEY, ?KEY2 | Keys]),
  ok.

-spec sadd([binary()], pos_integer()) ->pos_integer().
sadd(Keys, Port) ->
  erldis:sadd(process(Port), ?KEY, Keys).

-spec scard([binary()], pos_integer()) ->pos_integer().
scard(_Keys, Port) ->
  erldis:scard(process(Port), ?KEY).

-spec sdiff([binary()], pos_integer()) ->[binary()].
sdiff(_Keys, Port) ->
  erldis:sdiff(process(Port), [?KEY, ?KEY2]).

-spec sdiffstore([binary()], pos_integer()) ->[binary()].
sdiffstore(_Keys, Port) ->
  erldis:sdiffstore(process(Port), ?KEY, [?KEY, ?KEY2]).

-spec sinter_min([binary()], pos_integer()) ->[binary()].
sinter_min(_Keys, Port) ->
  erldis:sinter(process(Port), [?KEY, ?KEY2]).

-spec sinter_n([binary()], pos_integer()) ->[binary()].
sinter_n(_Keys, Port) ->
  erldis:sinter(process(Port), [?KEY, ?KEY2]).

-spec sinter_m([binary()], pos_integer()) ->[binary()].
sinter_m(Keys, Port) ->
  erldis:sinter(process(Port), Keys).

-spec sinterstore_min([binary()], pos_integer()) ->[binary()].
sinterstore_min(_Keys, Port) ->
  erldis:sinterstore(process(Port), ?KEY, [?KEY, ?KEY2]).

-spec sinterstore_n([binary()], pos_integer()) ->[binary()].
sinterstore_n(_Keys, Port) ->
  erldis:sinterstore(process(Port), ?KEY, [?KEY, ?KEY2]).

-spec sinterstore_m([binary()], pos_integer()) ->[binary()].
sinterstore_m(Keys, Port) ->
  erldis:sinterstore(process(Port), ?KEY, Keys).

-spec sismember([binary()], pos_integer()) ->true.
sismember([Key|_], Port) ->
  erldis:sismember(process(Port), ?KEY, Key).

-spec smembers([binary()], pos_integer()) ->[binary()].
smembers(_, Port) ->
  erldis:smembers(process(Port), ?KEY).

-spec smove([binary()], pos_integer()) ->boolean().
smove([Key|_], Port) ->
  erldis:smove(process(Port), ?KEY, ?KEY2, Key).

-spec spop([binary()], pos_integer()) ->binary().
spop(_Keys, Port) ->
  erldis:spop(process(Port), ?KEY).

-spec srandmember([binary()], pos_integer()) ->binary().
srandmember(_Keys, Port) ->
  erldis:srandmember(process(Port), ?KEY).

-spec srem([binary()], pos_integer()) ->number().
srem(Keys, Port) ->
  erldis:srem(process(Port), ?KEY, Keys).

-spec sunion([binary()], pos_integer()) ->[binary()].
sunion(_Keys, Port) ->
  erldis:sunion(process(Port), [?KEY, ?KEY2]).

-spec sunionstore([binary()], pos_integer()) ->[binary()].
sunionstore(_Keys, Port) ->
  erldis:sunionstore(process(Port), ?KEY, [?KEY, ?KEY2]).

process(Port) -> list_to_atom("erldis-tester-" ++ integer_to_list(Port)).