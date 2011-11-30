%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarker. Given a module to test with, it helps users determine
%%%      the order of functions
%%% @end
%%%-------------------------------------------------------------------
-module(edis_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").
-include("edis_bench.hrl").

-type symbols() :: #symbols{}.
-export_type([symbols/0]).

-type option() :: {start, pos_integer} | {step, pos_integer()} | {rounds, pos_integer()} |
        {extra_args, [term()]} | {outliers, pos_integer()} | {columns, pos_integer()} |
        {first_col, pos_integer()} | {rows, pos_integer()} | debug | {k, number()} | {x, number()} |
        {symbols, symbols()}.
-export_type([option/0]).

-export([bench/4, bench/3, bench/2, behaviour_info/1]).

-export([zero/1, constant/1, linear/1, quadratic/1, logarithmic/1, xlogarithmic/1, exponential/1]).

%% ====================================================================
%% External functions
%% ====================================================================
%% @hidden
-spec behaviour_info(callbacks|term()) -> [{atom(), non_neg_integer()}].
behaviour_info(callbacks) ->
  [{all, 0},
   {init, 1}, {init_per_testcase, 2}, {init_per_round, 3},
   {quit, 1}, {quit_per_testcase, 2}, {quit_per_round, 3}].

%% @doc Runs all the benchmarking functions on Module against {@link zero/0} function.
%%      The list is obtained calling Module:all().
-spec bench(atom(), [option()]) -> ok.
bench(Module, Options) ->
  ok = try Module:init(proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end,
  try
    lists:foreach(
      fun(Function) ->
              io:format("~n~p:~p ...~n", [Module, Function]),
              bench(Module, Function, zero, Options)
      end, Module:all())
  after
      try Module:quit(proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end
  end.

%% @doc Compares the different runs of Module:Function to a given function.
%%      Returns the standard deviation of the distances between them (outliers excluded).
%%      The higher the value the more different functions are.
-spec bench(atom(), atom(), atom() | fun((pos_integer()) -> number()), [option()]) -> ok.
bench(Module, Function, MathFunction, Options) when is_atom(MathFunction) ->
  bench(Module, Function, fun(X) -> ?MODULE:MathFunction(X) end, Options);
bench(Module, Function, MathFunction, Options) ->
  RawResults = run(Module, Function, Options),
  graph(
    [{K, V, proplists:get_value(x, Options, 0) +
        (proplists:get_value(k, Options, 1) * MathFunction(K))} || {K,V} <- RawResults],
    Options).

%% @doc Compares the different runs of Module1:Function1 with Module2:Function2
%%      Returns the standard deviation of the distances between them (outliers excluded).
%%      The higher the value the more different functions are.
-spec bench({atom(), atom(), [term()]}, {atom(), atom(), [term()]}, [option()]) -> ok.
bench({Module1, Function1, ExtraArgs1}, {Module2, Function2, ExtraArgs2}, GlobalOptions) ->
  RawResults1 = run(Module1, Function1, [{extra_args, ExtraArgs1}|GlobalOptions]),
  RawResults2 = run(Module2, Function2, [{extra_args, ExtraArgs2}|GlobalOptions]),
  RawResults = lists:zipwith(fun({K,V1}, {K,V2}) -> {K, V1, V2} end, RawResults1, RawResults2),
  graph(RawResults, GlobalOptions).

%% ====================================================================
%% Math functions
%% ====================================================================
%% @doc O(1) comparer
-spec zero(pos_integer()) -> pos_integer().
zero(_) -> 0.

%% @doc O(1) comparer
-spec constant(pos_integer()) -> pos_integer().
constant(_) -> 1.

%% @doc O(n) comparer
-spec linear(pos_integer()) -> pos_integer().
linear(N) -> N.

%% @doc O(n^2) comparer
-spec quadratic(pos_integer()) -> pos_integer().
quadratic(N) -> N * N.

%% @doc O(log(n)) comparer
-spec logarithmic(pos_integer()) -> float().
logarithmic(N) -> math:log(N) + 1.

%% @doc O(n*log(n)) comparer
-spec xlogarithmic(pos_integer()) -> float().
xlogarithmic(N) -> N * math:log(N) + 1.

%% @doc O(e^n) comparer
-spec exponential(pos_integer()) -> float().
exponential(N) -> math:pow(2.71828182845904523536028747135266249775724709369995, N).

%% ====================================================================
%% Internal functions
%% ====================================================================
%% @doc Runs the benchmarking function Module:Function using options.
-spec run(atom(), atom(), [option()]) -> [{pos_integer(), error | pos_integer()}].
run(Module, Function, Options) ->
  ok = try Module:init(proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end,
  try do_run(Module, Function, Options)
  after
      try Module:quit(proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end
  end.

do_run(Module, Function, Options) ->
  ok = try Module:init_per_testcase(Function, proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end,
  Start = proplists:get_value(start, Options, 1),
  try lists:map(fun(N) -> do_run(Module, Function, N, Options) end,
        lists:seq(Start,
                  Start + proplists:get_value(rounds, Options, 250) *
                    proplists:get_value(step, Options, 1),
                  proplists:get_value(step, Options, 1)))
  after
      try Module:quit_per_testcase(Function, proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end
  end.

do_run(Module, Function, N, Options) ->
  Items = lists:reverse(lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, N))),
  ok = try Module:init_per_round(Function, Items, proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end,
  try timer:tc(Module, Function, [Items | proplists:get_value(extra_args, Options, [])]) of
    {Time, Result} ->
      case proplists:get_bool(debug, Options) of
        true -> ?INFO("~p: ~p~n\t~p~n", [N, Time/1000, Result]);
        false -> ok
      end,
      {N, (Time+1)/1000}
  catch
    _:Error ->
      ?ERROR("Error on ~p:~p (N: ~p):~n\t~p~n", [Module, Function, N, Error]),
      {N, error}
  after
      try Module:quit_per_round(Function, Items, proplists:get_value(extra_args, Options, [])) catch _:undef -> ok end
  end.

graph(Results, Options) ->
  RawData = lists:sublist(Results,
                          proplists:get_value(first_col, Options, 1),
                          erlang:min(proplists:get_value(columns, Options, 250),
                                     proplists:get_value(rounds, Options, 250))),
  case proplists:get_bool(debug, Options) of
    true -> ?INFO("RawData:~n\t~p~n", [RawData]);
    false -> ok
  end,
  SortedBy2 = lists:keysort(2, [{K, V, M} || {K, V, M} <- RawData, V =/= error, M =/= error]),
  SortedBy3 = lists:keysort(3, [{K, V, M} || {K, V, M} <- RawData, V =/= error, M =/= error]),
  Outliers =
    [{K, error, M} || {K, error, M} <- RawData] ++ [{K, V, error} || {K, V, error} <- RawData] ++
      lists:sublist(lists:reverse(SortedBy2), 1, erlang:trunc(proplists:get_value(outliers, Options, 20)/2) + 1) ++
      lists:sublist(lists:reverse(SortedBy3), 1, erlang:trunc(proplists:get_value(outliers, Options, 20)/2) + 1),
  Data =
    [case lists:member({K,V,M}, Outliers) of
       true -> {K, 0, M};
       false -> {K, V, M}
     end || {K,V,M} <- Results],
  Top = lists:max([erlang:max(V, M) || {_, V, M} <- Data]),
  Bottom = erlang:trunc(lists:min([erlang:min(V, M) || {_, V, M} <- Data, V > 0, M > 0]) / 2),
  Step = 
    case {Top, Bottom} of
      {error, _} -> throw(everything_is_an_error);
      {_, error} -> throw(everything_is_an_error);
      _ ->
        (Top - Bottom) / proplists:get_value(rows, Options, 70)
    end,
  graph(Top, Bottom, Step, proplists:get_value(symbols, Options, #symbols{}), Data).

graph(Top, Bottom, _Step, _Symbols, Data) when Top =< Bottom ->
  io:format("       ~s~n", [lists:duplicate(length(Data), $-)]),
  io:format("       ~s~n", [lists:map(fun({K, _, _}) -> integer_to_list(K rem 10) end, Data)]);
graph(Top, Bottom, Step, Symbols, Data) ->
  io:format("~7.2.0f~s~n",
            [Top * 1.0,
             lists:map(
               fun({_, V, M}) when Top >= V, V > Top - Step,
                                   Top >= M, M > Top - Step ->
                       case {Top - V, Top - M} of
                         {Pos, Mos} when Pos < Step/2, Mos < Step/2 -> Symbols#symbols.up_up;
                         {Pos, Mos} when Pos < Step/2, Mos >= Step/2 -> Symbols#symbols.up_down;
                         {Pos, Mos} when Pos >= Step/2, Mos < Step/2 -> Symbols#symbols.down_up;
                         {Pos, Mos} when Pos >= Step/2, Mos >= Step/2 -> Symbols#symbols.down_down
                       end;
                  ({_, V, _M}) when Top >= V, V > Top - Step ->
                       case Top - V of
                         Pos when Pos < Step/2 -> Symbols#symbols.up_none;
                         Pos when Pos >= Step/2 -> Symbols#symbols.down_none
                       end;
                  ({_, _V, M}) when Top >= M, M > Top - Step ->
                       case Top - M of
                         Pos when Pos < Step/2 -> Symbols#symbols.none_up;
                         Pos when Pos >= Step/2 -> Symbols#symbols.none_down
                       end;
                  (_) -> Symbols#symbols.none_none
               end, Data)]),
  graph(Top-Step, Bottom, Step, Symbols, Data).