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

-type option() :: {start, pos_integer} | {step, pos_integer()} | {rounds, pos_integer()} |
        {extra_args, [term()]} | {outliers, pos_integer()} | {columns, pos_integer()} |
        {first_col, pos_integer()} | {rows, pos_integer()} | debug | {k, number()} | {x, number()}.
-export_type([option/0]).

-export([compare/4, compare/3,
         graph/4, graph/3, graph/2,
         run/3, run/2, run/1,
         behaviour_info/1]).

-export([zero/1, constant/1, linear/1, quadratic/1, logarithmic/1, exponential/1]).

%% ====================================================================
%% External functions
%% ====================================================================
%% @hidden
-spec behaviour_info(callbacks|term()) -> [{atom(), non_neg_integer()}].
behaviour_info(callbacks) ->
  [{all, 0},
   {init, 0}, {init_per_testcase, 1}, {init_per_round, 2},
   {quit, 0}, {quit_per_testcase, 1}, {quit_per_round, 2}].

%% @doc Runs all the benchmarking functions on Module.
%%      The list is obtained calling Module:all().
-spec run(atom()) -> [{atom(), [{pos_integer(), error | pos_integer()}]}].
run(Module) ->
  ok = try Module:init() catch _:undef -> ok end,
  try
    lists:map(fun(Function) ->
                      {Function, do_run(Module, Function, [])}
              end, Module:all())
  after
      try Module:quit() catch _:undef -> ok end
  end.

%% @doc Runs the benchmarking function Module:Function/1.
%% @equiv run(Module, Function, []).
-spec run(atom(), atom()) -> [{pos_integer(), error | pos_integer()}].
run(Module, Function) ->
  run(Module, Function, []).

%% @doc Runs the benchmarking function Module:Function using options.
-spec run(atom(), atom(), [option()]) -> [{pos_integer(), error | pos_integer()}].
run(Module, Function, Options) ->
  ok = try Module:init() catch _:undef -> ok end,
  try do_run(Module, Function, Options)
  after
      try Module:quit() catch _:undef -> ok end
  end.

%% @doc Compares the different runs of Module:Function to a given function.
%%      Returns the standard deviation of the distances between them (outliers excluded).
%%      The higher the value the more different functions are.
-spec compare(atom(), atom(), atom() | fun((pos_integer()) -> number()), [option()]) -> float().
compare(Module, Function, MathFunction, Options) when is_atom(MathFunction) ->
  compare(Module, Function, fun(X) -> ?MODULE:MathFunction(X) end, Options);
compare(Module, Function, MathFunction, Options) ->
  RawResults = run(Module, Function, Options),
  Distances =
    [case {V, proplists:get_value(x, Options, 0) +
             (proplists:get_value(k, Options, 100) * MathFunction(K))} of
       {error, _} -> 0;
       {_, 0} -> 0;
       {V, M} -> M / V
     end || {K, V} <- RawResults],
  WithoutOutliers =
    lists:sublist(
      lists:sort(Distances), 1,
      proplists:get_value(rounds, Options, 250) - 2 * proplists:get_value(outliers, Options, 20)),
  Avg = lists:sum(WithoutOutliers) / length(WithoutOutliers),
  case proplists:get_bool(debug, Options) of
    true -> ?INFO("~p~n", [lists:sort(Distances)]);
    false -> ok
  end,
  do_graph(RawResults, MathFunction, Options),
  lists:sum([case Distance of
               Distance when Distance >= Avg -> Distance - Avg;
               _ -> Avg - Distance
             end || Distance <- Distances]) / length(Distances).
%% math:sqrt([(Distance - Avg) * (Distance - Avg) / Avg || Distance <- Distances])).

%% @doc Compares the different runs of Module:Function/1 to a given function.
%% @equiv compare(Module, Function, []).
-spec compare(atom(), atom(), atom() | fun((pos_integer()) -> number())) -> float().
compare(Module, Function, MathFunction) ->
  compare(Module, Function, MathFunction, []).

%% @doc Graphs the results of running Module:Function/1 using ASCII Art
%% @equiv graph(Module, Function, []).
-spec graph(atom(), atom()) -> ok.
graph(Module, Function) ->
  graph(Module, Function, []).

%% @doc Graphs the results of running Module:Function using ASCII Art
%% @equiv graph(Module, Function, zero, Options).
-spec graph(atom(), atom(), [option()]) -> ok.
graph(Module, Function, Options) ->
  graph(Module, Function, zero, Options).

%% @doc Graphs the results of running Module:Function using ASCII Art
-spec graph(atom(), atom(), atom() | fun((pos_integer()) -> number()), [option()]) -> ok.
graph(Module, Function, MathFunction, Options) when is_atom(MathFunction) ->
  graph(Module, Function, fun(X) -> ?MODULE:MathFunction(X) end, Options);
graph(Module, Function, MathFunction, Options) ->
  do_graph(run(Module, Function, Options), MathFunction, Options).

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

%% @doc O(e^n) comparer
-spec exponential(pos_integer()) -> float().
exponential(N) -> math:pow(2.71828182845904523536028747135266249775724709369995, N).

%% ====================================================================
%% Internal functions
%% ====================================================================
do_run(Module, Function, Options) ->
  ok = try Module:init_per_testcase(Function) catch _:undef -> ok end,
  Start = proplists:get_value(start, Options, 1),
  try lists:map(fun(N) -> do_run(Module, Function, N, Options) end,
        lists:seq(
                  Start,
                  Start +proplists:get_value(rounds, Options, 250) *
                    proplists:get_value(step, Options, 1),
                  proplists:get_value(step, Options, 1)))
  after
      try Module:quit_per_testcase(Function) catch _:undef -> ok end
  end.

do_run(Module, Function, N, Options) ->
  Items = lists:reverse(lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, N))),
  ok = try Module:init_per_round(Function, Items) catch _:undef -> ok end,
  try timer:tc(Module, Function, [Items | proplists:get_value(extra_args, Options, [])]) of
    {Time, _Result} ->
      case proplists:get_bool(debug, Options) of
        true -> ?INFO("~p: ~p~n", [N, Time]);
        false -> ok
      end,
      {N, Time}
  catch
    _:Error ->
      ?ERROR("Error on ~p:~p (N: ~p):~n\t~p~n", [Module, Function, N, Error]),
      {N, error}
  after
      try Module:quit_per_round(Function, Items) catch _:undef -> ok end
  end.

do_graph(Results, MathFunction, Options) ->
  RawData = lists:sublist(Results,
                          proplists:get_value(first_col, Options, 1),
                          erlang:min(proplists:get_value(columns, Options, 250),
                                     proplists:get_value(rounds, Options, 250))),
  SortedData = lists:keysort(2, [{K, V} || {K, V} <- RawData, V =/= error]),
  Outliers =
    [{K, error} || {K, error} <- RawData] ++
      lists:sublist(lists:reverse(SortedData), 1, proplists:get_value(outliers, Options, 20)),
  Data = [case lists:member({K,V}, Outliers) of
            true -> {K, 0, proplists:get_value(x, Options, 0) +
                       (proplists:get_value(k, Options, 100) * MathFunction(K))};
            false -> {K, V, proplists:get_value(x, Options, 0) +
                        (proplists:get_value(k, Options, 100) * MathFunction(K))}
          end || {K,V} <- RawData],
  Top = lists:max([erlang:max(V, M) || {_, V, M} <- Data]),
  Bottom = erlang:trunc(lists:min([erlang:min(V, M) || {_, V, M} <- Data, V > 0, M > 0]) / 2),
  Step = erlang:trunc((Top - Bottom + 1) / proplists:get_value(rows, Options, 70)) + 1,
  do_graph(Top, Bottom, Step, Data).

do_graph(Top, Bottom, _Step, Data) when Top =< Bottom ->
  io:format("      ~s~n", [lists:duplicate(length(Data), $-)]),
  io:format("      ~s~n", [lists:map(fun({K, _, _}) -> integer_to_list(K rem 10) end, Data)]);
do_graph(Top, Bottom, Step, Data) ->
  io:format("~6w~s~n",
            [Top,
             lists:map(
               fun({_, V, M}) when Top >= V, V > Top - Step,
                                   Top >= M, M > Top - Step ->
                       case {Top - V, Top - M} of
                         {Pos, Mos} when Pos < Step/2, Mos < Step/2 -> $";  %% both on top
                         {Pos, Mos} when Pos < Step/2, Mos >= Step/2 -> $=; %% top and bottom
                         {Pos, Mos} when Pos >= Step/2, Mos < Step/2 -> $=; %% top and bottom
                         {Pos, Mos} when Pos >= Step/2, Mos >= Step/2 -> $. %% both on bottom
                       end;
                  ({_, V, _M}) when Top >= V, V > Top - Step ->
                       case Top - V of
                         Pos when Pos < Step/3 -> $¨;
                         Pos when Pos >= 2*Step/3 -> $_;
                         _Pos -> $-
                       end;
                  ({_, _V, M}) when Top >= M, M > Top - Step ->
                       case Top - M of
                         Pos when Pos < Step/3 -> $¨;
                         Pos when Pos >= 2*Step/3 -> $_;
                         _Pos -> $-
                       end;
                  (_) -> $\s
               end, Data)]),
  do_graph(Top-Step, Bottom, Step, Data).