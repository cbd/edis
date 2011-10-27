%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarker. Given a module to test with, it helps users determine
%%%      the order of functions
%%% @end
%%%-------------------------------------------------------------------
-module(edis_benchmarker).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-export([test/2, test/1, behaviour_info/1]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec behaviour_info(callbacks|term()) -> [{atom(), non_neg_integer()}].
behaviour_info(callbacks) ->
  [{all, 0},
   {init, 0}, {init_per_testcase, 1}, {init_per_round, 2},
   {quit, 0}, {quit_per_testcase, 1}, {quit_per_round, 2}].

-spec test(atom()) -> [{atom(), [{pos_integer(), error | pos_integer()}]}].
test(Module) ->
  ok = Module:init(),
  try
    lists:map(fun(Function) ->
                      {Function, do_test(Module, Function)}
              end, Module:all())
  after
      Module:quit()
  end.

-spec test(atom(), atom()) -> [{pos_integer(), error | pos_integer()}].
test(Module, Function) ->
  ok = Module:init(),
  try do_test(Module, Function)
  after
      Module:quit()
  end.

%% ====================================================================
%% Internal functions
%% ====================================================================
do_test(Module, Function) ->
  ok = Module:init_per_testcase(Function),
  try lists:map(fun(N) -> do_test(Module, Function, N) end,
        lists:seq(1, 500))
  after
      Module:quit_per_testcase(Function)
  end.

do_test(Module, Function, N) ->
  Items = lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, N)),
  ok = Module:init_per_round(Function, Items),
  try timer:tc(Module, Function, [Items]) of
    {Time, _Result} -> {N, Time}
  catch
    _:Error ->
      ?ERROR("Error on ~p:~p (N: ~p):~n\t~p~n", [Module, Function, N, Error]),
      {N, error}
  after
      Module:quit_per_round(Function, Items)
  end.