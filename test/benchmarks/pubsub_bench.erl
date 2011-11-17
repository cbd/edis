%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for pubsub commands
%%% @end
%%%-------------------------------------------------------------------
-module(pubsub_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([psubscribe/1, publish/1, punsubscribe/1]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec all() -> [atom()].
all() -> [Fun || {Fun, _} <- ?MODULE:module_info(exports) -- edis_bench:behaviour_info(callbacks),
                 Fun =/= module_info].

-spec init() -> ok.
init() -> ok.

-spec quit() -> ok.
quit() -> ok.

-spec init_per_testcase(atom()) -> ok.
init_per_testcase(publish) -> ok;
init_per_testcase(_Function) -> edis_pubsub:add_sup_handler().

-spec quit_per_testcase(atom()) -> ok.
quit_per_testcase(publish) -> ok;
quit_per_testcase(_Function) -> edis_pubsub:delete_handler().

-spec init_per_round(atom(), [binary()]) -> ok.
init_per_round(publish, Keys) ->
  lists:foreach(
    fun(Key) ->
            P = proc_lib:spawn(
                  fun() ->
                          ok = edis_pubsub:add_sup_handler(),
                          receive
                            stop -> edis_pubsub:delete_handler()
                          end
                  end),
            Name = binary_to_atom(<<"pubsub-bench-", Key/binary>>, utf8),
            case erlang:whereis(Name) of
              undefined -> true;
              _ -> erlang:unregister(Name)
            end,
            erlang:register(Name, P)
    end, Keys),
  wait_for_handlers(length(Keys));
init_per_round(_Fun, _Keys) -> ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(publish, Keys) ->
  lists:foreach(
    fun(Key) ->
            binary_to_atom(<<"pubsub-bench-", Key/binary>>, utf8) ! stop
    end, Keys),
  wait_for_handlers(0);
quit_per_round(_Fun, _Keys) -> ok.

-spec psubscribe([binary()]) -> {[term()], gb_set()}.
psubscribe(Patterns) ->
  lists:foldl(
    fun(Pattern, {_Result, AccPatternSet}) ->
            NextPatternSet = gb_sets:add_element(Pattern, AccPatternSet),
            {[<<"psubscribe">>, Pattern, gb_sets:size(NextPatternSet)], NextPatternSet}
    end, {nothing, gb_sets:empty()}, Patterns).

-spec punsubscribe([binary()]) -> {[term()], gb_set()}.
punsubscribe(Patterns) ->
  InitialState = psubscribe(lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, 10000))),
  lists:foldl(
    fun(Pattern, {_Result, AccPatternSet}) ->
            NextPatternSet = gb_sets:del_element(Pattern, AccPatternSet),
            {[<<"psubscribe">>, Pattern, gb_sets:size(NextPatternSet)], NextPatternSet}
    end, InitialState, Patterns).

-spec publish([binary()]) -> non_neg_integer().
publish([Key|_]) ->
  edis_pubsub:notify(#edis_message{channel = Key, message = Key}),
  edis_pubsub:count_handlers().

wait_for_handlers(N) ->
  case edis_pubsub:count_handlers() of
    N -> ok;
    _ -> timer:sleep(100), wait_for_handlers(N)
  end.