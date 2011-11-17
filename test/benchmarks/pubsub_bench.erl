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
-export([psubscribe/1]).

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
init_per_testcase(_Function) -> edis_pubsub:add_sup_handler().

-spec quit_per_testcase(atom()) -> ok.
quit_per_testcase(_Function) -> edis_pubsub:delete_handler().

-spec init_per_round(atom(), [binary()]) -> ok.
init_per_round(_Fun, _Keys) -> ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(_Fun, _Keys) -> ok.

-spec psubscribe([binary()]) -> {[binary()], gb_set()}.
psubscribe(Patterns) ->
      lists:foldl(
      fun(Pattern, {_Result, AccPatternSet}) ->
              NextPatternSet = gb_sets:add_element(Pattern, AccPatternSet),
              {[<<"psubscribe">>, Pattern, gb_sets:size(NextPatternSet)], NextPatternSet}
      end, {nothing, gb_sets:empty()}, Patterns).