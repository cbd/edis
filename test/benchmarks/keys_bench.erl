%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for keys commands
%%% @end
%%%-------------------------------------------------------------------
-module(keys_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_benchmarker).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([del/1]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec all() -> [atom()].
all() -> [del].

-spec init() -> ok.
init() -> ok.

-spec quit() -> ok.
quit() -> ok.

-spec init_per_testcase(atom()) -> ok.
init_per_testcase(_Function) -> ok.

-spec quit_per_testcase(atom()) -> ok.
quit_per_testcase(_Function) -> ok.

-spec init_per_round(atom(), [binary()]) -> ok.
init_per_round(del, Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, Key} || Key <- Keys],
                  group = keys, result_type = ok}).

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(del, _Keys) -> ok.

-spec del([binary()]) -> pos_integer().
del(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"DEL">>, args = Keys, group = keys, result_type = number}).