%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for sets commands
%%% @end
%%%-------------------------------------------------------------------
-module(sets_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-set">>).
-define(KEY2, <<"test-set2">>).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([sadd/1, scard/1, sdiff/1, sdiffstore/1]).

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
init_per_testcase(_Function) -> ok.

-spec quit_per_testcase(atom()) -> ok.
quit_per_testcase(_Function) -> ok.

-spec init_per_round(atom(), [binary()]) -> ok.
init_per_round(scard, Keys) -> sadd(Keys), ok;
init_per_round(Fun, Keys) when Fun =:= sdiff;
                               Fun =:= sdiffstore ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SADD">>,
                  args = [?KEY2 | lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, 100))],
                  group = sets, result_type = number}),
  sadd(Keys),
  ok;
init_per_round(_Fun, _Keys) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], group = keys, result_type = number}),
  ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(_, _Keys) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], group = keys, result_type = number}
        ),
  ok.

-spec sadd([binary()]) -> pos_integer().
sadd(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SADD">>, args = [?KEY | Keys],
                  group = sets, result_type = number}).

-spec scard([binary()]) -> pos_integer().
scard(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SCARD">>, args = [?KEY], group = sets, result_type = number}).

-spec sdiff([binary()]) -> [binary()].
sdiff(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SDIFF">>, args = [?KEY, ?KEY2], group = sets, result_type = multi_bulk}).

-spec sdiffstore([binary()]) -> [binary()].
sdiffstore(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SDIFFSTORE">>, args = [?KEY, ?KEY, ?KEY2],
                  group = sets, result_type = multi_bulk}).