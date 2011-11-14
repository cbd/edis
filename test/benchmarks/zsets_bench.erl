%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for zsets commands
%%% @end
%%%-------------------------------------------------------------------
-module(zsets_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-zset">>).
-define(KEY2, <<"test-zset2">>).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([zadd/1, zadd_one/1, zcard/1, zcount_n/1, zcount_m/1]).

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
init_per_round(Fun, Keys) when Fun =:= zcard;
                               Fun =:= zadd_one;
                               Fun =:= zcount_n ->
  zadd(Keys),
  ok;
init_per_round(Fun, _Keys) when Fun =:= zcount_m ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY, [{1.0 * I, <<"x">>} || I <- lists:seq(1, 10000)]],
                  group = sets, result_type = number}),
  ok;
init_per_round(_Fun, _Keys) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], group = keys, result_type = number}),
  ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(_, Keys) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY, ?KEY2 | Keys], group = keys, result_type = number}
        ),
  ok.

-spec zadd_one([binary()]) -> pos_integer().
zadd_one(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>, args = [?KEY, [{1.0, ?KEY2}]],
                  group = sets, result_type = number}).

-spec zadd([binary()]) -> pos_integer().
zadd(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY, [{1.0, Key} || Key <- Keys]],
                  group = sets, result_type = number}).

-spec zcard([binary()]) -> pos_integer().
zcard(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZCARD">>, args = [?KEY], group = sets, result_type = number}).

-spec zcount_n([binary()]) -> pos_integer().
zcount_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZCOUNT">>, args = [?KEY, {inc, 1.0}, {inc, 1.0}],
                  group = sets, result_type = number}).

-spec zcount_m([binary()]) -> pos_integer().
zcount_m([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZCOUNT">>, args = [?KEY, {inc, 1.0}, {inc, edis_util:binary_to_float(Key)}],
                  group = sets, result_type = number}).
