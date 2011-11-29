%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for string commands
%%% @end
%%%-------------------------------------------------------------------
-module(strings_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-include("edis.hrl").

-define(KEY, <<"test-string">>).

-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([append/1, decr/1, decrby/1, get/1, getbit/1, getrange/1, getset/1, incr/1, incrby/1,
         mget/1, mset/1, msetnx/1, set/1, setex/1, setnx/1, setbit/1, setrange/1, strlen/1]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec all() -> [atom()].
all() -> [Fun || {Fun, _} <- ?MODULE:module_info(exports) -- edis_bench:behaviour_info(callbacks),
                 Fun =/= module_info].

-spec init([]) -> ok.
init(_Extra) -> ok.

-spec quit([]) -> ok.
quit(_Extra) -> ok.

-spec init_per_testcase(atom(), []) -> ok.
init_per_testcase(_Function, _Extra) -> ok.

-spec quit_per_testcase(atom(), []) -> ok.
quit_per_testcase(_Function, _Extra) -> ok.

-spec init_per_round(atom(), [binary()], []) -> ok.
init_per_round(Fun, Keys, _Extra) when Fun =:= decr;
                                       Fun =:= decrby;
                                       Fun =:= incr;
                                       Fun =:= incrby ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SET">>,
                  args = [?KEY, edis_util:integer_to_binary(length(Keys))],
                  result_type = ok, group = strings});
init_per_round(mget, Keys, _Extra) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, <<"X">>} || Key <- Keys],
                  result_type = ok, group = strings});
init_per_round(Fun, Keys, _Extra) when Fun =:= mset;
                                       Fun =:= msetnx ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = Keys, result_type = ok, group = keys}),
  ok;
init_per_round(Fun, _Keys, _Extra) when Fun =:= set;
                                        Fun =:= setex;
                                        Fun =:= setnx->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], result_type = ok, group = keys}),
  ok;
init_per_round(_Fun, Keys, _Extra) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SET">>,
                  args = [?KEY, list_to_binary(lists:duplicate(length(Keys), $X))],
                  result_type = ok, group = strings}).

-spec quit_per_round(atom(), [binary()], []) -> ok.
quit_per_round(_, _Keys, _Extra) -> ok.

-spec append([binary()]) -> pos_integer().
append([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"APPEND">>, args = [?KEY, Key],
                  group = strings, result_type = number}).

-spec decr([binary()]) -> integer().
decr(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"DECR">>, args = [?KEY],
                  group = strings, result_type = number}).

-spec decrby([binary()]) -> integer().
decrby(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"DECRBY">>, args = [?KEY, length(Keys)],
                  group = strings, result_type = number}).

-spec get([binary()]) -> binary().
get(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GET">>, args = [?KEY],
                  group = strings, result_type = bulk}).

-spec getbit([binary()]) -> 1 | 0.
getbit(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GETBIT">>, args = [?KEY,
                                              random:uniform(length(Keys)) - 1],
                  group = strings, result_type = number}).

-spec getrange([binary()]) -> binary().
getrange(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GETRANGE">>, args = [?KEY, 1, -2],
                  group = strings, result_type = number}).

-spec getset([binary()]) -> binary().
getset([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GETSET">>, args = [?KEY, Key],
                  group = strings, result_type = bulk}).

-spec incr([binary()]) -> integer().
incr(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"INCR">>, args = [?KEY],
                  group = strings, result_type = number}).

-spec incrby([binary()]) -> integer().
incrby(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"INCRBY">>, args = [?KEY, length(Keys)],
                  group = strings, result_type = number}).

-spec mget([binary()]) -> [binary()].
mget(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MGET">>, args = Keys, group = strings, result_type = multi_bulk}).

-spec mset([binary()]) -> ok.
mset(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, <<"X">>} || Key <- Keys],
                  result_type = ok, group = strings}).

-spec msetnx([binary()]) -> ok.
msetnx(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, <<"X">>} || Key <- Keys],
                  result_type = ok, group = strings}).

-spec set([binary()]) -> ok.
set(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SET">>,
                  args = [?KEY, unicode:characters_to_binary(lists:duplicate(length(Keys), $X))],
                  result_type = ok, group = strings}).

-spec setex([binary()]) -> ok.
setex(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SETEX">>,
                  args = [?KEY, length(Keys), unicode:characters_to_binary(lists:duplicate(length(Keys), $X))],
                  result_type = ok, group = strings}).

-spec setnx([binary()]) -> ok.
setnx(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SETNX">>,
                  args = [?KEY, unicode:characters_to_binary(lists:duplicate(length(Keys), $X))],
                  result_type = boolean, group = strings}).

-spec setbit([binary()]) -> 1 | 0.
setbit(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SETBIT">>, args = [?KEY, random:uniform(length(Keys)) - 1,
                                              random:uniform(2) - 1],
                  group = strings, result_type = number}).

-spec setrange([binary()]) -> pos_integer().
setrange(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SETRANGE">>, args = [?KEY, random:uniform(length(Keys)) - 1, <<"xxx">>],
                  group = strings, result_type = number}).

-spec strlen([binary()]) -> pos_integer().
strlen(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"STRLEN">>, args = [?KEY], group = strings, result_type = number}).