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

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([append/1, decr/1, decrby/1, get/1, getbit/1, getrange/1, getset/1, incr/1, incrby/1,
         mget/1, mset/1, msetnx/1]).

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
init_per_round(Fun, Keys) when Fun =:= append;
                               Fun =:= get;
                               Fun =:= getbit;
                               Fun =:= getrange;
                               Fun =:= getset ->
  [{ok, Deleted} | OkKeys] =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"EXEC">>, group = transaction, result_type = multi_result,
                    args = [#edis_command{cmd = <<"DEL">>, args = [<<"test-string">>],
                                          group = keys, result_type = number} |
                                           [#edis_command{cmd = <<"APPEND">>,
                                                          args = [<<"test-string">>, <<"X">>],
                                                          result_type = number,
                                                          group = strings} || _Key <- Keys]]}),
  case Deleted of
    0 -> ok;
    1 -> ok
  end,
  case {length(OkKeys), length(Keys)} of
    {X,X} -> ok
  end;
init_per_round(Fun, Keys) when Fun =:= decr;
                               Fun =:= decrby;
                               Fun =:= incr;
                               Fun =:= incrby ->
  [{ok, Deleted} , ok] =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"EXEC">>, group = transaction, result_type = multi_result,
                    args = [#edis_command{cmd = <<"DEL">>, args = [<<"test-string">>],
                                          group = keys, result_type = number},
                            #edis_command{cmd = <<"SET">>,
                                          args = [<<"test-string">>, edis_util:integer_to_binary(length(Keys))],
                                          result_type = ok, group = strings}]}),
  case Deleted of
    0 -> ok;
    1 -> ok
  end;
init_per_round(mget, Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"MSET">>, args = [{Key, <<"X">>} || Key <- Keys],
                  result_type = ok, group = strings});
init_per_round(Fun, Keys) when Fun =:= mset;
                               Fun =:= msetnx ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = Keys, result_type = ok, group = keys}),
  ok;
init_per_round(_Fun, _Keys) -> ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(_, _Keys) -> ok.

-spec append([binary()]) -> pos_integer().
append([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"APPEND">>, args = [<<"test-string">>, Key],
                  group = strings, result_type = number}).

-spec decr([binary()]) -> integer().
decr(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"DECR">>, args = [<<"test-string">>],
                  group = strings, result_type = number}).

-spec decrby([binary()]) -> integer().
decrby(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"DECRBY">>, args = [<<"test-string">>, length(Keys)],
                  group = strings, result_type = number}).

-spec get([binary()]) -> binary().
get(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GET">>, args = [<<"test-string">>],
                  group = strings, result_type = bulk}).

-spec getbit([binary()]) -> 1 | 0.
getbit(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GETBIT">>, args = [<<"test-string">>,
                                              random:uniform(length(Keys)) - 1],
                  group = strings, result_type = number}).

-spec getrange([binary()]) -> binary().
getrange(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GETRANGE">>, args = [<<"test-string">>, 1, -2],
                  group = strings, result_type = number}).

-spec getset([binary()]) -> binary().
getset([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"GETSET">>, args = [<<"test-string">>, Key],
                  group = strings, result_type = bulk}).

-spec incr([binary()]) -> integer().
incr(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"INCR">>, args = [<<"test-string">>],
                  group = strings, result_type = number}).

-spec incrby([binary()]) -> integer().
incrby(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"INCRBY">>, args = [<<"test-string">>, length(Keys)],
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