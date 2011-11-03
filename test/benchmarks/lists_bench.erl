%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for lists commands
%%% @end
%%%-------------------------------------------------------------------
-module(lists_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-list">>).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([blpop/1, blpop_nothing/1, brpop/1, brpop_nothing/1, brpoplpush/1, lindex/1, linsert/1,
         llen/1, lpop/1]).

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
init_per_round(Fun, Keys) when Fun =:= blpop_nothing;
                               Fun =:= brpop_nothing ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY | Keys], group = keys, result_type = number}),
  ok;
init_per_round(lindex, Keys) ->
  _ =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"LPUSH">>,
                    args = [?KEY | [<<"x">> || _ <- lists:seq(1, erlang:max(5000, length(Keys)))]],
                    group = hashes, result_type = ok}),
  ok;
init_per_round(_Fun, Keys) ->
  _ =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"LPUSH">>, args = [?KEY | Keys], group = lists, result_type = number}),
  ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(_, _Keys) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], group = keys, result_type = number}
        ),
  ok.

-spec blpop_nothing([binary()]) -> timeout.
blpop_nothing(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"BLPOP">>, args = Keys,
                  timeout = 10, expire = edis_util:now(),
                  group = lists, result_type = multi_bulk}, 10).

-spec blpop([binary()]) -> undefined.
blpop(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"BLPOP">>, args = [?KEY],
                  timeout = 1000, expire = edis_util:now() + 1,
                  group = lists, result_type = multi_bulk}, 1000).

-spec brpop_nothing([binary()]) -> timeout.
brpop_nothing(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"BRPOP">>, args = Keys,
                  timeout = 10, expire = edis_util:now(),
                  group = lists, result_type = multi_bulk}, 10).

-spec brpop([binary()]) -> undefined.
brpop(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"BRPOP">>, args = [?KEY],
                  timeout = 1000, expire = edis_util:now() + 1,
                  group = lists, result_type = multi_bulk}, 1000).

-spec brpoplpush([binary()]) -> undefined.
brpoplpush(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"BRPOPLPUSH">>, args = [?KEY, <<(?KEY)/binary, "-2">>],
                  timeout = 1000, expire = edis_util:now() + 1,
                  group = lists, result_type = bulk}, 1000).

-spec lindex([binary()]) -> binary().
lindex(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"LINDEX">>, args = [?KEY, length(Keys)],
                  group = lists, result_type = bulk}).

-spec linsert([binary()]) -> binary().
linsert([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"LINSERT">>, args = [?KEY, before, Key, <<"x">>],
                  group = lists, result_type = bulk}).

-spec llen([binary()]) -> binary().
llen(_) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"LLEN">>, args = [?KEY], group = lists, result_type = number}).

-spec lpop([binary()]) -> undefined.
lpop(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"LPOP">>, args = [?KEY],
                  timeout = 1000, expire = edis_util:now() + 1,
                  group = lists, result_type = multi_bulk}, 1000).

