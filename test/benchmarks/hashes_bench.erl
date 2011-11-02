%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for hashes commands
%%% @end
%%%-------------------------------------------------------------------
-module(hashes_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-hash">>).

-include("edis.hrl").

-export([all/0,
         init/0, init_per_testcase/1, init_per_round/2,
         quit/0, quit_per_testcase/1, quit_per_round/2]).
-export([hdel/1, hexists/1, hget/1, hgetall/1]).

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
init_per_round(hgetall, Keys) ->
  _ =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"HMSET">>, args = [?KEY, [{Key, <<"x">>} || Key <- Keys]],
                    group = hashes, result_type = ok}),
  ok;
init_per_round(_Fun, Keys) ->
  _ =
    edis_db:run(
      edis_db:process(0),
      #edis_command{cmd = <<"HMSET">>, args = [?KEY, [{Key, <<"x">>} || Key <- Keys] ++
                                                 [{<<Key/binary, "-2">>, <<"y">>} || Key <- Keys]],
                    group = hashes, result_type = ok}),
  ok.

-spec quit_per_round(atom(), [binary()]) -> ok.
quit_per_round(_, _Keys) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], group = keys, result_type = number}
        ),
  ok.

-spec hdel([binary()]) -> pos_integer().
hdel(Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HDEL">>, args = [?KEY | Keys], group = hashes, result_type = number}).

-spec hexists([binary(),...]) -> boolean().
hexists([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HEXISTS">>, args = [?KEY, Key], result_type = boolean, group = hashes}).

-spec hget([binary()]) -> binary().
hget([Key|_]) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HGET">>, args = [?KEY, Key], result_type = bulk, group = hashes}).

-spec hgetall([binary()]) -> binary().
hgetall(_Keys) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HGETALL">>, args = [?KEY], result_type = multi_bulk, group = hashes}).