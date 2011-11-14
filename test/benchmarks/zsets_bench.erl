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
-export([zadd/1, zadd_one/1, zcard/1, zcount_n/1, zcount_m/1, zincrby/1,
         zinterstore_min/1, zinterstore_n/1, zinterstore_k/1, zinterstore_m/1,
         zrange_n/1, zrange_m/1, zrangebyscore_n/1, zrangebyscore_m/1]).

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
                               Fun =:= zcount_n;
                               Fun =:= zincrby;
                               Fun =:= zrange_n;
                               Fun =:= zrangebyscore_n ->
  zadd(Keys),
  ok;
init_per_round(Fun, Keys) when Fun =:= zinterstore_min ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>, args = [?KEY2, [{1.0, <<"1">>}] ++
                                              [{random:uniform(I) * 1.0,
                                                <<(edis_util:integer_to_binary(I))/binary, "-never-match">>} || I <- lists:seq(1, 10000)]],
                  group = zsets, result_type = number}),
  zadd(Keys),
  ok;
init_per_round(Fun, Keys) when Fun =:= zinterstore_n ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY2, [{1.0, <<"1">>} |
                                    [{random:uniform(length(Keys)) * 1.0,
                                      <<Key/binary, "-never-match">>} || Key <- Keys, Key =/= <<"1">>]]],
                  group = zsets, result_type = number}),
  zadd(Keys),
  ok;
init_per_round(Fun, Keys) when Fun =:= zinterstore_k ->
  lists:foreach(fun(Key) ->
                        edis_db:run(
                          edis_db:process(0),
                          #edis_command{cmd = <<"ZADD">>,
                                        args = [Key, [{1.0, ?KEY}, {2.0, ?KEY2}, {3.0, Key}]],
                                        group = zsets, result_type = number})
                end, Keys);
init_per_round(Fun, Keys) when Fun =:= zinterstore_m ->
  L = length(Keys),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY2, [{random:uniform(L) * 1.0, Key} || Key <- Keys] ++
                            [{random:uniform(I) * 1.0,
                              <<(edis_util:integer_to_binary(I))/binary, "-never-match">>} || I <- lists:seq(L, 10000)]
                         ],
                  group = zsets, result_type = number}),
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY, [{random:uniform(L) * 1.0, Key} || Key <- Keys] ++
                            [{random:uniform(I) * 1.0, edis_util:integer_to_binary(I)} || I <- lists:seq(L, 10000)]
                         ],
                  group = zsets, result_type = number}),
  ok;
init_per_round(Fun, _Keys) when Fun =:= zcount_m;
                                Fun =:= zrange_m;
                                Fun =:= zrangebyscore_m ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY, [{1.0 * I, edis_util:integer_to_binary(I)} || I <- lists:seq(1, 10000)]],
                  group = zsets, result_type = number}),
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
                  group = zsets, result_type = number}).

-spec zadd([binary()]) -> pos_integer().
zadd(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZADD">>,
                  args = [?KEY, [{1.0, Key} || Key <- Keys]],
                  group = zsets, result_type = number}).

-spec zcard([binary()]) -> pos_integer().
zcard(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZCARD">>, args = [?KEY], group = zsets, result_type = number}).

-spec zcount_n([binary()]) -> pos_integer().
zcount_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZCOUNT">>, args = [?KEY, {inc, 1.0}, {inc, 1.0}],
                  group = zsets, result_type = number}).

-spec zcount_m([binary()]) -> pos_integer().
zcount_m([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZCOUNT">>, args = [?KEY, {inc, 1.0}, {inc, edis_util:binary_to_float(Key)}],
                  group = zsets, result_type = number}).

-spec zincrby([binary()]) -> binary().
zincrby([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZINCRBY">>, args = [?KEY, 1.0, Key], group = zsets, result_type = bulk}).

-spec zinterstore_min([binary()]) -> number().
zinterstore_min(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZINTERSTORE">>, args = [?KEY, [{?KEY2, 1.0}, {?KEY, 1.0}], sum],
                  group = zsets, result_type = number}).

-spec zinterstore_n([binary()]) -> number().
zinterstore_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZINTERSTORE">>, args = [?KEY, [{?KEY, 1.0}, {?KEY2, 1.0}], max],
                  group = zsets, result_type = number}).

-spec zinterstore_k([binary()]) -> number().
zinterstore_k(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZINTERSTORE">>, args = [?KEY, [{Key, 1.0} || Key <- Keys], min],
                  group = zsets, result_type = number}).

-spec zinterstore_m([binary()]) -> number().
zinterstore_m(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZINTERSTORE">>, args = [?KEY, [{?KEY, 1.0}, {?KEY2, 1.0}], sum],
                  group = zsets, result_type = number}).

-spec zrange_n([binary()]) -> [binary()].
zrange_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZRANGE">>, args = [?KEY, 0, 1], group = zsets, result_type = multi_bulk}).

-spec zrange_m([binary()]) -> [binary()].
zrange_m([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZRANGE">>, args = [?KEY, 0, edis_util:binary_to_integer(Key)],
                  group = zsets, result_type = multi_bulk}).

-spec zrangebyscore_n([binary()]) -> [binary()].
zrangebyscore_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZRANGEBYSCORE">>, args = [?KEY, {exc, 0.0}, {inc, 1.0}], group = zsets, result_type = multi_bulk}).

-spec zrangebyscore_m([binary()]) -> [binary()].
zrangebyscore_m([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"ZRANGEBYSCORE">>, args = [?KEY, {exc, 0.0}, {inc, edis_util:binary_to_float(Key)}],
                  group = zsets, result_type = multi_bulk}).