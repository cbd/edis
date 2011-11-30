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
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([sadd/1, scard/1, sdiff/1, sdiffstore/1, sinter_min/1, sinter_n/1, sinter_m/1,
         sinterstore_min/1, sinterstore_n/1, sinterstore_m/1, sismember/1, smembers/1,
         smove/1, spop/1, srandmember/1, srem/1, sunion/1, sunionstore/1]).

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
init_per_round(Fun, Keys, _Extra) when Fun =:= scard;
                                       Fun =:= sismember;
                                       Fun =:= smembers;
                                       Fun =:= smove;
                                       Fun =:= spop;
                                       Fun =:= srandmember ->
  sadd(Keys),
  ok;
init_per_round(Fun, Keys, _Extra) when Fun =:= sinter_min;
                                       Fun =:= sinterstore_min ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SADD">>, args = [?KEY2, <<"1">>],
                  group = sets, result_type = number}),
  sadd(Keys),
  ok;
init_per_round(Fun, Keys, _Extra) when Fun =:= sinter_n;
                                       Fun =:= sinterstore_n ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SADD">>, args = [?KEY2 | Keys],
                  group = sets, result_type = number}),
  sadd(Keys),
  ok;
init_per_round(Fun, Keys, _Extra) when Fun =:= sinter_m;
                                       Fun =:= sinterstore_m ->
  lists:foreach(fun(Key) ->
                        edis_db:run(
                          edis_db:process(0),
                          #edis_command{cmd = <<"SADD">>, args = [Key, ?KEY, ?KEY2, Key],
                                        group = sets, result_type = number})
                end, Keys);
init_per_round(Fun, Keys, _Extra) when Fun =:= sdiff;
                                       Fun =:= sdiffstore;
                                       Fun =:= sunion;
                                       Fun =:= sunionstore ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SADD">>,
                  args = [?KEY2 | lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, 100))],
                  group = sets, result_type = number}),
  sadd(Keys),
  ok;
init_per_round(Fun, _Keys, _Extra) when Fun =:= srem ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SADD">>,
                  args = [?KEY | lists:map(fun edis_util:integer_to_binary/1, lists:seq(1, 10000))],
                  group = sets, result_type = number}),
  ok;
init_per_round(_Fun, _Keys, _Extra) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY], group = keys, result_type = number}),
  ok.

-spec quit_per_round(atom(), [binary()], []) -> ok.
quit_per_round(_, Keys, _Extra) ->
  _ = edis_db:run(
        edis_db:process(0),
        #edis_command{cmd = <<"DEL">>, args = [?KEY, ?KEY2 | Keys], group = keys, result_type = number}
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

-spec sinter_min([binary()]) -> [binary()].
sinter_min(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SINTER">>, args = [?KEY, ?KEY2], group = sets, result_type = multi_bulk}).

-spec sinter_n([binary()]) -> [binary()].
sinter_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SINTER">>, args = [?KEY, ?KEY2], group = sets, result_type = multi_bulk}).

-spec sinter_m([binary()]) -> [binary()].
sinter_m(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SINTER">>, args = Keys, group = sets, result_type = multi_bulk}).

-spec sinterstore_min([binary()]) -> [binary()].
sinterstore_min(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SINTERSTORE">>, args = [?KEY, ?KEY, ?KEY2],
                  group = sets, result_type = multi_bulk}).

-spec sinterstore_n([binary()]) -> [binary()].
sinterstore_n(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SINTERSTORE">>, args = [?KEY, ?KEY, ?KEY2],
                  group = sets, result_type = multi_bulk}).

-spec sinterstore_m([binary()]) -> [binary()].
sinterstore_m(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SINTERSTORE">>, args = [?KEY | Keys],
                  group = sets, result_type = multi_bulk}).

-spec sismember([binary()]) -> true.
sismember([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SISMEMBER">>, args = [?KEY, Key], group = sets, result_type = boolean}).

-spec smembers([binary()]) -> [binary()].
smembers(_) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SMEMBERS">>, args = [?KEY], group = sets, result_type = multi_bulk}).

-spec smove([binary()]) -> boolean().
smove([Key|_]) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SMOVE">>, args = [?KEY, ?KEY2, Key], group = sets, result_type = boolean}).

-spec spop([binary()]) -> binary().
spop(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SPOP">>, args = [?KEY], group = sets, result_type = bulk}).

-spec srandmember([binary()]) -> binary().
srandmember(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SRANDMEMBER">>, args = [?KEY], group = sets, result_type = bulk}).

-spec srem([binary()]) -> number().
srem(Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SREM">>, args = [?KEY | Keys], group = sets, result_type = number}).

-spec sunion([binary()]) -> [binary()].
sunion(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SUNION">>, args = [?KEY, ?KEY2], group = sets, result_type = multi_bulk}).

-spec sunionstore([binary()]) -> [binary()].
sunionstore(_Keys) ->
  catch edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"SUNIONSTORE">>, args = [?KEY, ?KEY, ?KEY2],
                  group = sets, result_type = multi_bulk}).
