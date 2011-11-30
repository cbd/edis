%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Benchmarks for hashes commands using erldis
%%% @end
%%%-------------------------------------------------------------------
-module(erldis_hashes_bench).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_bench).

-define(KEY, <<"test-hash">>).

-include("edis.hrl").
-include("edis_bench.hrl").

-export([bench/1, bench/2, bench/4]).
-export([all/0,
         init/1, init_per_testcase/2, init_per_round/3,
         quit/1, quit_per_testcase/2, quit_per_round/3]).
-export([hdel/2, hexists/2, hget/2, hgetall/2, hincrby/2, hkeys/2, hlen/2, hmget/2, hmset/2,
         hset/2, hsetnx/2, hvals/2]).

%% ====================================================================
%% External functions
%% ====================================================================
-spec bench(atom()) -> ok.
bench(Function) -> bench(Function, []).

-spec bench(atom(), [edis_bench:option()]) -> ok.
bench(Function, Options) -> bench(Function, 6380, 6379, Options).

-spec bench(atom(), pos_integer(), pos_integer(), [edis_bench:option()]) -> ok.
bench(Function, P1, P2, Options) ->
  edis_bench:bench({?MODULE, Function, [P1]}, {?MODULE, Function, [P2]},
                   Options ++
                     [{outliers,100}, {symbols, #symbols{down_down  = $x,
                                                         up_up      = $x,
                                                         up_down    = $x,
                                                         down_up    = $x,
                                                         down_none  = $e,
                                                         up_none    = $e,
                                                         none_down  = $r,
                                                         none_up    = $r}}]).

-spec all() -> [atom()].
all() -> [Fun || {Fun, _} <- ?MODULE:module_info(exports) -- edis_bench:behaviour_info(callbacks),
                 Fun =/= module_info].

-spec init([pos_integer()]) -> ok.
init([Port]) ->
  case erldis:connect(localhost,Port) of
    {ok, Client} ->
      Name = process(Port),
      case erlang:whereis(Name) of
        undefined -> true;
        _ -> erlang:unregister(Name)
      end,
      erlang:register(Name, Client),
      ok;
    Error -> throw(Error)
  end.

-spec quit([pos_integer()]) -> ok.
quit([Port]) ->
  Name = process(Port),
  case erlang:whereis(Name) of
    undefined -> ok;
    Client -> erldis_client:stop(Client)
  end,
  ok.

-spec init_per_testcase(atom(), [pos_integer()]) -> ok.
init_per_testcase(_Function, _Extra) -> ok.

-spec quit_per_testcase(atom(), [pos_integer()]) -> ok.
quit_per_testcase(_Function, _Extra) -> ok.

-spec init_per_round(atom(), [binary()], [pos_integer()]) -> ok.
init_per_round(incrby, Keys, [Port]) ->
  _ = erldis:hset(process(Port), ?KEY, ?KEY, edis_util:integer_to_binary(length(Keys))),
  ok;
init_per_round(Fun, Keys, [Port]) when Fun =:= hgetall;
                                       Fun =:= hkeys;
                                       Fun =:= hvals;
                                       Fun =:= hlen ->
  erldis:hmset(process(Port), ?KEY, [{Key, <<"x">>} || Key <- Keys]);
init_per_round(Fun, _Keys, [Port]) when Fun =:= hmget; Fun =:= hmset ->
  erldis:hmset(process(Port),
               ?KEY, [{edis_util:integer_to_binary(Key), <<"x">>} || Key <- lists:seq(1, 5000)]);
init_per_round(_Fun, Keys, [Port]) ->
  erldis:hmset(process(Port),
               ?KEY, [{Key, <<"x">>} || Key <- Keys] ++
                 [{<<Key/binary, "-2">>, <<"y">>} || Key <- Keys]).

-spec quit_per_round(atom(), [binary()], [pos_integer()]) -> ok.
quit_per_round(_, _Keys, [Port]) ->
  _ = erldis:del(process(Port), ?KEY),
  ok.


-spec hdel([binary()], pos_integer()) -> pos_integer().
hdel(Keys, Port) ->
  erldis:hdel(process(Port), ?KEY, Keys).

-spec hexists([binary(),...], pos_integer()) -> boolean().
hexists([Key|_], Port) ->
  erldis:hexists(process(Port), ?KEY, Key).

-spec hget([binary()], pos_integer()) -> binary().
hget([Key|_], Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HGET">>, args = [?KEY, Key], result_type = bulk, group = hashes}).

-spec hgetall([binary()], pos_integer()) -> binary().
hgetall(_Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HGETALL">>, args = [?KEY], result_type = multi_bulk, group = hashes}).

-spec hincrby([binary()], pos_integer()) -> integer().
hincrby(Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HINCRBY">>, args = [?KEY, ?KEY, length(Keys)],
                  group = hashes, result_type = number}).

-spec hkeys([binary()], pos_integer()) -> binary().
hkeys(_Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HKEYS">>, args = [?KEY], result_type = multi_bulk, group = hashes}).

-spec hlen([binary()], pos_integer()) -> binary().
hlen(_Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HLEN">>, args = [?KEY], result_type = number, group = hashes}).

-spec hmget([binary()], pos_integer()) -> binary().
hmget(Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HMGET">>, args = [?KEY | Keys], result_type = multi_bulk, group = hashes}).

-spec hmset([binary()], pos_integer()) -> binary().
hmset(Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HMSET">>, args = [?KEY, [{Key, <<"y">>} || Key <- Keys]],
                  result_type = ok, group = hashes}).

-spec hset([binary()], pos_integer()) -> binary().
hset([Key|_], Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HSET">>, args = [?KEY, Key, Key], result_type = boolean, group = hashes}).

-spec hsetnx([binary()], pos_integer()) -> binary().
hsetnx([Key|_], Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HSETNX">>, args = [?KEY,
                                              case random:uniform(2) of
                                                1 -> Key;
                                                2 -> <<Key/binary, "__">>
                                              end, Key], result_type = boolean, group = hashes}).

-spec hvals([binary()], pos_integer()) -> binary().
hvals(_Keys, Port) ->
  edis_db:run(
    edis_db:process(0),
    #edis_command{cmd = <<"HVALS">>, args = [?KEY], result_type = multi_bulk, group = hashes}).

process(Port) -> list_to_atom("erldis-tester-" ++ integer_to_list(Port)).